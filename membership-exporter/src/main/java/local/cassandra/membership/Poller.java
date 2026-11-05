package local.cassandra.membership;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.Logger;
import java.util.function.LongSupplier;

final class Poller implements AutoCloseable {
    enum State { STARTING, HEALTHY, BACKOFF, OPEN, HALF_OPEN, STALE, STALLED, STOPPED }
    static final class View {
        final Good good;
        final boolean fresh, circuit;
        final State state;
        final double retryAfter, age;
        View(Good good,boolean fresh,boolean circuit,State state,double retryAfter,double age) {
            this.good=good;this.fresh=fresh;this.circuit=circuit;this.state=state;this.retryAfter=retryAfter;this.age=age;
        }
    }
    static final class Good {
        final Snapshot snapshot; final long nanos; final double timestamp;
        Good(Snapshot snapshot,long nanos) {this.snapshot=snapshot;this.nanos=nanos;this.timestamp=System.currentTimeMillis()/1000.0;}
    }
    private static final Logger LOG=Logger.getLogger(Poller.class.getName());
    final Config config; final Reader reader;
    final AtomicReference<Good> last=new AtomicReference<>();
    final AtomicLong polls=new AtomicLong(), failures=new AtomicLong(), slowPolls=new AtomicLong(), conflicts=new AtomicLong();
    final AtomicBoolean inFlight=new AtomicBoolean();
    final AtomicLong timeoutFailures=new AtomicLong(), recoveryAttempts=new AtomicLong(), recoveries=new AtomicLong(), consecutiveFailures=new AtomicLong();
    private final ScheduledExecutorService scheduler=Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t=new Thread(r,"membership-poller");t.setDaemon(true);return t;
    });
    private final PollPolicy policy;
    private final LongSupplier clock;
    private long timeoutDelay;
    private volatile long nextAttemptNanos;
    private volatile long plannedDelayMs;
    private volatile State state=State.STARTING;
    volatile boolean lastSuccess, closed, circuitOpen;
    volatile long startedNanos;
    volatile double lastDuration, nextDelay;
    Poller(Config config,Reader reader) {
        this(config,reader,System::nanoTime);
    }
    Poller(Config config,Reader reader,LongSupplier clock) {
        this.config=config;this.reader=reader;this.clock=clock;this.policy=new PollPolicy(config.pollSeconds*1000L,config.backoffSeconds*1000L);
        nextDelay=config.pollSeconds;plannedDelayMs=config.pollSeconds*1000L;
    }
    void start() {
        // Spread startup across a fleet; retries cannot form a tight loop.
        long initial=ThreadLocalRandom.current().nextLong(config.pollSeconds*1000L+1);
        nextAttemptNanos=clock.getAsLong()+TimeUnit.MILLISECONDS.toNanos(initial);schedule(initial);
    }
    private void schedule(long ms) {
        if(!closed) try {scheduler.schedule(() -> {
            long delay=runOnce(); if(!closed) schedule(delay);
        },ms,TimeUnit.MILLISECONDS);} catch(RejectedExecutionException ignored) { /* shutting down */ }
    }
    long runOnce() {
        boolean probe;
        synchronized(this) {
            if(closed || !inFlight.compareAndSet(false,true)) return config.pollSeconds*1000L;
            long remaining=nextAttemptNanos-clock.getAsLong();
            if(closed || (circuitOpen && remaining>0)) {
                inFlight.set(false);return Math.max(1,TimeUnit.NANOSECONDS.toMillis(remaining)+1);
            }
            probe=circuitOpen;
            if(probe) {state=State.HALF_OPEN;recoveryAttempts.incrementAndGet();}
            startedNanos=clock.getAsLong();polls.incrementAndGet();
        }
        boolean healthy=false, success=false, timeout=false;
        try {
            Snapshot snapshot=reader.read();
            // A delayed response may describe a state sampled long before it arrived.
            // Fail health promptly while waiting, and never publish that late result as fresh.
            if(clock.getAsLong()-startedNanos>config.socketTimeoutMs*2000000L)
                throw new java.net.SocketTimeoutException("Collection cycle exceeded freshness budget");
            synchronized(this) {
                last.set(new Good(snapshot,clock.getAsLong()));lastSuccess=true;circuitOpen=false;state=State.HEALTHY;
                conflicts.addAndGet(snapshot.conflicts);consecutiveFailures.set(0);timeoutDelay=0;
                if(probe) recoveries.incrementAndGet();
            }
            success=true;
            long elapsed=(clock.getAsLong()-startedNanos)/1000000;
            healthy=elapsed<config.slowPollMs;
            if(!healthy) slowPolls.incrementAndGet();
        } catch(Exception e) {
            for(Throwable cause=e;cause!=null;cause=cause.getCause()) {
                if(cause instanceof java.net.SocketTimeoutException) {timeout=true;break;}
            }
            synchronized(this) {
                lastSuccess=false;failures.incrementAndGet();consecutiveFailures.incrementAndGet();
                if(timeout) timeoutFailures.incrementAndGet();
                circuitOpen=probe || timeout;state=circuitOpen?State.OPEN:State.BACKOFF;
            }
            // No target URL, credential values, or exception text in logs/labels.
            LOG.warning("JMX collection unavailable ("+e.getClass().getSimpleName()+"); automatic retry with backoff");
            reader.close();
        } finally {
            synchronized(this) {
                lastDuration=(clock.getAsLong()-startedNanos)/1e9;
                long delay;
                if(!success && circuitOpen) {
                    long cap=config.maxTimeoutBackoffSeconds*1000L;
                    timeoutDelay=timeoutDelay==0?config.timeoutRetrySeconds*1000L:Math.min(cap,timeoutDelay*2);
                    long room=Math.min(cap-timeoutDelay,Math.max(1,timeoutDelay/10));
                    delay=timeoutDelay+(room>0?ThreadLocalRandom.current().nextLong(room+1):0);
                } else {
                    long base=policy.after(healthy);
                    // Recovery must not deliberately schedule beyond freshness of a good snapshot.
                    if(success && healthy) base=Math.max(config.pollSeconds*1000L,Math.min(base,config.staleSeconds*500L));
                    delay=policy.jittered(base);
                }
                plannedDelayMs=delay;nextDelay=delay/1000.0;nextAttemptNanos=clock.getAsLong()+TimeUnit.MILLISECONDS.toNanos(delay);
                inFlight.set(false);
            }
        }
        return Math.max(1,plannedDelayMs);
    }
    boolean fresh(Good good) {
        return good!=null && lastSuccess && (clock.getAsLong()-good.nanos)<config.staleSeconds*1000000000L
            && !closed && !(inFlight.get() && clock.getAsLong()-startedNanos>config.socketTimeoutMs*2000000L);
    }
    synchronized View view() {
        Good good=last.get();boolean fresh=fresh(good);State actual=state;
        if(closed) actual=State.STOPPED;
        else if(inFlight.get() && clock.getAsLong()-startedNanos>config.socketTimeoutMs*2000000L) actual=State.STALLED;
        else if(actual==State.HEALTHY && !fresh) actual=State.STALE;
        return new View(good,fresh,circuitOpen,actual,inFlight.get()?0:Math.max(0,(nextAttemptNanos-clock.getAsLong())/1e9),
            good==null?-1:Math.max(0,(clock.getAsLong()-good.nanos)/1e9));
    }
    public void close() {
        closed=true;scheduler.shutdownNow();
        // Do not race close/reconnect with a stuck read; daemon worker stays single and bounded.
        if(!inFlight.get()) reader.close();
    }
}
