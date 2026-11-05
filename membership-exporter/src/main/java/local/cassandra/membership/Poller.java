package local.cassandra.membership;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.Logger;

final class Poller implements AutoCloseable {
    static final class Good {
        final Snapshot snapshot; final long nanos; final double timestamp;
        Good(Snapshot snapshot) {this.snapshot=snapshot;this.nanos=System.nanoTime();this.timestamp=System.currentTimeMillis()/1000.0;}
    }
    private static final Logger LOG=Logger.getLogger(Poller.class.getName());
    final Config config; final Reader reader;
    final AtomicReference<Good> last=new AtomicReference<>();
    final AtomicLong polls=new AtomicLong(), failures=new AtomicLong(), slowPolls=new AtomicLong(), conflicts=new AtomicLong();
    final AtomicBoolean inFlight=new AtomicBoolean();
    private final ScheduledExecutorService scheduler=Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t=new Thread(r,"membership-poller");t.setDaemon(true);return t;
    });
    private final PollPolicy policy;
    volatile boolean lastSuccess, closed, circuitOpen;
    volatile long startedNanos;
    volatile double lastDuration, nextDelay;
    Poller(Config config,Reader reader) {
        this.config=config;this.reader=reader;this.policy=new PollPolicy(config.pollSeconds*1000L,config.backoffSeconds*1000L);
        nextDelay=config.pollSeconds;
    }
    void start() {
        // Spread startup across a fleet; retries cannot form a tight loop.
        schedule(ThreadLocalRandom.current().nextLong(config.pollSeconds*1000L+1));
    }
    private void schedule(long ms) {
        if(!closed) try {scheduler.schedule(() -> {
            long delay=runOnce(); if(!closed && !circuitOpen) schedule(delay);
        },ms,TimeUnit.MILLISECONDS);} catch(RejectedExecutionException ignored) { /* shutting down */ }
    }
    long runOnce() {
        if(closed || circuitOpen || !inFlight.compareAndSet(false,true)) return config.pollSeconds*1000L;
        startedNanos=System.nanoTime();polls.incrementAndGet(); boolean healthy=false;
        try {
            Snapshot snapshot=reader.read();
            last.set(new Good(snapshot));lastSuccess=true;conflicts.addAndGet(snapshot.conflicts);
            long elapsed=(System.nanoTime()-startedNanos)/1000000;
            healthy=elapsed<config.slowPollMs;
            if(!healthy) slowPolls.incrementAndGet();
        } catch(Exception e) {
            lastSuccess=false;failures.incrementAndGet();
            // RMI timeout cannot cancel server-side work. Do not accumulate more abandoned reads.
            for(Throwable cause=e;cause!=null;cause=cause.getCause()) {
                if(cause instanceof java.net.SocketTimeoutException) {circuitOpen=true;break;}
            }
            // No target URL, credential values, or exception text in logs/labels.
            LOG.warning("JMX poll failed ("+e.getClass().getSimpleName()+"); delaying next attempt");
            reader.close();
        } finally {
            lastDuration=(System.nanoTime()-startedNanos)/1e9;inFlight.set(false);
        }
        long delay=policy.jittered(policy.after(healthy));nextDelay=circuitOpen?0:delay/1000.0;return delay;
    }
    boolean fresh(Good good) {
        return good!=null && lastSuccess && (System.nanoTime()-good.nanos)<config.staleSeconds*1000000000L
            && !(inFlight.get() && System.nanoTime()-startedNanos>config.socketTimeoutMs*2000000L);
    }
    public void close() {
        closed=true;scheduler.shutdownNow();
        // Do not race close/reconnect with a stuck read; daemon worker stays single and bounded.
        if(!inFlight.get()) reader.close();
    }
}
