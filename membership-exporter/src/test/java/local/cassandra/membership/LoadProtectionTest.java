package local.cassandra.membership;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class LoadProtectionTest {
    static final class FakeReader implements Reader {
        AtomicLong calls=new AtomicLong(); volatile Exception error; volatile CountDownLatch gate;
        public Snapshot read() throws Exception {calls.incrementAndGet();if(gate!=null)gate.await();if(error!=null)throw error;return Snapshot.from(ModelTest.attrs(),Collections.emptySet(),10);}
        public long reads(){return calls.get();} public void close(){}
    }
    @Test void aStalledReadCannotSpawnParallelQueries() throws Exception {
        FakeReader r=new FakeReader();r.gate=new CountDownLatch(1);Poller p=new Poller(new Config(),r);
        ExecutorService workers=Executors.newFixedThreadPool(8);
        try {
            Future<?> blocked=workers.submit(p::runOnce);
            for(int i=0;i<100 && r.calls.get()==0;i++)Thread.sleep(5);
            List<Future<?>> attempts=new ArrayList<>();for(int i=0;i<200;i++) attempts.add(workers.submit(p::runOnce));
            for(Future<?> attempt:attempts)attempt.get(3,TimeUnit.SECONDS);
            assertThat(r.calls.get()).isEqualTo(1);assertThat(p.inFlight.get()).isTrue();
            r.gate.countDown();blocked.get(3,TimeUnit.SECONDS);
        } finally {r.gate.countDown();p.close();workers.shutdownNow();}
    }
    @Test void timeoutOpensCircuitWithoutRepeatedAbandonedServerCalls() {
        FakeReader r=new FakeReader();r.error=new java.io.IOException("wrapped",new java.net.SocketTimeoutException("read timed out"));
        Poller p=new Poller(new Config(),r);
        try {
            p.runOnce();for(int i=0;i<1000;i++)p.runOnce();
            assertThat(r.calls.get()).isEqualTo(1);assertThat(p.circuitOpen).isTrue();assertThat(p.lastSuccess).isFalse();
        } finally {p.close();}
    }
    @Test void slowSuccessfulReadBacksOffAndErrorsInvalidateCachedPeerState() throws Exception {
        Config c=new Config();c.slowPollMs=1;FakeReader r=new FakeReader();r.gate=new CountDownLatch(1);
        Poller p=new Poller(c,r);Thread release=new Thread(() -> {try{Thread.sleep(30);}catch(InterruptedException ignored){}r.gate.countDown();});release.start();
        try {
            assertThat(p.runOnce()).isGreaterThanOrEqualTo(60000);assertThat(p.slowPolls.get()).isEqualTo(1);
            assertThat(p.fresh(p.last.get())).isTrue();r.error=new java.io.IOException("broken");p.runOnce();
            assertThat(p.fresh(p.last.get())).isFalse();
            Set<String> names=new HashSet<>();new MembershipCollector(p).collect().forEach(s -> names.add(s.getMetadata().getName()));
            assertThat(names).doesNotContain("cassandra_membership_endpoint_status");
        } finally {release.join();p.close();}
    }
}
