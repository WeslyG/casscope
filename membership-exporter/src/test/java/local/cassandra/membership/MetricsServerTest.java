package local.cassandra.membership;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MetricsServerTest {
    private static void assertUnavailable(int port) throws Exception {
        assertThat(JmxHttpTest.status(port,"/metrics")).isEqualTo(200);
        assertThat(JmxHttpTest.scrape(port))
            .contains("cassandra_membership_collection_success{cluster=\"cassandra\",observer=\"local\"} 0.0",
                "cassandra_membership_collection_errors_total","cassandra_membership_retry_after_seconds")
            .doesNotContain("cassandra_membership_endpoint_status","cassandra_membership_endpoint_ring_state",
                "cassandra_membership_gossip_running","cassandra_membership_native_transport_running",
                "cassandra_membership_initialized","cassandra_membership_observer_info","cassandra_membership_endpoints{");
        assertThat(JmxHttpTest.status(port,"/-/ready")).isEqualTo(503);
    }
    @Test void startingAPollCannotExposeItsPredecessorsOldStartTime() throws Exception {
        Config c=new Config();AtomicLong clock=new AtomicLong();
        LoadProtectionTest.FakeReader reader=new LoadProtectionTest.FakeReader();Poller p=new Poller(c,reader,clock::get);
        Thread worker=new Thread(p::runOnce);
        try {
            p.runOnce();clock.addAndGet(TimeUnit.SECONDS.toNanos(31));
            synchronized(p) {
                worker.start();
                for(int i=0;i<200 && worker.getState()!=Thread.State.BLOCKED;i++) Thread.sleep(5);
                assertThat(worker.getState()).isEqualTo(Thread.State.BLOCKED);
                assertThat(p.inFlight.get()).isFalse();assertThat(p.view().fresh).isTrue();
            }
            worker.join(3000);assertThat(worker.isAlive()).isFalse();
        } finally {p.close();worker.join(3000);}
    }
    @Test void startupFailureStaleAndRecoveryAreVisibleWithoutScrapeTriggeredJmx() throws Exception {
        Config c=new Config();AtomicLong clock=new AtomicLong();
        LoadProtectionTest.FakeReader reader=new LoadProtectionTest.FakeReader();
        Poller p=new Poller(c,reader,clock::get);
        try(MetricsServer http=new MetricsServer(c,p,0)) {
            assertUnavailable(http.port());
            assertThat(JmxHttpTest.status(http.port(),"/-/healthy")).isEqualTo(200);
            assertThat(reader.calls.get()).isZero();
            p.runOnce();assertThat(JmxHttpTest.status(http.port(),"/metrics")).isEqualTo(200);
            assertThat(JmxHttpTest.scrape(http.port())).contains("cassandra_membership_observer_info");
            assertThat(JmxHttpTest.scrape(http.port(),"/diagnostics/metrics")).doesNotContain("cassandra_membership_endpoint_status","cassandra_membership_observer_info");
            clock.addAndGet(TimeUnit.SECONDS.toNanos(91));
            for(int i=0;i<100;i++) assertUnavailable(http.port());
            assertThat(reader.calls.get()).isEqualTo(1);
            assertThat(JmxHttpTest.scrape(http.port())).contains("state=\"stale\"} 1.0");
            p.runOnce();reader.error=new java.net.SocketTimeoutException();long delay=p.runOnce();
            assertUnavailable(http.port());
            for(int i=0;i<100;i++) assertUnavailable(http.port());
            assertThat(reader.calls.get()).isEqualTo(3);
            reader.error=null;clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(delay));p.runOnce();
            assertThat(JmxHttpTest.status(http.port(),"/metrics")).isEqualTo(200);
            assertThat(JmxHttpTest.scrape(http.port())).contains("cassandra_membership_circuit_open{cluster=\"cassandra\",observer=\"local\"} 0.0");
        } finally {p.close();}
    }
    @Test void stuckWorkerFailsReadinessAndNoSecondReadStarts() throws Exception {
        Config c=new Config();AtomicLong clock=new AtomicLong();
        LoadProtectionTest.FakeReader reader=new LoadProtectionTest.FakeReader();
        Poller p=new Poller(c,reader,clock::get);ExecutorService thread=Executors.newSingleThreadExecutor();
        try(MetricsServer http=new MetricsServer(c,p,0)) {
            p.runOnce();reader.gate=new CountDownLatch(1);Future<?> work=thread.submit(p::runOnce);
            for(int i=0;i<100 && reader.calls.get()<2;i++) Thread.sleep(5);
            clock.addAndGet(TimeUnit.SECONDS.toNanos(7));
            for(int i=0;i<100;i++) {p.runOnce();assertUnavailable(http.port());}
            assertThat(reader.calls.get()).isEqualTo(2);assertThat(p.view().state).isEqualTo(Poller.State.STALLED);
            reader.gate.countDown();work.get(3,TimeUnit.SECONDS);
            assertUnavailable(http.port());
            assertThat(p.circuitOpen).isTrue();
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos((long)Math.ceil(p.nextDelay*1000)));
            p.runOnce();
            assertThat(JmxHttpTest.status(http.port(),"/metrics")).isEqualTo(200);
        } finally {if(reader.gate!=null)reader.gate.countDown();thread.shutdownNow();p.close();}
    }
    @Test void freshDownAndDisabledCqlArePublishedWithSuccessfulCollection() throws Exception {
        Config c=new Config();
        java.util.Map<String,Object> attrs=ModelTest.attrs();
        attrs.put("NativeTransportRunning",false);
        attrs.put("UnreachableNodesWithPort",java.util.Arrays.asList("10.0.0.2:7000"));
        Reader reader=new Reader() {
            public Snapshot read() {return Snapshot.from(attrs,java.util.Collections.emptySet(),10);}
            public long reads() {return 1;}
            public void close() {}
        };
        Poller p=new Poller(c,reader);
        try(MetricsServer http=new MetricsServer(c,p,0)) {
            p.runOnce();
            assertThat(JmxHttpTest.status(http.port(),"/metrics")).isEqualTo(200);
            assertThat(JmxHttpTest.status(http.port(),"/-/ready")).isEqualTo(200);
            assertThat(JmxHttpTest.scrape(http.port())).contains(
                "cassandra_membership_collection_success{cluster=\"cassandra\",observer=\"local\"} 1.0",
                "cassandra_membership_native_transport_running{cluster=\"cassandra\",observer=\"local\"} 0.0",
                "cassandra_membership_endpoint_status{cluster=\"cassandra\",endpoint=\"10.0.0.2:7000\",observer=\"local\",status=\"down\"} 1.0");
        } finally {p.close();}
    }
}
