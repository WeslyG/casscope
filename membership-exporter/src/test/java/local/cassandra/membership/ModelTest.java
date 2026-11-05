package local.cassandra.membership;

import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ModelTest {
    static Map<String,Object> attrs() {
        Map<String,Object> a=new HashMap<>();
        for(String key:Arrays.asList("LiveNodesWithPort","UnreachableNodesWithPort","JoiningNodesWithPort","LeavingNodesWithPort","MovingNodesWithPort")) a.put(key,Collections.emptyList());
        a.put("GossipRunning",true);a.put("NativeTransportRunning",true);a.put("Initialized",true);a.put("LocalHostId","observer-host-id");return a;
    }
    @Test void independentAvailabilityAndRingStateAndConflict() {
        Map<String,Object> a=attrs();a.put("UnreachableNodesWithPort",Arrays.asList("10.0.0.2:7000"));
        a.put("JoiningNodesWithPort",Arrays.asList("10.0.0.2:7000"));
        Snapshot s=Snapshot.from(a,Collections.singleton("10.0.0.3:7000"),10);
        assertThat(s.peers.get("10.0.0.2:7000").status).isEqualTo("down");
        assertThat(s.peers.get("10.0.0.2:7000").state).isEqualTo("joining");
        assertThat(s.peers.get("10.0.0.3:7000").status).isEqualTo("unknown");
        a.put("LiveNodesWithPort",Arrays.asList("10.0.0.2:7000"));
        a.put("LeavingNodesWithPort",Arrays.asList("10.0.0.2:7000"));
        s=Snapshot.from(a,Collections.emptySet(),10);
        assertThat(s.peers.get("10.0.0.2:7000").status).isEqualTo("unknown");
        assertThat(s.peers.get("10.0.0.2:7000").state).isEqualTo("unknown");assertThat(s.conflicts).isEqualTo(2);
    }
    @Test void missingListIsFailureNotNormalAndCapIsEnforced() {
        Map<String,Object> missing=attrs();missing.remove("MovingNodesWithPort");
        assertThatThrownBy(() -> Snapshot.from(missing,Collections.emptySet(),10)).isInstanceOf(IllegalArgumentException.class);
        Map<String,Object> a=attrs();a.put("LiveNodesWithPort",Arrays.asList("10.0.0.1:7000","10.0.0.2:7000"));
        Map<String,Object> tooMany=a;
        assertThatThrownBy(() -> Snapshot.from(tooMany,Collections.emptySet(),1)).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void addressesDoNotUseDnsAndPreservePort() {
        assertThat(Endpoint.normalize("host/010.0.0.2:7001")).isEqualTo("10.0.0.2:7001");
        assertThat(Endpoint.normalize("/[2001:db8::1]:7000")).isEqualTo("[2001:db8:0:0:0:0:0:1]:7000");
        assertThat(Endpoint.normalize("[::ffff:10.0.0.2]:7000")).isEqualTo("10.0.0.2:7000");
        assertThatThrownBy(() -> Endpoint.normalize("2001:db8::1:7000")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Endpoint.normalize("arbitrary-host.example:7000")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Endpoint.normalize("300.0.0.1:7000")).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void backoffCapsAndJitterNeverShortensMinimum() {
        PollPolicy p=new PollPolicy(30000,300000);
        assertThat(p.after(false)).isEqualTo(60000);assertThat(p.after(false)).isEqualTo(120000);
        assertThat(p.after(false)).isEqualTo(240000);assertThat(p.after(false)).isEqualTo(300000);
        assertThat(p.after(true)).isEqualTo(150000);
        for(int i=0;i<100;i++) assertThat(p.jittered(30000)).isBetween(30000L,33000L);
    }
    @Test void unsafeConfigAndUnknownKeysAreRejected() throws Exception {
        Config c=new Config();c.pollSeconds=1;assertThatThrownBy(c::validate).isInstanceOf(IllegalArgumentException.class);
        Path path=Files.createTempFile("membership-config",".yaml");
        try {
            Files.write(path,"unknown: value\n".getBytes(StandardCharsets.UTF_8));
            assertThatThrownBy(() -> Config.load(path)).isInstanceOf(IllegalArgumentException.class);
            Files.write(path,"cluster: a\ncluster: b\n".getBytes(StandardCharsets.UTF_8));
            assertThatThrownBy(() -> Config.load(path)).isInstanceOf(RuntimeException.class);
            Files.write(path,"!!java.net.URL [http://example.invalid]\n".getBytes(StandardCharsets.UTF_8));
            assertThatThrownBy(() -> Config.load(path)).isInstanceOf(RuntimeException.class);
        } finally {Files.deleteIfExists(path);}
    }
}
