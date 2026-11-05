package local.cassandra.membership;

import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.snapshots.*;
import java.util.*;

final class MembershipCollector implements MultiCollector {
    private final Poller poller;
    private final Poller.View captured;
    private final boolean healthOnly;
    MembershipCollector(Poller poller) {this(poller,null,false);}
    MembershipCollector(Poller poller,Poller.View captured,boolean healthOnly) {
        this.poller=poller;this.captured=captured;this.healthOnly=healthOnly;
    }
    public MetricSnapshots collect() {
        Poller.View view=captured==null?poller.view():captured;
        Poller.Good good=view.good;boolean fresh=view.fresh;
        List<MetricSnapshot> out=new ArrayList<>();
        scalar(out,"collection_success","A successful fresh JMX snapshot is available",fresh?1:0);
        scalar(out,"snapshot_timestamp_seconds","Last successful snapshot Unix timestamp",good==null?0:good.timestamp);
        scalar(out,"snapshot_age_seconds","Age of last successful snapshot; -1 before first success",view.age);
        scalar(out,"poll_in_flight","One JMX cycle is in flight",poller.inFlight.get()?1:0);
        scalar(out,"circuit_open","Timeout recovery circuit active; only scheduled probes may read JMX",view.circuit?1:0);
        scalar(out,"polls_total","JMX polling cycles attempted",poller.polls.get());
        scalar(out,"jmx_reads_total","Application getAttributes calls; excludes RMI connection/control traffic",poller.reader.reads());
        scalar(out,"collection_errors_total","Failed JMX polling cycles",poller.failures.get());
        scalar(out,"slow_polls_total","Successful cycles slower than the configured threshold",poller.slowPolls.get());
        scalar(out,"state_conflicts_total","Contradictory state lists observed; no immediate retries",poller.conflicts.get());
        scalar(out,"last_poll_duration_seconds","Duration of last completed JMX cycle",poller.lastDuration);
        scalar(out,"next_poll_delay_seconds","Planned delay after a completed cycle",poller.nextDelay);
        scalar(out,"retry_after_seconds","Time until next planned attempt; zero while in flight",view.retryAfter);
        scalar(out,"consecutive_failures","Consecutive failed collection cycles",poller.consecutiveFailures.get());
        scalar(out,"timeouts_total","Collection cycles ending in a socket timeout",poller.timeoutFailures.get());
        scalar(out,"recovery_attempts_total","Scheduled timeout recovery probes",poller.recoveryAttempts.get());
        scalar(out,"recoveries_total","Successful collections that closed the timeout circuit",poller.recoveries.get());
        GaugeSnapshot.Builder states=gauge("collector_state","Collector state, one hot; independent of Cassandra peer state");
        for(Poller.State state:Poller.State.values()) states.dataPoint(point(view.state==state?1:0,"state",state.name().toLowerCase(Locale.ROOT)));
        out.add(states.build());
        if(fresh && !healthOnly) {
            Snapshot s=good.snapshot;
            scalar(out,"gossip_running","Local gossip enabled",s.gossip?1:0);
            scalar(out,"native_transport_running","Local CQL transport enabled",s.nativeTransport?1:0);
            scalar(out,"initialized","Local Cassandra initialized",s.initialized?1:0);
            scalar(out,"endpoints","Endpoints in gossip membership lists",s.peers.size());
            GaugeSnapshot.Builder status=gauge("endpoint_status","Peer availability from this observer");
            GaugeSnapshot.Builder ring=gauge("endpoint_ring_state","Peer ring state independent of availability");
            for(Map.Entry<String,Snapshot.Peer> entry:s.peers.entrySet()) {
                for(String value:Snapshot.STATUSES) status.dataPoint(point(entry.getValue().status.equals(value)?1:0,"endpoint",entry.getKey(),"status",value));
                for(String value:Snapshot.STATES) ring.dataPoint(point(entry.getValue().state.equals(value)?1:0,"endpoint",entry.getKey(),"state",value));
            }
            out.add(status.build());out.add(ring.build());
            out.add(gauge("observer_info","Observer host ID").dataPoint(point(1,"host_id",s.observerHostId)).build());
        }
        return new MetricSnapshots(out);
    }
    private GaugeSnapshot.Builder gauge(String name,String help) {
        return GaugeSnapshot.builder().name("cassandra_membership_"+name).help(help);
    }
    private GaugeSnapshot.GaugeDataPointSnapshot point(double value,String... extra) {
        String[] keys=new String[2+extra.length/2], values=new String[keys.length];
        keys[0]="cluster";values[0]=poller.config.cluster;keys[1]="observer";values[1]=poller.config.observer;
        for(int i=0;i<extra.length;i+=2) {keys[2+i/2]=extra[i];values[2+i/2]=extra[i+1];}
        return GaugeSnapshot.GaugeDataPointSnapshot.builder().value(value).labels(Labels.of(keys,values)).build();
    }
    private void scalar(List<MetricSnapshot> out,String name,String help,double value) {
        if(name.endsWith("_total")) {
            out.add(CounterSnapshot.builder().name("cassandra_membership_"+name.substring(0,name.length()-6)).help(help)
                .dataPoint(CounterSnapshot.CounterDataPointSnapshot.builder().value(value)
                    .labels(Labels.of(new String[]{"cluster","observer"},new String[]{poller.config.cluster,poller.config.observer})).build()).build());
        } else out.add(gauge(name,help).dataPoint(point(value)).build());
    }
}
