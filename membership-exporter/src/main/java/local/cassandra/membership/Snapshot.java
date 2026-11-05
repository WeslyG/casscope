package local.cassandra.membership;

import java.util.*;

final class Snapshot {
    static final String[] STATUSES={"up","down","unknown"};
    static final String[] STATES={"normal","joining","leaving","moving","unknown"};
    final Map<String, Peer> peers;
    final boolean gossip, nativeTransport, initialized;
    final String observerHostId;
    final long conflicts;
    Snapshot(Map<String,Peer> peers, boolean gossip, boolean nativeTransport, boolean initialized, String hostId, long conflicts) {
        this.peers=Collections.unmodifiableMap(new TreeMap<>(peers));
        this.gossip=gossip; this.nativeTransport=nativeTransport; this.initialized=initialized;
        this.observerHostId=hostId; this.conflicts=conflicts;
    }
    static final class Peer {
        final String status, state;
        Peer(String status,String state) {this.status=status;this.state=state;}
    }
    static Snapshot from(Map<String,Object> attrs, Set<String> known, int cap) {
        Set<String> live=list(attrs,"LiveNodesWithPort",cap), dead=list(attrs,"UnreachableNodesWithPort",cap);
        Set<String> join=list(attrs,"JoiningNodesWithPort",cap), leave=list(attrs,"LeavingNodesWithPort",cap);
        Set<String> move=list(attrs,"MovingNodesWithPort",cap);
        Set<String> all=new TreeSet<>(known);
        for(Set<String> set:Arrays.asList(live,dead,join,leave,move)) all.addAll(set);
        if(all.size()>cap) throw new IllegalArgumentException("Endpoint limit exceeded");
        Map<String,Peer> peers=new TreeMap<>(); long conflicts=0;
        for(String ep:all) {
            boolean u=live.contains(ep), d=dead.contains(ep);
            String status=u==d?"unknown":u?"up":"down";
            if(u&&d) conflicts++;
            int transitions=(join.contains(ep)?1:0)+(leave.contains(ep)?1:0)+(move.contains(ep)?1:0);
            String state=transitions>1?"unknown":join.contains(ep)?"joining":leave.contains(ep)?"leaving":move.contains(ep)?"moving":"normal";
            if(transitions>1) conflicts++;
            peers.put(ep,new Peer(status,state));
        }
        Object id=attrs.get("LocalHostId");
        if(!(id instanceof String)) throw new IllegalArgumentException("Missing LocalHostId");
        return new Snapshot(peers,bool(attrs,"GossipRunning"),bool(attrs,"NativeTransportRunning"),bool(attrs,"Initialized"),(String)id,conflicts);
    }
    private static boolean bool(Map<String,Object> attrs,String name) {
        Object value=attrs.get(name); if(!(value instanceof Boolean)) throw new IllegalArgumentException("Missing boolean attribute: "+name);
        return (Boolean)value;
    }
    private static Set<String> list(Map<String,Object> attrs,String name,int cap) {
        Object value=attrs.get(name); if(!(value instanceof List)) throw new IllegalArgumentException("Missing list attribute: "+name);
        List<?> values=(List<?>)value; if(values.size()>cap) throw new IllegalArgumentException("Endpoint limit exceeded");
        Set<String> out=new HashSet<>(); for(Object item:values) {
            if(!(item instanceof String)) throw new IllegalArgumentException("Invalid endpoint type"); out.add(Endpoint.normalize((String)item));
        }
        return out;
    }
}
