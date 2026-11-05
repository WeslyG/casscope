package local.cassandra.membership;

import java.net.*;
import java.util.Locale;

final class Endpoint {
    private Endpoint() {}
    static String normalize(String raw) {
        if (raw==null || raw.length()>256) throw new IllegalArgumentException("Invalid endpoint");
        String s=raw.trim(); int slash=s.lastIndexOf('/'); if(slash>=0) s=s.substring(slash+1);
        String host, port;
        if(s.startsWith("[")) {
            int end=s.indexOf(']');
            if(end<0 || end+1>=s.length() || s.charAt(end+1)!=':') throw new IllegalArgumentException("IPv6 endpoint needs port");
            host=s.substring(1,end); port=s.substring(end+2);
        } else {
            int colon=s.lastIndexOf(':');
            if(colon<1 || s.indexOf(':')!=colon) throw new IllegalArgumentException("Endpoint needs explicit port/bracketed IPv6");
            host=s.substring(0,colon); port=s.substring(colon+1);
        }
        int p=Integer.parseInt(port); if(p<1 || p>65535) throw new IllegalArgumentException("Invalid port");
        if(host.contains(":")) {
            if(!host.matches("[0-9a-fA-F:.]+")) throw new IllegalArgumentException("Only literal IPv6 accepted");
            try { host=InetAddress.getByName(host).getHostAddress().toLowerCase(Locale.ROOT); }
            catch(UnknownHostException e) { throw new IllegalArgumentException("Invalid IPv6"); }
            // The JDK canonicalizes IPv4-mapped IPv6 literals to Inet4Address.
            return host.indexOf(':')<0?host+":"+p:"["+host+"]:"+p;
        }
        String[] octets=host.split("\\.", -1);
        if(octets.length!=4) throw new IllegalArgumentException("Only literal IPv4 accepted");
        StringBuilder ip=new StringBuilder();
        for(String octet:octets) {
            if(!octet.matches("[0-9]{1,3}")) throw new IllegalArgumentException("Invalid IPv4");
            int n=Integer.parseInt(octet); if(n>255) throw new IllegalArgumentException("Invalid IPv4");
            if(ip.length()>0) ip.append('.'); ip.append(n);
        }
        return ip+":"+p;
    }
}
