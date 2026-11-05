package local.cassandra.membership;

import java.io.*;
import java.net.*;
import java.rmi.server.RMISocketFactory;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.*;
import javax.management.remote.*;

final class JmxReader implements Reader {
    static final String[] ATTRIBUTES={"LiveNodesWithPort","UnreachableNodesWithPort","JoiningNodesWithPort",
        "LeavingNodesWithPort","MovingNodesWithPort","GossipRunning","NativeTransportRunning","Initialized","LocalHostId"};
    static final ObjectName STORAGE;
    static { try {STORAGE=new ObjectName("org.apache.cassandra.db:type=StorageService");}
        catch(Exception e) {throw new ExceptionInInitializerError(e);} }
    private final Config config;
    private final AtomicLong reads=new AtomicLong();
    private JMXConnector connector;
    private MBeanServerConnection connection;
    JmxReader(Config config) {this.config=config;}

    // Process-wide only in our standalone JVM. No changes to the Cassandra JVM.
    static void configureTimeouts(int ms) throws IOException {
        System.setProperty("sun.rmi.transport.tcp.responseTimeout",String.valueOf(ms));
        System.setProperty("sun.rmi.transport.tcp.handshakeTimeout",String.valueOf(ms));
        System.setProperty("java.rmi.server.useCodebaseOnly","true");
        RMISocketFactory.setSocketFactory(new RMISocketFactory() {
            public Socket createSocket(String host,int port) throws IOException {
                Socket socket=new Socket();
                try {socket.connect(new InetSocketAddress(host,port),ms);socket.setSoTimeout(ms);return socket;}
                catch(IOException e) {socket.close();throw e;}
            }
            public ServerSocket createServerSocket(int port) throws IOException {return new ServerSocket(port);}
        });
    }
    private void connect() throws IOException {
        Map<String,Object> env=new HashMap<>();
        // Disable JMX client's periodic connection checks: the single poller owns reconnects.
        env.put("jmx.remote.x.client.connection.check.period",0L);
        if(config.usernameEnv!=null) {
            String user=System.getenv(config.usernameEnv), pass=System.getenv(config.passwordEnv);
            if(user==null || pass==null) throw new SecurityException("Missing credential environment variable");
            env.put(JMXConnector.CREDENTIALS,new String[]{user,pass});
        }
        connector=JMXConnectorFactory.connect(new JMXServiceURL(config.jmxUrl),env);
        connection=connector.getMBeanServerConnection();
    }
    public Snapshot read() throws Exception {
        if(connection==null) connect();
        reads.incrementAndGet();
        AttributeList list=connection.getAttributes(STORAGE,ATTRIBUTES);
        Map<String,Object> attrs=new HashMap<>();
        for(Attribute attr:list.asList()) attrs.put(attr.getName(),attr.getValue());
        return Snapshot.from(attrs,Collections.emptySet(),config.maxEndpoints);
    }
    public long reads() {return reads.get();}
    public void close() {
        JMXConnector old=connector;connector=null;connection=null;
        if(old!=null) try {old.close();} catch(IOException ignored) { /* Next cycle reconnects after backoff. */ }
    }
}
