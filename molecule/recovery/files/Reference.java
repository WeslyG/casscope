import javax.management.*;
import javax.management.remote.*;
import java.util.*;

/** Independent test recorder: no Casscope classes, mapper, or counters. */
public class Reference {
    static String json(Object o) {
        if (o == null) return "null";
        if (o instanceof Boolean || o instanceof Number) return o.toString();
        if (o instanceof Collection) {
            List<String> a = new ArrayList<>();
            for (Object v : (Collection<?>) o) a.add(json(v));
            return "[" + String.join(",", a) + "]";
        }
        return "\"" + o.toString().replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }
    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        Map<String,Object> env = new HashMap<>();
        env.put("jmx.remote.x.client.connection.check.period", 0L);
        try (JMXConnector c = JMXConnectorFactory.connect(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:7199/jmxrmi"), env)) {
            MBeanServerConnection m = c.getMBeanServerConnection();
            if (args.length > 0 && args[0].equals("logging")) {
                m.invoke(new ObjectName("java.util.logging:type=Logging"), "setLoggerLevel",
                    new Object[]{"javax.management.remote.rmi", args[1]}, new String[]{"java.lang.String", "java.lang.String"});
                System.out.println("{\"logging\":" + json(args[1]) + "}"); return;
            }
            String[] attrs = {"LiveNodesWithPort", "UnreachableNodesWithPort", "JoiningNodesWithPort", "LeavingNodesWithPort", "MovingNodesWithPort", "GossipRunning", "NativeTransportRunning", "Initialized", "LocalHostId"};
            do {
                start = System.currentTimeMillis();
                AttributeList list = m.getAttributes(new ObjectName("org.apache.cassandra.db:type=StorageService"), attrs);
                List<String> a = new ArrayList<>();
                for (Attribute v : list.asList()) a.add(json(v.getName()) + ":" + json(v.getValue()));
                System.out.println("{\"start_ms\":" + start + ",\"end_ms\":" + System.currentTimeMillis() + ",\"attributes\":{" + String.join(",", a) + "}}");
                System.out.flush();
                if (args.length == 0 || !args[0].equals("watch")) break;
                Thread.sleep(5000);
            } while (true);
        }
    }
}
