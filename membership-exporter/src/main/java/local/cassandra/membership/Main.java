package local.cassandra.membership;

import java.nio.file.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

public final class Main {
    public static void main(String[] args) throws Exception {
        if(args.length==1 && "--version".equals(args[0])) {
            String version=Main.class.getPackage().getImplementationVersion();
            System.out.println("Casscope "+(version==null?"development":version));return;
        }
        if(args.length!=2 || !"--config".equals(args[0])) {
            System.err.println("Usage: java -jar casscope-<version>.jar --config exporter.yaml | --version");
            System.exit(2);return;
        }
        Config config=Config.load(Paths.get(args[1]));
        JmxReader.configureTimeouts(config.socketTimeoutMs);
        Poller poller=new Poller(config,new JmxReader(config));
        MetricsServer server=new MetricsServer(config,poller);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {poller.close();server.close();},"membership-shutdown"));
        poller.start();Logger.getLogger(Main.class.getName()).info("Casscope started; one background JMX poller, HTTP serves snapshots only");
        new CountDownLatch(1).await();
    }
}
