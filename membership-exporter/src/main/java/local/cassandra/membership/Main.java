package local.cassandra.membership;

import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
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
        PrometheusRegistry registry=new PrometheusRegistry();registry.register(new MembershipCollector(poller));
        ThreadPoolExecutor http=new ThreadPoolExecutor(2,2,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(16),r -> {
            Thread t=new Thread(r,"membership-http");t.setDaemon(true);return t;
        },new ThreadPoolExecutor.AbortPolicy());
        HTTPServer server=HTTPServer.builder().hostname(config.listenAddress).port(config.port).registry(registry).executorService(http).buildAndStart();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {poller.close();server.close();http.shutdownNow();},"membership-shutdown"));
        poller.start();Logger.getLogger(Main.class.getName()).info("Casscope started; one background JMX poller, HTTP serves snapshots only");
        new CountDownLatch(1).await();
    }
}
