package local.cassandra.membership;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.prometheus.metrics.exporter.httpserver.MetricsHandler;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.*;

/** HTTP never initiates JMX. Health and membership use the same captured view. */
final class MetricsServer implements AutoCloseable {
    private final HttpServer server;
    private final ExecutorService workers;
    MetricsServer(Config config,Poller poller) throws IOException {
        this(config,poller,config.port);
    }
    MetricsServer(Config config,Poller poller,int port) throws IOException {
        server=HttpServer.create(new InetSocketAddress(config.listenAddress,port),16);
        workers=new ThreadPoolExecutor(2,2,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(16),r -> {
            Thread t=new Thread(r,"membership-http");t.setDaemon(true);return t;
        },new ThreadPoolExecutor.AbortPolicy());
        server.setExecutor(workers);
        server.createContext("/",exchange -> {
            String path=exchange.getRequestURI().getPath();
            if(!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow","GET");reply(exchange,405,"Method not allowed\n");return;
            }
            if("/-/healthy".equals(path)) {reply(exchange,200,"Casscope process is running\n");return;}
            boolean diagnostics="/diagnostics/metrics".equals(path);
            if(!diagnostics && !"/metrics".equals(path) && !"/-/ready".equals(path)) {
                reply(exchange,404,"Not found\n");return;
            }
            Poller.View view=poller.view();
            if("/-/ready".equals(path) && !view.fresh) {
                if(view.retryAfter>0) exchange.getResponseHeaders().set("Retry-After",Long.toString((long)Math.ceil(view.retryAfter)));
                reply(exchange,503,"JMX collection unavailable: state="+view.state.name().toLowerCase(Locale.ROOT)
                    +"; inspect /metrics\n");return;
            }
            if("/-/ready".equals(path)) {reply(exchange,200,"Fresh JMX collection is available\n");return;}
            PrometheusRegistry registry=new PrometheusRegistry();
            registry.register(new MembershipCollector(poller,view,diagnostics));
            new MetricsHandler(registry).handle(exchange);
        });
        server.start();
    }
    int port() {return server.getAddress().getPort();}
    private static void reply(HttpExchange exchange,int code,String text) throws IOException {
        byte[] body=text.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type","text/plain; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control","no-store");
        exchange.sendResponseHeaders(code,body.length);
        try(OutputStream output=exchange.getResponseBody()) {output.write(body);} finally {exchange.close();}
    }
    public void close() {server.stop(0);workers.shutdownNow();}
}
