package local.cassandra.membership;

import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.rmi.registry.*;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.*;
import javax.management.remote.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class JmxHttpTest {
    static class Bean implements DynamicMBean {
        final AtomicInteger batches=new AtomicInteger(), singles=new AtomicInteger(), invokes=new AtomicInteger();
        volatile Map<String,Object> data=ModelTest.attrs();
        public Object getAttribute(String name) {singles.incrementAndGet();return data.get(name);}
        public AttributeList getAttributes(String[] names) {
            batches.incrementAndGet();AttributeList list=new AttributeList();
            for(String name:names)if(data.containsKey(name))list.add(new Attribute(name,data.get(name)));return list;
        }
        public void setAttribute(Attribute a){throw new UnsupportedOperationException();}
        public AttributeList setAttributes(AttributeList a){throw new UnsupportedOperationException();}
        public Object invoke(String n,Object[] p,String[] s){invokes.incrementAndGet();throw new UnsupportedOperationException();}
        public MBeanInfo getMBeanInfo() {
            List<MBeanAttributeInfo> infos=new ArrayList<>();
            for(String name:JmxReader.ATTRIBUTES)infos.add(new MBeanAttributeInfo(name,Object.class.getName(),name,true,false,false));
            return new MBeanInfo(getClass().getName(),"Fake StorageService",infos.toArray(new MBeanAttributeInfo[0]),null,null,null);
        }
    }
    @Test void realRmiTimeoutKeepsHttpAliveAndStopsFurtherReadAttempts() throws Exception {
        int rmiPort, httpPort;
        try(ServerSocket a=new ServerSocket(0);ServerSocket b=new ServerSocket(0)){rmiPort=a.getLocalPort();httpPort=b.getLocalPort();}
        Registry rmi=LocateRegistry.createRegistry(rmiPort);MBeanServer mbeans=MBeanServerFactory.createMBeanServer();
        Bean bean=new Bean(){public AttributeList getAttributes(String[] names){
            batches.incrementAndGet();try{Thread.sleep(2000);}catch(InterruptedException e){Thread.currentThread().interrupt();}return new AttributeList();
        }};
        mbeans.registerMBean(bean,JmxReader.STORAGE);
        JMXServiceURL url=new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:"+rmiPort+"/jmxrmi");
        JMXConnectorServer jmx=JMXConnectorServerFactory.newJMXConnectorServer(url,null,mbeans);jmx.start();
        java.nio.file.Path config=java.nio.file.Files.createTempFile("membership-timeout",".yaml");
        java.nio.file.Path log=java.nio.file.Files.createTempFile("membership-timeout",".log");
        Process child=null;
        try {
            String yaml="jmx_url: "+url+"\nport: "+httpPort+"\npoll_interval_seconds: 15\nsocket_timeout_ms: 200\nslow_poll_ms: 100\n";
            java.nio.file.Files.write(config,yaml.getBytes(StandardCharsets.UTF_8));
            String java=System.getProperty("java.home")+File.separator+"bin"+File.separator+"java";
            String cp=System.getProperty("surefire.test.class.path",System.getProperty("java.class.path"));
            child=new ProcessBuilder(java,"-cp",cp,Main.class.getName(),"--config",config.toString()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
            String body="";long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(25);
            while(System.nanoTime()<deadline) {
                try{body=scrape(httpPort);}catch(IOException ignored){}
                if(body.contains("cassandra_membership_circuit_open{cluster=\"cassandra\",observer=\"local\"} 1.0"))break;
                Thread.sleep(100);
            }
            assertThat(body).contains("cassandra_membership_circuit_open{cluster=\"cassandra\",observer=\"local\"} 1.0");
            int reads=bean.batches.get();assertThat(reads).isBetween(1,2);
            Thread.sleep(2500);
            for(int i=0;i<30;i++)assertThat(scrape(httpPort)).doesNotContain("cassandra_membership_endpoint_status");
            assertThat(bean.batches.get()).isEqualTo(reads);assertThat(child.isAlive()).isTrue();
        } finally {
            if(child!=null){child.destroy();if(!child.waitFor(5,TimeUnit.SECONDS))child.destroyForcibly();}
            jmx.stop();UnicastRemoteObject.unexportObject(rmi,true);MBeanServerFactory.releaseMBeanServer(mbeans);
            java.nio.file.Files.deleteIfExists(config);java.nio.file.Files.deleteIfExists(log);
        }
    }
    static String scrape(int port) throws Exception {
        HttpURLConnection connection=(HttpURLConnection)new URL("http://127.0.0.1:"+port+"/metrics").openConnection();
        connection.setConnectTimeout(3000);connection.setReadTimeout(3000);connection.setRequestProperty("Accept","text/plain;version=0.0.4");
        try(InputStream input=connection.getInputStream();ByteArrayOutputStream bytes=new ByteArrayOutputStream()) {
            byte[] buf=new byte[8192];int n;while((n=input.read(buf))!=-1)bytes.write(buf,0,n);return bytes.toString("UTF-8");
        } finally {connection.disconnect();}
    }
    @Test void realRmiAndTwoHundredHttpScrapesUseOneBatchReadAndRemoveOldLabelsets() throws Exception {
        int port;try(ServerSocket socket=new ServerSocket(0)){port=socket.getLocalPort();}
        Registry rmi=LocateRegistry.createRegistry(port);
        MBeanServer mbeans=MBeanServerFactory.createMBeanServer();Bean bean=new Bean();mbeans.registerMBean(bean,JmxReader.STORAGE);
        JMXServiceURL url=new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:"+port+"/jmxrmi");
        JMXConnectorServer jmx=JMXConnectorServerFactory.newJMXConnectorServer(url,null,mbeans);jmx.start();
        Config config=new Config();config.jmxUrl=url.toString();JmxReader reader=new JmxReader(config);Poller poller=new Poller(config,reader);
        PrometheusRegistry registry=new PrometheusRegistry();registry.register(new MembershipCollector(poller));
        ExecutorService httpWorkers=Executors.newFixedThreadPool(2), clients=Executors.newFixedThreadPool(4);
        HTTPServer http=HTTPServer.builder().hostname("127.0.0.1").port(0).registry(registry).executorService(httpWorkers).buildAndStart();
        try {
            Map<String,Object> initial=ModelTest.attrs();initial.put("LiveNodesWithPort",Arrays.asList("10.0.0.2:7000"));bean.data=initial;
            poller.runOnce();assertThat(poller.lastSuccess).isTrue();
            List<Future<String>> requests=new ArrayList<>();for(int i=0;i<200;i++)requests.add(clients.submit(() -> scrape(http.getPort())));
            for(Future<String> request:requests)assertThat(request.get(10,TimeUnit.SECONDS)).contains("cassandra_membership_endpoint_status");
            assertThat(bean.batches.get()).isEqualTo(1);assertThat(bean.singles.get()).isZero();assertThat(bean.invokes.get()).isZero();
            String body=scrape(http.getPort());
            assertThat(body).contains("# TYPE cassandra_membership_polls_total counter");
            assertThat(body).contains("status=\"up\"");
            Map<String,Object> down=ModelTest.attrs();down.put("UnreachableNodesWithPort",Arrays.asList("10.0.0.2:7000"));down.put("JoiningNodesWithPort",Arrays.asList("10.0.0.2:7000"));bean.data=down;poller.runOnce();
            body=scrape(http.getPort());assertThat(body).contains("state=\"joining\"");
            bean.data=ModelTest.attrs();poller.runOnce();assertThat(scrape(http.getPort())).doesNotContain("10.0.0.2:7000");
            Map<String,Object> broken=ModelTest.attrs();broken.remove("MovingNodesWithPort");bean.data=broken;poller.runOnce();
            assertThat(scrape(http.getPort())).doesNotContain("cassandra_membership_endpoint_status");
            assertThat(bean.batches.get()).isEqualTo(4);assertThat(bean.invokes.get()).isZero();
        } finally {
            poller.close();http.close();httpWorkers.shutdownNow();clients.shutdownNow();jmx.stop();
            UnicastRemoteObject.unexportObject(rmi,true);MBeanServerFactory.releaseMBeanServer(mbeans);
        }
    }
}
