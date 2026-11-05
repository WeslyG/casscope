package local.cassandra.membership;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

public final class Config {
    public String cluster = "cassandra", observer = "local";
    public String jmxUrl = "service:jmx:rmi:///jndi/rmi://127.0.0.1:7199/jmxrmi";
    public String listenAddress = "127.0.0.1", usernameEnv, passwordEnv;
    public int port = 9505, pollSeconds = 30, backoffSeconds = 300;
    public int socketTimeoutMs = 3000, slowPollMs = 1000, staleSeconds = 90, maxEndpoints = 1000;

    public static Config load(Path path) throws IOException {
        if (Files.size(path) > 65536) throw new IllegalArgumentException("Configuration too large");
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false); options.setMaxAliasesForCollections(0); options.setCodePointLimit(65536);
        Object loaded;
        try (InputStream input = Files.newInputStream(path)) {
            loaded = new Yaml(new SafeConstructor(options)).load(input);
        }
        if (!(loaded instanceof Map)) throw new IllegalArgumentException("Expected YAML mapping");
        Config c = new Config();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) loaded).entrySet()) {
            String k = String.valueOf(entry.getKey()), v = String.valueOf(entry.getValue());
            switch (k) {
                case "cluster": c.cluster=v; break; case "observer": c.observer=v; break;
                case "jmx_url": c.jmxUrl=v; break; case "listen_address": c.listenAddress=v; break;
                case "username_env": c.usernameEnv=v; break; case "password_env": c.passwordEnv=v; break;
                case "port": c.port=Integer.parseInt(v); break;
                case "poll_interval_seconds": c.pollSeconds=Integer.parseInt(v); break;
                case "max_backoff_seconds": c.backoffSeconds=Integer.parseInt(v); break;
                case "socket_timeout_ms": c.socketTimeoutMs=Integer.parseInt(v); break;
                case "slow_poll_ms": c.slowPollMs=Integer.parseInt(v); break;
                case "stale_after_seconds": c.staleSeconds=Integer.parseInt(v); break;
                case "max_endpoints": c.maxEndpoints=Integer.parseInt(v); break;
                default: throw new IllegalArgumentException("Unknown config key: " + k);
            }
        }
        c.validate(); return c;
    }
    public void validate() {
        if (cluster.isEmpty() || observer.isEmpty() || cluster.length()>128 || observer.length()>128)
            throw new IllegalArgumentException("cluster/observer must have 1..128 characters");
        if (!jmxUrl.startsWith("service:jmx:rmi:")) throw new IllegalArgumentException("Only JMX/RMI is supported");
        if (port<1 || port>65535 || pollSeconds<15 || pollSeconds>3600 || backoffSeconds<pollSeconds || backoffSeconds>3600
                || socketTimeoutMs<100 || socketTimeoutMs>30000
                || slowPollMs<1 || slowPollMs>socketTimeoutMs || staleSeconds<pollSeconds || staleSeconds>7200
                || maxEndpoints<1 || maxEndpoints>10000)
            throw new IllegalArgumentException("Invalid limits: minimum poll 15s; check exporter.yaml");
        if ((usernameEnv==null)!=(passwordEnv==null)) throw new IllegalArgumentException("Both credential environment names required");
    }
}
