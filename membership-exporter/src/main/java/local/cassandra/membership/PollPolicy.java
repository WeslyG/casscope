package local.cassandra.membership;

import java.util.concurrent.ThreadLocalRandom;

final class PollPolicy {
    private final long baseMs, maxMs;
    private long delay;
    PollPolicy(long baseMs,long maxMs) {this.baseMs=baseMs;this.maxMs=maxMs;this.delay=baseMs;}
    long after(boolean healthy) {
        // Fast recovery is gradual; slow successful reads also trigger backoff.
        delay=healthy?Math.max(baseMs,delay/2):Math.min(maxMs,delay>maxMs/2?maxMs:delay*2);
        return delay;
    }
    long jittered(long ms) {
        // Positive jitter: never shorten the configured minimum spacing.
        long room=Math.min(maxMs-ms,Math.max(1,ms/10));
        return ms+(room>0?ThreadLocalRandom.current().nextLong(room+1):0);
    }
}
