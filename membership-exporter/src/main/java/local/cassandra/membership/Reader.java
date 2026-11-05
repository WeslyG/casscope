package local.cassandra.membership;

interface Reader extends AutoCloseable {
    Snapshot read() throws Exception;
    long reads();
    void close();
}
