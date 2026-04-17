package org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer.dispatcher;

public final class BulkWriterDispatchers {

    public static final String ROUND_ROBIN = "round_robin";

    private BulkWriterDispatchers() {}

    public static BulkWriterDispatcher create(String strategy) {
        String s = strategy == null ? ROUND_ROBIN : strategy.trim().toLowerCase();
        switch (s) {
            case ROUND_ROBIN:
                return new RoundRobinDispatcher();
            default:
                throw new IllegalArgumentException(
                        "unknown bulk writer dispatch strategy: " + strategy);
        }
    }
}
