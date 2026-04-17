package org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer.dispatcher;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinDispatcher implements BulkWriterDispatcher {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int route(SeaTunnelRow row, int writerCount) {
        return Math.floorMod(counter.getAndIncrement(), writerCount);
    }

    @Override
    public String name() {
        return "round_robin";
    }
}
