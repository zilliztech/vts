package org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer.dispatcher;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

/**
 * Decides which BulkWriter slot (0..writerCount-1) a row goes to when the sink
 * uses an internal RemoteBulkWriter pool.
 *
 * <p>Implementations must be safe to call concurrently from the sink's write thread.
 * Bulk import assigns a single task-level TSO to every row in a job, so duplicate
 * pk rows within one import share the same timestamp and dedup via
 * {@code ts > entry.ts} is a no-op — routing choice does not change visibility
 * semantics for duplicate pks.
 */
public interface BulkWriterDispatcher {

    int route(SeaTunnelRow row, int writerCount);

    String name();
}
