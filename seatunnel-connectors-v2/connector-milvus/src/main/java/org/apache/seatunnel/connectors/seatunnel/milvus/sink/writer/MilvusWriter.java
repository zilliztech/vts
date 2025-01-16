package org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

public interface MilvusWriter {
    void write(SeaTunnelRow element) throws Exception;
    void commit(Boolean async) throws Exception;
    boolean needCommit();
    void close() throws Exception;
    long getWriteCache();
}
