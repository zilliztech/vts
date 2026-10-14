/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.milvus.sink;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import org.apache.seatunnel.connectors.seatunnel.milvus.sink.state.MilvusCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.state.MilvusSinkState;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusConnectorUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer.MilvusWriterFactory;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer.MilvusWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** MilvusSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Milvus. */
@Slf4j
public class MilvusSinkWriter
        implements SinkWriter<SeaTunnelRow, MilvusCommitInfo, MilvusSinkState>, SupportMultiTableSinkWriter<Void> {
    // Guards writer creation, write, prepareCommit and close within this sink instance.
    private final Object writerOperationLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final String collection;
    private final MilvusClientV2 milvusClient;
    private final MilvusWriterFactory writerFactory;

    private final AtomicLong writeCount = new AtomicLong(0);

    private static String redactSensitiveConfig(ReadonlyConfig config) {
        return String.valueOf(config)
                .replaceAll("(\"token\"\\s*:\\s*\")[^\"]*(\")", "$1******$2")
                .replaceAll("(token\\s*=\\s*)[^,}\\s]+", "$1******");
    }

    public MilvusSinkWriter(
            Context context,
            CatalogTable catalogTable,
            ReadonlyConfig config,
            List<MilvusSinkState> milvusSinkStates) {
        this(
                context,
                catalogTable,
                config,
                milvusSinkStates,
                createClient(context, config));
    }

    private static MilvusClientV2 createClient(Context context, ReadonlyConfig config) {
        MilvusWriterFactory.validateCdcParallelism(config, context);
        return new MilvusClientV2(MilvusConnectorUtils.getConnectConfig(config));
    }

    MilvusSinkWriter(
            Context context,
            CatalogTable catalogTable,
            ReadonlyConfig config,
            List<MilvusSinkState> milvusSinkStates,
            MilvusClientV2 milvusClient) {
        MilvusWriterFactory.validateCdcParallelism(config, context);
        this.collection = catalogTable.getTablePath().getTableName();
        log.info("create Milvus sink writer success");
        log.info("MilvusSinkWriter config: {}", redactSensitiveConfig(config));
        this.milvusClient = milvusClient;
        DescribeCollectionResp describeCollectionResp =
                milvusClient.describeCollection(
                        DescribeCollectionReq.builder().collectionName(collection).build());
        writerFactory =
                new MilvusWriterFactory(
                        catalogTable, config, milvusClient, describeCollectionResp);
    }

    /**
     * write data to third party data receiver.
     *
     * @param element the data need be written.
     */
    @Override
    public void write(SeaTunnelRow element) {
        synchronized (writerOperationLock) {
            if (closed.get()) {
                throw new IllegalStateException("Milvus sink writer is closed");
            }
            writerFactory.validateRow(element);
            MilvusWriter batchWriter = writerFactory.getOrCreateWriter(element);
            try {
                batchWriter.write(element);
            } catch (Exception e) {
                log.error("write data to milvus failed, error: {}", e.getMessage());
                throw new MilvusConnectorException(MilvusConnectionErrorCode.WRITE_ERROR, e);
            }
        }

        // 增加计数并定期提交
        writeCount.incrementAndGet();
        if (writeCount.get() % 10000 == 0) {
            log.info("Successfully put {} records to Milvus, Collection: {}. Total records written: {}", "10000",
                    this.collection, writeCount.get());
        }
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) throws IOException {
        throw new IOException(
                "Milvus sink does not support runtime schema changes: " + event);
    }

    /**
     * prepare the commit, will be called before {@link #snapshotState(long checkpointId)}. If you
     * need to use 2pc, you can return the commit info in this method, and receive the commit info
     * in {@link SinkCommitter#commit(List)}. If this method failed (by throw exception), **Only**
     * Spark engine will call {@link #abortPrepare()}
     *
     * @return the commit info need to commit
     */
    @Override
    public Optional<MilvusCommitInfo> prepareCommit() throws IOException {
        synchronized (writerOperationLock) {
            if (closed.get()) {
                throw new IOException("Milvus sink writer is closed");
            }
            for (MilvusWriter writer : writerFactory.getWriters()) {
                if (!writer.needCommit()) {
                    continue;
                }
                try {
                    writer.commit(true);
                } catch (Exception e) {
                    throw new IOException("Flush Milvus writer before checkpoint failed", e);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Used to abort the {@link #prepareCommit()}, if the prepareCommit failed, there is no
     * CommitInfoT, so the rollback work cannot be done by {@link SinkCommitter}. But we can use
     * this method to rollback side effects of {@link #prepareCommit()}. Only use it in Spark engine
     * at now.
     */
    @Override
    public void abortPrepare() {}

    /**
     * call it when SinkWriter close
     *
     * @throws IOException if close failed
     */
    @Override
    public void close() throws IOException {
        if (closed.get()) {
            log.info("BatchWriter already closed");
            return;
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<Exception> failures = new ArrayList<>();
        synchronized (writerOperationLock) {
            if (!closed.get()) {
                closed.set(true);
                log.info("Stopping Milvus Client");
                for (MilvusWriter batchWriter : writerFactory.getWriters()) {
                    try {
                        batchWriter.close();
                        // Execute asynchronous wait job finish
                        futures.add(CompletableFuture.runAsync(batchWriter::waitJobFinish));
                    } catch (Exception e) {
                        failures.add(e);
                    }
                }
                // Partition writers share this client; release it only after every flush attempt.
                try {
                    milvusClient.close(10);
                } catch (Exception e) {
                    failures.add(e);
                }
            } else {
                log.info("BatchWriter already closed");
            }
        }
        // Wait for all waitJobFinish calls to complete
        for (CompletableFuture<Void> future : futures) {
            try {
                future.join();
            } catch (Exception e) {
                failures.add(e);
            }
        }
        if (!failures.isEmpty()) {
            IOException error = new IOException("Close Milvus sink failed", failures.get(0));
            failures.stream().skip(1).forEach(error::addSuppressed);
            throw error;
        }
        log.info(
                "Successfully put {} records to Milvus, collection: {}",
                writeCount.get(),
                this.collection);
        log.info("Stop Milvus Client success");
    }
}
