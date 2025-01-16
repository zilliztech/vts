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

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import static org.apache.seatunnel.connectors.seatunnel.milvus.common.MilvusConstant.DEFAULT_PARTITION;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusCommonConfig.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusCommonConfig.URL;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.common.StageBucket;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.BULK_WRITER_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.DATABASE;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.state.MilvusCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.state.MilvusSinkState;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.StageHelper;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer.MilvusBufferBatchWriter;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer.MilvusBulkWriter;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer.MilvusWriter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** MilvusSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Milvus. */
@Slf4j
public class MilvusSinkWriter
        implements SinkWriter<SeaTunnelRow, MilvusCommitInfo, MilvusSinkState>, SupportMultiTableSinkWriter<Void> {
    //set this to static, then all thread will reuse this
    private final static Map<String, MilvusWriter> batchWriters = new ConcurrentHashMap<>();
    private final static AtomicBoolean closed = new AtomicBoolean(false);
    private final CatalogTable catalogTable;
    private final String collection;
    private final ReadonlyConfig config;
    private final MilvusClientV2 milvusClient;
    private final Boolean useBulkWriter;
    private final StageBucket stageBucket;

    private final DescribeCollectionResp  describeCollectionResp;
    private final Boolean hasPartitionKey;

    private final static AtomicLong writeCount = new AtomicLong();

    public MilvusSinkWriter(
            Context context,
            CatalogTable catalogTable,
            ReadonlyConfig config,
            List<MilvusSinkState> milvusSinkStates) {
        this.config = config;
        this.catalogTable = catalogTable;
        this.collection = catalogTable.getTablePath().getTableName();
        log.info("create Milvus sink writer success");
        log.info("MilvusSinkWriter config: " + config);
        ConnectConfig connectConfig = ConnectConfig.builder()
                        .uri(config.get(URL))
                        .token(config.get(TOKEN))
                        .dbName(config.get(DATABASE))
                        .build();
        this.milvusClient = new MilvusClientV2(connectConfig);
        describeCollectionResp = milvusClient.describeCollection(DescribeCollectionReq.builder().collectionName(collection).build());
        hasPartitionKey = describeCollectionResp.getCollectionSchema().getFieldSchemaList().stream().anyMatch(CreateCollectionReq.FieldSchema::getIsPartitionKey);
        useBulkWriter = !config.get(BULK_WRITER_CONFIG).isEmpty();
        // apply for a stage session bucket to store parquet files
        stageBucket = StageHelper.getStageBucket(config.get(BULK_WRITER_CONFIG));

    }

    /**
     * write data to third party data receiver.
     *
     * @param element the data need be written.
     */
    @Override
    public void write(SeaTunnelRow element) {
        String partition = StringUtils.isEmpty(element.getPartitionName()) ? DEFAULT_PARTITION : element.getPartitionName();
        if (hasPartitionKey) {
            partition = DEFAULT_PARTITION;
        }
        if (partition.contains("-")) {
            partition = partition.replace("-", "_");
        }
        String partitionId = catalogTable.getTablePath() + "." + partition;

        // 获取或创建 MilvusWriter
        String finalPartition = partition;
        MilvusWriter batchWriter = batchWriters.computeIfAbsent(partitionId, id -> {
            synchronized (batchWriters) {
                if (!milvusClient.hasPartition(HasPartitionReq.builder()
                        .collectionName(catalogTable.getTablePath().getTableName())
                        .partitionName(finalPartition).build())) {
                    milvusClient.createPartition(CreatePartitionReq.builder()
                            .collectionName(catalogTable.getTablePath().getTableName())
                            .partitionName(finalPartition).build());
                }
                return useBulkWriter
                        ? new MilvusBulkWriter(this.catalogTable, config, stageBucket, describeCollectionResp, finalPartition)
                        : new MilvusBufferBatchWriter(this.catalogTable, config, milvusClient, finalPartition);
            }
        });

        // 写入数据
        synchronized (batchWriter) {
            try {
                batchWriter.write(element);
            } catch (Exception e) {
                throw new MilvusConnectorException(MilvusConnectionErrorCode.WRITE_ERROR, e);
            }
        }

        // 增加计数并定期提交
        writeCount.incrementAndGet();
        if (writeCount.get() % 10000 == 0) {
            log.info("Successfully put {} records to Milvus. Total records written: {}", "10000", writeCount.get());
        }
        if (writeCount.get() % config.get(MilvusSinkConfig.WRITER_CACHE) == 0) {
            synchronized (batchWriters) {
                try {
                    for (MilvusWriter writer : batchWriters.values()) {
                        writer.commit(true);
                    }
                } catch (Exception e) {
                    throw new MilvusConnectorException(MilvusConnectionErrorCode.COMMIT_ERROR, e);
                }
            }
        }
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
        synchronized (batchWriters) {
            if (!closed.get()) {
                log.info("Stopping Milvus Client");
                for (MilvusWriter batchWriter : batchWriters.values()) {
                    try {
                        writeCount.addAndGet(batchWriter.getWriteCache());
                        batchWriter.commit(false);
                        batchWriter.close();
                    } catch (Exception e) {
                        throw new MilvusConnectorException(MilvusConnectionErrorCode.CLOSE_CLIENT_ERROR, e);
                    }
                }
                log.info("Successfully put {} records to Milvus", writeCount.get());
                log.info("Stop Milvus Client success");
                closed.set(true); // Mark as closed
            } else {
                log.info("BatchWriter already closed");
            }
        }
    }
}
