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
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** MilvusSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Milvus. */
@Slf4j
public class MilvusSinkWriter
        implements SinkWriter<SeaTunnelRow, MilvusCommitInfo, MilvusSinkState>, SupportMultiTableSinkWriter<Void> {

    private final Map<String, MilvusWriter> batchWriters = new HashMap<>();
    private final CatalogTable catalogTable;
    private final String collection;
    private final ReadonlyConfig config;
    private final MilvusClientV2 milvusClient;
    private final Boolean useBulkWriter;
    private final StageBucket stageBucket;

    private final DescribeCollectionResp  describeCollectionResp;
    private final Boolean hasPartitionKey;

    private final AtomicLong writeCount = new AtomicLong();

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
        if(hasPartitionKey){
            // If the collection has a partition key, just ignore the partition name in the element
            partition = DEFAULT_PARTITION;
        }
        if(partition.contains("-")){
            // Replace the '-' in the partition name with '_', milvus does not support '-'
            partition = partition.replace("-", "_");
        }
        MilvusWriter batchWriter = batchWriters.get(partition);
        if(batchWriter == null){
            // Check if the partition exists, if not, create it
            HasPartitionReq hasPartitionReq = HasPartitionReq.builder()
                    .collectionName(catalogTable.getTablePath().getTableName())
                    .partitionName(partition)
                    .build();
            Boolean hasPartition = milvusClient.hasPartition(hasPartitionReq);
            if (!hasPartition && !Objects.equals(partition, DEFAULT_PARTITION)) {
                CreatePartitionReq createPartitionReq = CreatePartitionReq.builder()
                        .collectionName(catalogTable.getTablePath().getTableName())
                        .partitionName(partition)
                        .build();
                milvusClient.createPartition(createPartitionReq);
            }
            // Create a new batch writer
            if(useBulkWriter){
                batchWriter = new MilvusBulkWriter(this.catalogTable, config, stageBucket, describeCollectionResp, partition);
                batchWriters.put(partition, batchWriter);
            }else {
                batchWriter = new MilvusBufferBatchWriter(this.catalogTable, config, milvusClient, partition);
                batchWriters.put(partition, batchWriter);
            }
        }

        try {
            batchWriter.write(element);
        } catch (Exception e) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.WRITE_ERROR, e);
        }
        writeCount.incrementAndGet();
        if(writeCount.get() % 10000 == 0){
            // Print the number of records written every 10000 records
            log.info("Successfully put {} records to Milvus. Total records written: {}", "10000", this.writeCount.get());
        }

        if(writeCount.get() % config.get(MilvusSinkConfig.WRITER_CACHE) == 0){
            // commit every 1000000 records
            // This is to prevent the number of records in the batch writer from becoming too large
            // flush all batch writers every 1000000 records
            try {
                for (MilvusWriter writer : batchWriters.values()){
                    writer.commit();
                }
            } catch (Exception e) {
                throw new MilvusConnectorException(MilvusConnectionErrorCode.COMMIT_ERROR, e);
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

        log.info("Stopping Milvus Client");
        for (MilvusWriter batchWriter : batchWriters.values()){
            try {
                writeCount.addAndGet(batchWriter.getWriteCache());
                batchWriter.close();
            } catch (Exception e) {
                throw new MilvusConnectorException(MilvusConnectionErrorCode.CLOSE_CLIENT_ERROR, e);
            }
        }
        log.info("Successfully put {} records to Milvus", this.writeCount.get());
        log.info("Stop Milvus Client success");

    }
}
