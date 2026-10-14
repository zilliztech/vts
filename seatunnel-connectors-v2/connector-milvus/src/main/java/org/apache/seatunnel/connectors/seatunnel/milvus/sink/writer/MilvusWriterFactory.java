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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.external.dto.StageBucket;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkWriteMode;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusConnectorUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.StageHelper;

import org.apache.commons.lang3.StringUtils;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.milvus.common.MilvusConstants.DEFAULT_PARTITION;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.BULK_WRITER_CONFIG;

public class MilvusWriterFactory {

    private final int expectedRowArity;
    private final MilvusWriteStrategy strategy;

    public MilvusWriterFactory(
            CatalogTable catalogTable,
            ReadonlyConfig config,
            MilvusClientV2 milvusClient,
            DescribeCollectionResp describeCollectionResp) {
        this.expectedRowArity = catalogTable.getSeaTunnelRowType().getTotalFields();
        MilvusSinkWriteMode writeMode = config.get(MilvusSinkConfig.WRITE_MODE);
        validateCdcReplica(config);
        boolean useBulkWriter = !config.get(BULK_WRITER_CONFIG).isEmpty();
        if (writeMode == MilvusSinkWriteMode.CDC && useBulkWriter) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INVALID_PARAM,
                    "Milvus CDC write mode does not support bulk_writer_config");
        }
        if (writeMode == MilvusSinkWriteMode.CDC
                && Boolean.TRUE.equals(describeCollectionResp.getAutoID())
                && !MilvusConnectorUtils.allowInsertAutoId(describeCollectionResp)) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INVALID_PARAM,
                    "Milvus CDC write mode requires allow_insert_auto_id=true on the target autoID collection");
        }
        this.strategy =
                createStrategy(
                        writeMode,
                        useBulkWriter,
                        catalogTable,
                        config,
                        milvusClient,
                        describeCollectionResp);
    }

    /** Called under the owning sink writer's writerOperationLock. */
    public MilvusWriter getOrCreateWriter(SeaTunnelRow row) {
        return strategy.getOrCreateWriter(row);
    }

    /** Returns each owned writer once, without partition aliases. */
    public List<MilvusWriter> getWriters() {
        return strategy.getWriters();
    }

    public static void validateCdcParallelism(ReadonlyConfig config, SinkWriter.Context context) {
        validateCdcReplica(config);
        if (config.get(MilvusSinkConfig.WRITE_MODE) == MilvusSinkWriteMode.CDC
                && context.getNumberOfParallelSubtasks() != 1) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INVALID_PARAM,
                    "Milvus CDC write mode requires sink parallelism = 1 to preserve event order; actual: "
                            + context.getNumberOfParallelSubtasks());
        }
    }

    public static void validateCdcReplica(ReadonlyConfig config) {
        if (config.get(MilvusSinkConfig.WRITE_MODE) == MilvusSinkWriteMode.CDC
                && config.get(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA) != 1) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INVALID_PARAM,
                    "Milvus CDC write mode requires multi_table_sink_replica = 1 to preserve event order");
        }
    }

    public void validateRow(SeaTunnelRow element) {
        validateRowArity(element);
        strategy.validateRowKind(element);
    }

    private void validateRowArity(SeaTunnelRow element) {
        if (element.getArity() != expectedRowArity) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.WRITE_ERROR,
                    String.format(
                            "Milvus sink row field count mismatch: expected %d fields, but received %d",
                            expectedRowArity, element.getArity()));
        }
    }

    private MilvusWriteStrategy createStrategy(
            MilvusSinkWriteMode writeMode,
            boolean useBulkWriter,
            CatalogTable catalogTable,
            ReadonlyConfig config,
            MilvusClientV2 milvusClient,
            DescribeCollectionResp describeCollectionResp) {
        if (writeMode == MilvusSinkWriteMode.CDC) {
            return new CdcWriteStrategy(catalogTable, config, milvusClient, describeCollectionResp);
        }
        if (useBulkWriter) {
            StageBucket stageBucket = StageHelper.getStageBucket(config.get(BULK_WRITER_CONFIG));
            return new BulkAppendWriteStrategy(
                    catalogTable, config, milvusClient, stageBucket, describeCollectionResp);
        }
        return new BatchAppendWriteStrategy(catalogTable, config, milvusClient, describeCollectionResp);
    }

    private interface MilvusWriteStrategy {
        MilvusWriter getOrCreateWriter(SeaTunnelRow row);

        List<MilvusWriter> getWriters();

        default void validateRowKind(SeaTunnelRow element) {}
    }

    private abstract static class AppendWriteStrategy implements MilvusWriteStrategy {
        private final Map<String, MilvusWriter> writers = new LinkedHashMap<>();
        private final String collection;
        private final MilvusClientV2 milvusClient;
        private final boolean hasPartitionKey;

        private AppendWriteStrategy(
                CatalogTable catalogTable,
                MilvusClientV2 milvusClient,
                DescribeCollectionResp description) {
            this.collection = catalogTable.getTablePath().getTableName();
            this.milvusClient = milvusClient;
            this.hasPartitionKey = MilvusConnectorUtils.hasPartitionKey(description);
        }

        @Override
        public MilvusWriter getOrCreateWriter(SeaTunnelRow row) {
            String partition =
                    hasPartitionKey || StringUtils.isEmpty(row.getPartitionName())
                            ? DEFAULT_PARTITION
                            : row.getPartitionName().replace("-", "_");
            MilvusWriter writer = writers.get(partition);
            if (writer == null) {
                ensurePartition(partition);
                // Preserve APPEND's flush-before-creating-a-partition-writer behavior.
                try {
                    for (MilvusWriter existing : writers.values()) {
                        existing.commit(true);
                    }
                } catch (Exception e) {
                    throw new MilvusConnectorException(MilvusConnectionErrorCode.COMMIT_ERROR, e);
                }
                writer = createPartitionWriter(partition);
                writers.put(partition, writer);
            }
            return writer;
        }

        @Override
        public List<MilvusWriter> getWriters() {
            return new ArrayList<>(writers.values());
        }

        protected abstract MilvusWriter createPartitionWriter(String partitionName);

        private void ensurePartition(String partition) {
            if (DEFAULT_PARTITION.equals(partition)) {
                return;
            }
            if (!milvusClient.hasPartition(
                    HasPartitionReq.builder()
                            .collectionName(collection)
                            .partitionName(partition)
                            .build())) {
                milvusClient.createPartition(
                        CreatePartitionReq.builder()
                                .collectionName(collection)
                                .partitionName(partition)
                                .build());
                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new MilvusConnectorException(
                            MilvusConnectionErrorCode.CREATE_PARTITION_ERROR, e);
                }
            }
        }

        @Override
        public void validateRowKind(SeaTunnelRow element) {
            RowKind rowKind = element.getRowKind();
            if (rowKind != null && rowKind != RowKind.INSERT) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.WRITE_ERROR,
                        "Milvus APPEND write mode only supports INSERT rows. "
                                + "Set write_mode = cdc for Milvus CDC INSERT/DELETE rows.");
            }
        }
    }

    private static class BatchAppendWriteStrategy extends AppendWriteStrategy {
        private final CatalogTable catalogTable;
        private final ReadonlyConfig config;
        private final MilvusClientV2 milvusClient;
        private final DescribeCollectionResp describeCollectionResp;

        private BatchAppendWriteStrategy(
                CatalogTable catalogTable,
                ReadonlyConfig config,
                MilvusClientV2 milvusClient,
                DescribeCollectionResp describeCollectionResp) {
            super(catalogTable, milvusClient, describeCollectionResp);
            this.catalogTable = catalogTable;
            this.config = config;
            this.milvusClient = milvusClient;
            this.describeCollectionResp = describeCollectionResp;
        }

        @Override
        protected MilvusWriter createPartitionWriter(String partitionName) {
            return new MilvusBufferBatchWriter(
                    catalogTable, config, milvusClient, describeCollectionResp, partitionName);
        }
    }

    private static class BulkAppendWriteStrategy extends AppendWriteStrategy {
        private final CatalogTable catalogTable;
        private final ReadonlyConfig config;
        private final StageBucket stageBucket;
        private final DescribeCollectionResp describeCollectionResp;

        private BulkAppendWriteStrategy(
                CatalogTable catalogTable,
                ReadonlyConfig config,
                MilvusClientV2 milvusClient,
                StageBucket stageBucket,
                DescribeCollectionResp describeCollectionResp) {
            super(catalogTable, milvusClient, describeCollectionResp);
            this.catalogTable = catalogTable;
            this.config = config;
            this.stageBucket = stageBucket;
            this.describeCollectionResp = describeCollectionResp;
        }

        @Override
        protected MilvusWriter createPartitionWriter(String partitionName) {
            return new MilvusBulkWriter(
                    catalogTable, config, stageBucket, describeCollectionResp, partitionName);
        }
    }

    private static class CdcWriteStrategy implements MilvusWriteStrategy {
        private MilvusWriter writer;
        private final CatalogTable catalogTable;
        private final ReadonlyConfig config;
        private final MilvusClientV2 milvusClient;
        private final DescribeCollectionResp describeCollectionResp;

        private CdcWriteStrategy(
                CatalogTable catalogTable,
                ReadonlyConfig config,
                MilvusClientV2 milvusClient,
                DescribeCollectionResp describeCollectionResp) {
            this.catalogTable = catalogTable;
            this.config = config;
            this.milvusClient = milvusClient;
            this.describeCollectionResp = describeCollectionResp;
        }

        @Override
        public MilvusWriter getOrCreateWriter(SeaTunnelRow row) {
            if (writer == null) {
                writer =
                        new MilvusCdcWriter(
                                catalogTable, config, milvusClient, describeCollectionResp);
            }
            return writer;
        }

        @Override
        public List<MilvusWriter> getWriters() {
            return writer == null ? Collections.emptyList() : Collections.singletonList(writer);
        }
    }
}
