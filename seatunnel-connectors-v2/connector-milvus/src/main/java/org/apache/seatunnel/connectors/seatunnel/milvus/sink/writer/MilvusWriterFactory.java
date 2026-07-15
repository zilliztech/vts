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

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.external.dto.StageBucket;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkWriteMode;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.StageHelper;

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
        boolean useBulkWriter = !config.get(BULK_WRITER_CONFIG).isEmpty();
        if (writeMode == MilvusSinkWriteMode.CDC && useBulkWriter) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INVALID_PARAM,
                    "Milvus CDC write mode does not support bulk_writer_config");
        }
        if (writeMode == MilvusSinkWriteMode.CDC
                && Boolean.TRUE.equals(describeCollectionResp.getAutoID())) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INVALID_PARAM,
                    "Milvus CDC write mode does not support autoID collections");
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

    public MilvusWriter create(String partitionName) {
        return strategy.create(partitionName);
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
            return new BulkAppendWriteStrategy(catalogTable, config, stageBucket, describeCollectionResp);
        }
        return new BatchAppendWriteStrategy(catalogTable, config, milvusClient, describeCollectionResp);
    }

    private interface MilvusWriteStrategy {
        MilvusWriter create(String partitionName);

        default void validateRowKind(SeaTunnelRow element) {}
    }

    private abstract static class AppendWriteStrategy implements MilvusWriteStrategy {
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
            this.catalogTable = catalogTable;
            this.config = config;
            this.milvusClient = milvusClient;
            this.describeCollectionResp = describeCollectionResp;
        }

        @Override
        public MilvusWriter create(String partitionName) {
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
                StageBucket stageBucket,
                DescribeCollectionResp describeCollectionResp) {
            this.catalogTable = catalogTable;
            this.config = config;
            this.stageBucket = stageBucket;
            this.describeCollectionResp = describeCollectionResp;
        }

        @Override
        public MilvusWriter create(String partitionName) {
            return new MilvusBulkWriter(
                    catalogTable, config, stageBucket, describeCollectionResp, partitionName);
        }
    }

    private static class CdcWriteStrategy implements MilvusWriteStrategy {
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
        public MilvusWriter create(String partitionName) {
            return new MilvusCdcWriter(
                    catalogTable, config, milvusClient, describeCollectionResp, partitionName);
        }

    }
}
