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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog.MilvusFieldSchema;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusConnectorUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusSinkConverter;

import static org.apache.seatunnel.connectors.seatunnel.milvus.common.MilvusConstants.DEFAULT_PARTITION;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.CDC_BATCH_FLUSH_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.FIELD_SCHEMA;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

@Slf4j
public class MilvusCdcWriter implements MilvusWriter {
    private final CatalogTable catalogTable;
    private final String collectionName;
    private final String partitionName;
    private final Boolean hasPartitionKey;
    private final MilvusClientV2 milvusClient;
    private final DescribeCollectionResp descriptionCollectionResp;
    private final MilvusSinkConverter milvusSinkConverter;
    private final Map<String, String> milvusFieldMapper;
    private final int primaryKeyIndex;
    private final int batchSize;
    private final long cdcBatchFlushIntervalMs;
    private final LongSupplier currentTimeMillisSupplier;
    private final Map<Object, SeaTunnelRow> pendingRows;
    private RowKind pendingRowKind;
    private RowKind lastTargetRowKind;
    private long lastFlushTime;
    private final AtomicLong writeCache = new AtomicLong();
    private final AtomicLong writeCount = new AtomicLong();

    public MilvusCdcWriter(
            CatalogTable catalogTable,
            ReadonlyConfig config,
            MilvusClientV2 milvusClient,
            DescribeCollectionResp describeCollectionResp,
            String partitionName) {
        this(
                catalogTable,
                config,
                milvusClient,
                describeCollectionResp,
                partitionName,
                System::currentTimeMillis);
    }

    MilvusCdcWriter(
            CatalogTable catalogTable,
            ReadonlyConfig config,
            MilvusClientV2 milvusClient,
            DescribeCollectionResp describeCollectionResp,
            String partitionName,
            LongSupplier currentTimeMillisSupplier) {
        this.catalogTable = catalogTable;
        this.collectionName = catalogTable.getTablePath().getTableName();
        this.partitionName = partitionName;
        this.milvusClient = milvusClient;
        this.descriptionCollectionResp = describeCollectionResp;
        this.hasPartitionKey = MilvusConnectorUtils.hasPartitionKey(describeCollectionResp);
        this.milvusSinkConverter = MilvusSinkConverter.fromConfig(config);
        this.milvusFieldMapper = buildFieldMapper(config);
        this.primaryKeyIndex =
                primaryKeyIndex(catalogTable, describeCollectionResp, milvusFieldMapper);
        this.batchSize = config.get(BATCH_SIZE);
        this.cdcBatchFlushIntervalMs = config.get(CDC_BATCH_FLUSH_INTERVAL_MS);
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.lastFlushTime = currentTimeMillisSupplier.getAsLong();
        this.pendingRows = new LinkedHashMap<>();
    }

    @Override
    public void write(SeaTunnelRow element) {
        // Milvus is a keyed/upsert sink. UPDATE_BEFORE is only the retract image of a
        // keyed update; primary-key changes must be represented as DELETE + INSERT.
        if (element.getRowKind() == RowKind.UPDATE_BEFORE) {
            return;
        }
        RowKind targetRowKind = targetRowKind(element);
        Object primaryKey = requirePrimaryKeyValue(element, targetRowKind);
        boolean operationChanged = !pendingRows.isEmpty() && pendingRowKind != targetRowKind;
        // After a primary-key change, do not leave the new row pending until the next
        // message or checkpoint once the old key has already been deleted.
        boolean flushCurrentAfterSwitch =
                lastTargetRowKind == RowKind.DELETE && targetRowKind == RowKind.INSERT;
        if (operationChanged) {
            flushPending();
        }
        pendingRowKind = targetRowKind;
        pendingRows.remove(primaryKey);
        pendingRows.put(primaryKey, element);
        long pendingEventCount = writeCache.incrementAndGet();
        if (flushCurrentAfterSwitch || pendingEventCount >= batchSize) {
            flushPending();
        } else {
            flushPendingIfIntervalElapsed();
        }
        lastTargetRowKind = targetRowKind;
    }

    private void writeRows(RowKind rowKind, List<SeaTunnelRow> rows) {
        long applyStartTimestampMs = System.currentTimeMillis();
        if (rowKind == RowKind.DELETE) {
            delete(partitionName, primaryKeys(rows));
        } else if (rowKind == RowKind.INSERT) {
            upsert(partitionName, rows);
        } else {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                    "Milvus CDC sink only supports INSERT and DELETE row kinds, but received: "
                            + rowKind);
        }
        long applyTimestampMs = System.currentTimeMillis();
        long applyCostMs = applyTimestampMs - applyStartTimestampMs;
        logAppliedBatch(rowKind, rows, applyTimestampMs, applyCostMs);
        writeCount.addAndGet(rows.size());
    }

    @Override
    public void commit(Boolean async) {
        flushPending();
    }

    @Override
    public boolean needCommit() {
        return writeCache.get() > 0;
    }

    @Override
    public void close() throws Exception {
        commit(true);
        this.milvusClient.close(10);
    }

    @Override
    public long getWriteCache() {
        return writeCache.get();
    }

    @Override
    public void waitJobFinish() {}

    private void upsert(String partitionName, List<SeaTunnelRow> elements) {
        List<JsonObject> data = new ArrayList<>(elements.size());
        for (SeaTunnelRow element : elements) {
            data.add(
                    milvusSinkConverter.buildMilvusData(
                            catalogTable, descriptionCollectionResp, milvusFieldMapper, element));
        }
        UpsertReq upsertReq =
                UpsertReq.builder().collectionName(this.collectionName).data(data).build();

        if (StringUtils.isNotEmpty(partitionName)
                && !partitionName.equals(DEFAULT_PARTITION)
                && !this.hasPartitionKey) {
            upsertReq.setPartitionName(partitionName);
        }

        try {
            milvusClient.upsert(upsertReq);
        } catch (Exception e) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.WRITE_DATA_FAIL, "upsert CDC data failed", e);
        }
    }

    private void delete(String partitionName, List<Object> primaryKeys) {
        DeleteReq.DeleteReqBuilder builder =
                DeleteReq.builder().collectionName(this.collectionName).ids(primaryKeys);

        if (StringUtils.isNotEmpty(partitionName)
                && !partitionName.equals(DEFAULT_PARTITION)
                && !this.hasPartitionKey) {
            builder.partitionName(partitionName);
        }

        try {
            milvusClient.delete(builder.build());
        } catch (Exception e) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.WRITE_DATA_FAIL, "delete CDC data failed", e);
        }
    }

    private Object primaryKeyValue(SeaTunnelRow element) {
        return element.getField(primaryKeyIndex);
    }

    private List<Object> primaryKeyValues(List<SeaTunnelRow> rows) {
        List<Object> primaryKeys = new ArrayList<>(rows.size());
        for (SeaTunnelRow row : rows) {
            primaryKeys.add(primaryKeyValue(row));
        }
        return primaryKeys;
    }

    private List<Object> primaryKeys(List<SeaTunnelRow> rows) {
        List<Object> primaryKeys = primaryKeyValues(rows);
        for (Object primaryKey : primaryKeys) {
            if (primaryKey == null) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                        "delete row primary key must not be null");
            }
        }
        return primaryKeys;
    }

    private Object requirePrimaryKeyValue(SeaTunnelRow element, RowKind targetRowKind) {
        Object primaryKey = primaryKeyValue(element);
        if (primaryKey == null) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                    targetRowKind == RowKind.DELETE
                            ? "delete row primary key must not be null"
                            : "upsert row primary key must not be null");
        }
        return primaryKey;
    }

    private RowKind targetRowKind(SeaTunnelRow element) {
        RowKind rowKind = element.getRowKind();
        if (rowKind == null) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                    "Milvus CDC row kind must not be null");
        }
        if (rowKind == RowKind.INSERT || rowKind == RowKind.UPDATE_AFTER) {
            return RowKind.INSERT;
        }
        if (rowKind == RowKind.DELETE) {
            return RowKind.DELETE;
        }
        throw new MilvusConnectorException(
                MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                "Unsupported Milvus CDC row kind: " + rowKind);
    }

    private void flushPending() {
        if (pendingRows.isEmpty()) {
            return;
        }
        writeRows(pendingRowKind, new ArrayList<>(pendingRows.values()));
        lastFlushTime = currentTimeMillisSupplier.getAsLong();
        pendingRows.clear();
        pendingRowKind = null;
        writeCache.set(0L);
    }

    private void flushPendingIfIntervalElapsed() {
        long currentTime = currentTimeMillisSupplier.getAsLong();
        // TODO: Replace this write-triggered interval check with SeaTunnel engine-level
        // timer flush after STIP-23 (#10717 / #10800) is available in this branch.
        if (currentTime < lastFlushTime
                || currentTime - lastFlushTime >= cdcBatchFlushIntervalMs) {
            flushPending();
        }
    }

    private void logAppliedBatch(
            RowKind rowKind, List<SeaTunnelRow> rows, long applyTimestampMs, long applyCostMs) {
        SeaTunnelRow first = rows.get(0);
        SeaTunnelRow last = rows.get(rows.size() - 1);
        log.info(
                "Milvus CDC sink applied batch: collection={}, partition={}, type={}, rowCount={}, firstPrimaryKey={}, lastPrimaryKey={}, applyTimestampMs={}, applyCostMs={}",
                collectionName,
                effectivePartitionName(first),
                rowKind,
                rows.size(),
                primaryKeyValue(first),
                primaryKeyValue(last),
                applyTimestampMs,
                applyCostMs);
    }

    private String effectivePartitionName(SeaTunnelRow element) {
        return StringUtils.defaultIfEmpty(partitionName, element.getPartitionName());
    }

    private static int primaryKeyIndex(
            CatalogTable catalogTable,
            DescribeCollectionResp describeCollectionResp,
            Map<String, String> milvusFieldMapper) {
        String primaryKeyField = describeCollectionResp.getPrimaryFieldName();
        if (StringUtils.isEmpty(primaryKeyField)) {
            PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
            if (primaryKey != null && primaryKey.getColumnNames().size() == 1) {
                primaryKeyField = primaryKey.getColumnNames().get(0);
            }
        }
        if (StringUtils.isEmpty(primaryKeyField)) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                    "Milvus CDC sink requires a single primary key field");
        }

        String[] fieldNames = catalogTable.getSeaTunnelRowType().getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            String sourceFieldName = fieldNames[i];
            String targetFieldName =
                    milvusFieldMapper.getOrDefault(sourceFieldName, sourceFieldName);
            if (targetFieldName.equals(primaryKeyField)) {
                return i;
            }
        }
        throw new MilvusConnectorException(
                MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                "primary key field not found in source row schema: " + primaryKeyField);
    }

    private static Map<String, String> buildFieldMapper(ReadonlyConfig config) {
        Gson gson = new Gson();
        Type type = new TypeToken<List<MilvusFieldSchema>>() {}.getType();
        List<MilvusFieldSchema> fieldSchemaList =
                gson.fromJson(gson.toJson(config.get(FIELD_SCHEMA)), type);

        Map<String, String> milvusFieldMapper = new HashMap<>();
        if (fieldSchemaList != null) {
            for (MilvusFieldSchema field : fieldSchemaList) {
                String sourceFieldName = field.getSourceFieldName();
                if (sourceFieldName != null) {
                    milvusFieldMapper.put(sourceFieldName, field.getFieldName());
                }
            }
        }
        return milvusFieldMapper;
    }
}
