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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog.MilvusFieldSchema;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusConnectorUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusSinkConverter;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.milvus.shaded.io.grpc.StatusRuntimeException;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.client.RetryConfig;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.utils.RpcUtils;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.apache.seatunnel.api.table.type.CommonOptions.EVENT_TIME;
import static org.apache.seatunnel.connectors.seatunnel.milvus.common.MilvusConstants.DEFAULT_PARTITION;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.CDC_BATCH_FLUSH_INTERVAL_MS;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.CDC_OVERLAP_UPSERT_CUTOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.ENABLE_UPSERT;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.FIELD_SCHEMA;

@Slf4j
public class MilvusCdcWriter implements MilvusWriter {
    private enum WriteOperation {
        INSERT,
        UPSERT,
        DELETE
    }

    // Milvus server error code for ErrPartitionNotFound (pkg/util/merr).
    private static final int PARTITION_NOT_FOUND = 200;
    private final CatalogTable catalogTable;
    private final String collectionName;
    private final Boolean hasPartitionKey;
    private final MilvusClientV2 milvusClient;
    private final RetryConfig clientRetryConfig = RetryConfig.builder().build();
    private final DescribeCollectionResp descriptionCollectionResp;
    private final MilvusSinkConverter milvusSinkConverter;
    private final Map<String, String> milvusFieldMapper;
    private final int primaryKeyIndex;
    private final int batchSize;
    private final long cdcBatchFlushIntervalMs;
    private final Long cdcOverlapUpsertCutoffMs;
    private final boolean enableUpsert;
    private final LongSupplier currentTimeMillisSupplier;
    private final Map<Object, SeaTunnelRow> pendingRows;
    private final Set<String> knownPartitions = new HashSet<>();
    private WriteOperation pendingOperation;
    private String pendingPartitionName;
    private WriteOperation lastOperation;
    private long lastFlushTime;
    private final AtomicLong writeCache = new AtomicLong();
    private final AtomicLong writeCount = new AtomicLong();

    public MilvusCdcWriter(
            CatalogTable catalogTable,
            ReadonlyConfig config,
            MilvusClientV2 milvusClient,
            DescribeCollectionResp describeCollectionResp) {
        this(catalogTable, config, milvusClient, describeCollectionResp, System::currentTimeMillis);
    }

    MilvusCdcWriter(
            CatalogTable catalogTable,
            ReadonlyConfig config,
            MilvusClientV2 milvusClient,
            DescribeCollectionResp describeCollectionResp,
            LongSupplier currentTimeMillisSupplier) {
        this.catalogTable = catalogTable;
        this.collectionName = catalogTable.getTablePath().getTableName();
        this.milvusClient = milvusClient;
        this.descriptionCollectionResp = describeCollectionResp;
        this.hasPartitionKey = MilvusConnectorUtils.hasPartitionKey(describeCollectionResp);
        this.milvusSinkConverter = MilvusSinkConverter.fromConfig(config);
        this.milvusFieldMapper = buildFieldMapper(config);
        this.primaryKeyIndex =
                primaryKeyIndex(catalogTable, describeCollectionResp, milvusFieldMapper);
        this.batchSize = config.get(BATCH_SIZE);
        this.cdcBatchFlushIntervalMs = config.get(CDC_BATCH_FLUSH_INTERVAL_MS);
        this.enableUpsert = config.get(ENABLE_UPSERT);
        this.cdcOverlapUpsertCutoffMs =
                config.getOptional(CDC_OVERLAP_UPSERT_CUTOFF_MS).orElse(null);
        if (cdcOverlapUpsertCutoffMs != null && cdcOverlapUpsertCutoffMs < 0) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INVALID_PARAM,
                    "cdc_overlap_upsert_cutoff_ms must be a non-negative Unix timestamp in milliseconds");
        }
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.lastFlushTime = currentTimeMillisSupplier.getAsLong();
        this.pendingRows = new LinkedHashMap<>();
    }

    @Override
    public void write(SeaTunnelRow element) {
        WriteOperation operation = resolveOperation(element);
        Object primaryKey = requirePrimaryKeyValue(element, operation);
        String partitionName = targetPartition(element, operation);
        if (isBatchChanged(operation, partitionName)) {
            flushPending();
        }

        // Keep repeated inserts/upserts in separate requests; repeated deletes can be combined.
        if (operation != WriteOperation.DELETE && pendingRows.containsKey(primaryKey)) {
            flushPending();
        }

        pendingOperation = operation;
        pendingPartitionName = partitionName;
        pendingRows.remove(primaryKey);
        pendingRows.put(primaryKey, element);
        long pendingEventCount = writeCache.incrementAndGet();

        // After a primary-key change, do not leave the new row pending until the next
        // message or checkpoint once the old key has already been deleted.
        boolean flushCurrentAfterSwitch =
                lastOperation == WriteOperation.DELETE && operation != WriteOperation.DELETE;
        if (flushCurrentAfterSwitch || pendingEventCount >= batchSize) {
            flushPending();
        } else {
            flushPendingIfIntervalElapsed();
        }
        lastOperation = operation;
    }

    private boolean isBatchChanged(WriteOperation operation, String partitionName) {
        return !pendingRows.isEmpty()
                && (pendingOperation != operation
                        || !Objects.equals(pendingPartitionName, partitionName));
    }

    private WriteOperation resolveOperation(SeaTunnelRow row) {
        RowKind rowKind = row.getRowKind();
        if (rowKind == null) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                    "Milvus CDC row kind must not be null");
        }
        switch (rowKind) {
            case DELETE:
            case UPDATE_BEFORE:
                return WriteOperation.DELETE;
            case INSERT:
            case UPDATE_AFTER:
                return resolveInsertOperation(row);
            default:
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                        "Unsupported Milvus CDC row kind: " + rowKind);
        }
    }

    private WriteOperation resolveInsertOperation(SeaTunnelRow row) {
        if (enableUpsert) {
            return WriteOperation.UPSERT;
        }
        if (cdcOverlapUpsertCutoffMs == null) {
            return WriteOperation.INSERT;
        }
        Object eventTime = row.getOptions().get(EVENT_TIME.getName());
        if (!(eventTime instanceof Long) || (Long) eventTime < 0) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                    "cdc_overlap_upsert_cutoff_ms requires source EventTime metadata as a non-negative Long in Unix milliseconds");
        }
        // This is a per-row decision, not a permanent phase switch: another channel or a
        // checkpoint replay can deliver older events after rows beyond the cutoff.
        return (Long) eventTime <= cdcOverlapUpsertCutoffMs
                ? WriteOperation.UPSERT
                : WriteOperation.INSERT;
    }

    private void writeRows(
            WriteOperation operation, String partitionName, List<SeaTunnelRow> rows) {
        long applyStartTimestampMs = System.currentTimeMillis();
        if (operation != WriteOperation.DELETE) {
            ensurePartition(partitionName);
        }
        switch (operation) {
            case INSERT:
                insert(partitionName, rows);
                break;
            case UPSERT:
                upsert(partitionName, rows);
                break;
            case DELETE:
                delete(partitionName, primaryKeys(rows));
                break;
            default:
                throw new IllegalStateException("Unsupported CDC write operation: " + operation);
        }
        long applyTimestampMs = System.currentTimeMillis();
        long applyCostMs = applyTimestampMs - applyStartTimestampMs;
        logAppliedBatch(operation, partitionName, rows, applyTimestampMs, applyCostMs);
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
    }

    @Override
    public long getWriteCache() {
        return writeCache.get();
    }

    @Override
    public void waitJobFinish() {}

    private void insert(String partitionName, List<SeaTunnelRow> elements) {
        // Replay source inserts with their original primary keys. Retries after an
        // ambiguous failure can insert duplicate rows; this operation is not idempotent.
        InsertReq insertReq = buildInsertRequest(partitionName, elements);
        try {
            milvusClient.insert(insertReq);
        } catch (Exception e) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.WRITE_DATA_FAIL, "insert CDC data failed", e);
        }
    }

    private InsertReq buildInsertRequest(String partitionName, List<SeaTunnelRow> elements) {
        InsertReq insertReq =
                InsertReq.builder()
                        .collectionName(collectionName)
                        .data(buildData(elements))
                        .build();
        if (hasExplicitPartition(partitionName)) {
            insertReq.setPartitionName(partitionName);
        }
        return insertReq;
    }

    private void upsert(String partitionName, List<SeaTunnelRow> elements) {
        if (Boolean.TRUE.equals(descriptionCollectionResp.getAutoID())) {
            upsertAutoId(partitionName, elements);
            return;
        }
        UpsertReq upsertReq =
                UpsertReq.builder()
                        .collectionName(collectionName)
                        .data(buildData(elements))
                        .build();
        if (hasExplicitPartition(partitionName)) {
            upsertReq.setPartitionName(partitionName);
        }
        try {
            milvusClient.upsert(upsertReq);
        } catch (Exception e) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.WRITE_DATA_FAIL, "upsert CDC data failed", e);
        }
    }

    private void upsertAutoId(String partitionName, List<SeaTunnelRow> elements) {
        if (!MilvusConnectorUtils.allowInsertAutoId(descriptionCollectionResp)) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INVALID_PARAM,
                    "Milvus CDC upsert requires allow_insert_auto_id=true on the target autoID collection");
        }
        // Convert and validate the entire batch before deleting anything.
        InsertReq insertRequest = buildInsertRequest(partitionName, elements);
        List<Object> primaryKeys = primaryKeys(elements);
        RpcUtils retries = new RpcUtils();
        retries.retryConfig(clientRetryConfig);
        // MilvusSinkWriter's writerOperationLock excludes other client operations for this
        // entire scope, including checkpoint flush and close. The connector creates its client
        // with SDK defaults, so restore those defaults when leaving this scope.
        try {
            milvusClient.retryConfig(RetryConfig.builder().maxRetryTimes(1).build());
            retries.retry(
                    () -> {
                        try {
                            // Milvus SDK 3.0.9 sets filter on DeleteReq without clearing ids.
                            // Reusing it fails the next call's parameter validation, so build
                            // a fresh delete request on every attempt.
                            milvusClient.delete(buildDeleteRequest(partitionName, primaryKeys));
                            milvusClient.insert(insertRequest);
                            return null;
                        } catch (MilvusClientException error) {
                            // The no-retry SDK path wraps transport failures. Unwrap them so
                            // the outer policy can classify and retry delete + insert together.
                            if (error.getErrorCode() == ErrorCode.RPC_ERROR
                                    && error.getCause() instanceof StatusRuntimeException) {
                                throw (StatusRuntimeException) error.getCause();
                            }
                            throw error;
                        }
                    });
        } catch (Exception e) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.WRITE_DATA_FAIL, "upsert CDC AutoID data failed", e);
        } finally {
            milvusClient.retryConfig(clientRetryConfig);
        }
    }

    private List<JsonObject> buildData(List<SeaTunnelRow> elements) {
        List<JsonObject> data = new ArrayList<>(elements.size());
        for (SeaTunnelRow element : elements) {
            data.add(
                    milvusSinkConverter.buildMilvusData(
                            catalogTable,
                            descriptionCollectionResp,
                            milvusFieldMapper,
                            element,
                            MilvusConnectorUtils.allowInsertAutoId(descriptionCollectionResp)));
        }
        return data;
    }

    private boolean hasExplicitPartition(String partitionName) {
        return StringUtils.isNotEmpty(partitionName) && !hasPartitionKey;
    }

    private String targetPartition(SeaTunnelRow row, WriteOperation operation) {
        if (hasPartitionKey) {
            return null;
        }
        String partition = row.getPartitionName();
        if (StringUtils.isEmpty(partition)) {
            // An unspecified delete covers all partitions; an insert uses the default one.
            return operation == WriteOperation.DELETE ? null : DEFAULT_PARTITION;
        }
        return partition.replace('-', '_');
    }

    private void ensurePartition(String partitionName) {
        if (!hasExplicitPartition(partitionName)
                || DEFAULT_PARTITION.equals(partitionName)
                || knownPartitions.contains(partitionName)) {
            return;
        }
        try {
            if (!Boolean.TRUE.equals(
                    milvusClient.hasPartition(
                            HasPartitionReq.builder()
                                    .collectionName(collectionName)
                                    .partitionName(partitionName)
                                    .build()))) {
                milvusClient.createPartition(
                        CreatePartitionReq.builder()
                                .collectionName(collectionName)
                                .partitionName(partitionName)
                                .build());
            }
            knownPartitions.add(partitionName);
        } catch (Exception e) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.CREATE_PARTITION_ERROR, e);
        }
    }

    private DeleteReq buildDeleteRequest(String partitionName, List<Object> primaryKeys) {
        DeleteReq.DeleteReqBuilder builder =
                DeleteReq.builder().collectionName(this.collectionName).ids(primaryKeys);

        if (hasExplicitPartition(partitionName)) {
            builder.partitionName(partitionName);
        }
        return builder.build();
    }

    private void delete(String partitionName, List<Object> primaryKeys) {
        try {
            milvusClient.delete(buildDeleteRequest(partitionName, primaryKeys));
        } catch (Exception e) {
            // Check only after a partition-not-found error, keeping normal deletes to one RPC.
            if (hasExplicitPartition(partitionName)
                    && !DEFAULT_PARTITION.equals(partitionName)
                    && e instanceof MilvusClientException
                    && ((MilvusClientException) e).getServerErrCode() == PARTITION_NOT_FOUND) {
                try {
                    if (Boolean.FALSE.equals(
                            milvusClient.hasPartition(
                                    HasPartitionReq.builder()
                                            .collectionName(collectionName)
                                            .partitionName(partitionName)
                                            .build()))) {
                        knownPartitions.remove(partitionName);
                        return;
                    }
                } catch (Exception lookupError) {
                    e.addSuppressed(lookupError);
                }
            }
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

    private Object requirePrimaryKeyValue(SeaTunnelRow element, WriteOperation operation) {
        Object primaryKey = primaryKeyValue(element);
        if (primaryKey == null) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                    operation == WriteOperation.DELETE
                            ? "delete row primary key must not be null"
                            : "insert row primary key must not be null");
        }
        return primaryKey;
    }

    private void flushPending() {
        if (pendingRows.isEmpty()) {
            return;
        }
        writeRows(pendingOperation, pendingPartitionName, new ArrayList<>(pendingRows.values()));
        lastFlushTime = currentTimeMillisSupplier.getAsLong();
        pendingRows.clear();
        pendingOperation = null;
        pendingPartitionName = null;
        writeCache.set(0L);
    }

    private void flushPendingIfIntervalElapsed() {
        long currentTime = currentTimeMillisSupplier.getAsLong();
        // TODO: Replace this write-triggered interval check with SeaTunnel engine-level
        // timer flush after STIP-23 (#10717 / #10800) is available in this branch.
        if (currentTime < lastFlushTime || currentTime - lastFlushTime >= cdcBatchFlushIntervalMs) {
            flushPending();
        }
    }

    private void logAppliedBatch(
            WriteOperation operation,
            String partitionName,
            List<SeaTunnelRow> rows,
            long applyTimestampMs,
            long applyCostMs) {
        SeaTunnelRow first = rows.get(0);
        SeaTunnelRow last = rows.get(rows.size() - 1);
        log.info(
                "Milvus CDC sink applied batch: collection={}, partition={}, type={}, rowCount={},"
                        + " firstPrimaryKey={}, lastPrimaryKey={}, applyTimestampMs={}, applyCostMs={}",
                collectionName,
                partitionName,
                operation,
                rows.size(),
                primaryKeyValue(first),
                primaryKeyValue(last),
                applyTimestampMs,
                applyCostMs);
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
