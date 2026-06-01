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

package org.apache.seatunnel.e2e.connector.v2.milvus;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.GeometryType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog.MilvusCatalog;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.milvus.MilvusContainer;

import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.QueryResults;
import io.milvus.param.ConnectParam;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.DeleteParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.response.QueryResultsWrapper;
import lombok.extern.slf4j.Slf4j;
import milvus.com.google.gson.Gson;
import milvus.com.google.gson.JsonObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Currently SPARK and FLINK not support adapt")
public class MilvusIT extends TestSuiteBase implements TestResource {

    private static final String HOST = "milvus-e2e";
    // Using Milvus 2.6+ for GEOMETRY and Timestamptz support
    private static final String MILVUS_IMAGE = "milvusdb/milvus:v2.6-latest";
    private static final String TOKEN = "root:Milvus";
    private MilvusContainer container;
    private MilvusServiceClient milvusClient;
    private static final String COLLECTION_NAME = "simple_example";
    private static final String COLLECTION_NAME_1 = "simple_example_1";
    private static final String COLLECTION_NAME_2 = "simple_example_2";
    private static final String COLLECTION_NAME_WITH_PARTITIONKEY =
            "simple_example_with_partitionkey";
    private static final String COLLECTION_NAME_NEW_TYPES = "test_new_types";
    private static final String CDC_SOURCE_COLLECTION = "milvus_cdc_source_e2e";
    private static final String CDC_TARGET_COLLECTION = "milvus_cdc_target_e2e";
    private static final String CDC_ID_FIELD = "id";
    private static final String CDC_VECTOR_FIELD = "embedding";
    private static final String CDC_TITLE_FIELD = "title";
    private static final long CDC_RECORD_ID = 1001L;
    private static final String CDC_RECORD_TITLE = "Milvus CDC E2E";
    private static final String ID_FIELD = "book_id";
    private static final String VECTOR_FIELD = "book_intro";
    private static final String VECTOR_FIELD2 = "book_kind";
    private static final String VECTOR_FIELD3 = "book_binary";
    private static final String VECTOR_FIELD4 = "book_map";

    private static final String TITLE_FIELD = "book_title";
    private static final Integer VECTOR_DIM = 4;

    private Catalog catalog;
    private static final Gson gson = new Gson();

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.container =
                new MilvusContainer(MILVUS_IMAGE)
                        .withEnv("DEPLOY_MODE", "STANDALONE")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST);
        Startables.deepStart(Stream.of(this.container)).join();
        log.info("Milvus host is {}", container.getHost());
        log.info("Milvus container started");
        Awaitility.given().ignoreExceptions().await().atMost(720L, TimeUnit.SECONDS);
        this.initMilvus();
        this.initSourceData();
    }

    private void initMilvus()
            throws SQLException, ClassNotFoundException, InstantiationException,
                    IllegalAccessException {
        Map<String, Object> config = new HashMap<>();
        config.put(MilvusSinkConfig.URL.key(), this.container.getEndpoint());
        config.put(MilvusSinkConfig.TOKEN.key(), TOKEN);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(config);
        catalog = new MilvusCatalog(COLLECTION_NAME, readonlyConfig);
        catalog.open();
        milvusClient =
                new MilvusServiceClient(
                        ConnectParam.newBuilder()
                                .withUri(this.container.getEndpoint())
                                .withToken(TOKEN)
                                .build());
    }

    private void initSourceData() {
        // Define fields
        List<FieldType> fieldsSchema =
                Arrays.asList(
                        FieldType.newBuilder()
                                .withName(ID_FIELD)
                                .withDataType(DataType.Int64)
                                .withPrimaryKey(true)
                                .withAutoID(false)
                                .build(),
                        FieldType.newBuilder()
                                .withName(VECTOR_FIELD)
                                .withDataType(DataType.FloatVector)
                                .withDimension(VECTOR_DIM)
                                .build(),
                        FieldType.newBuilder()
                                .withName(VECTOR_FIELD2)
                                .withDataType(DataType.Float16Vector)
                                .withDimension(VECTOR_DIM)
                                .build(),
                        FieldType.newBuilder()
                                .withName(VECTOR_FIELD3)
                                .withDataType(DataType.BinaryVector)
                                .withDimension(VECTOR_DIM * 2)
                                .build(),
                        FieldType.newBuilder()
                                .withName(VECTOR_FIELD4)
                                .withDataType(DataType.SparseFloatVector)
                                .build(),
                        FieldType.newBuilder()
                                .withName(TITLE_FIELD)
                                .withDataType(DataType.VarChar)
                                .withMaxLength(64)
                                .build());

        // Create the collection with 3 fields
        R<RpcStatus> ret =
                milvusClient.createCollection(
                        CreateCollectionParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME)
                                .withFieldTypes(fieldsSchema)
                                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to create collection! Error: " + ret.getMessage());
        }

        // Specify an index type on the vector field.
        ret =
                milvusClient.createIndex(
                        CreateIndexParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME)
                                .withFieldName(VECTOR_FIELD)
                                .withIndexType(IndexType.FLAT)
                                .withMetricType(MetricType.L2)
                                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(
                    "Failed to create index on vector field! Error: " + ret.getMessage());
        }

        ret =
                milvusClient.createIndex(
                        CreateIndexParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME)
                                .withFieldName(VECTOR_FIELD2)
                                .withIndexType(IndexType.FLAT)
                                .withMetricType(MetricType.L2)
                                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(
                    "Failed to create index on vector field! Error: " + ret.getMessage());
        }
        ret =
                milvusClient.createIndex(
                        CreateIndexParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME)
                                .withFieldName(VECTOR_FIELD3)
                                .withIndexType(IndexType.BIN_FLAT)
                                .withMetricType(MetricType.HAMMING)
                                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(
                    "Failed to create index on vector field! Error: " + ret.getMessage());
        }

        ret =
                milvusClient.createIndex(
                        CreateIndexParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME)
                                .withFieldName(VECTOR_FIELD4)
                                .withIndexType(IndexType.SPARSE_INVERTED_INDEX)
                                .withMetricType(MetricType.IP)
                                .build());
        if (ret.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(
                    "Failed to create index on vector field! Error: " + ret.getMessage());
        }

        // Call loadCollection() to enable automatically loading data into memory for searching
        milvusClient.loadCollection(
                LoadCollectionParam.newBuilder().withCollectionName(COLLECTION_NAME).build());

        log.info("Collection created");

        // Define fields With Partition Key
        List<FieldType> fieldsSchemaWithPartitionKey =
                Arrays.asList(
                        FieldType.newBuilder()
                                .withName(ID_FIELD)
                                .withDataType(DataType.Int64)
                                .withPrimaryKey(true)
                                .withAutoID(false)
                                .build(),
                        FieldType.newBuilder()
                                .withName(VECTOR_FIELD)
                                .withDataType(DataType.FloatVector)
                                .withDimension(VECTOR_DIM)
                                .build(),
                        FieldType.newBuilder()
                                .withName(VECTOR_FIELD2)
                                .withDataType(DataType.Float16Vector)
                                .withDimension(VECTOR_DIM)
                                .build(),
                        FieldType.newBuilder()
                                .withName(VECTOR_FIELD3)
                                .withDataType(DataType.BinaryVector)
                                .withDimension(VECTOR_DIM * 2)
                                .build(),
                        FieldType.newBuilder()
                                .withName(VECTOR_FIELD4)
                                .withDataType(DataType.SparseFloatVector)
                                .build(),
                        FieldType.newBuilder()
                                .withName(TITLE_FIELD)
                                .withDataType(DataType.VarChar)
                                .withPartitionKey(true)
                                .withMaxLength(64)
                                .build());

        // Create the collection with 3 fields
        R<RpcStatus> ret2 =
                milvusClient.createCollection(
                        CreateCollectionParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME_WITH_PARTITIONKEY)
                                .withFieldTypes(fieldsSchemaWithPartitionKey)
                                .build());
        if (ret2.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to create collection! Error: " + ret.getMessage());
        }

        // Specify an index type on the vector field.
        ret2 =
                milvusClient.createIndex(
                        CreateIndexParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME_WITH_PARTITIONKEY)
                                .withFieldName(VECTOR_FIELD)
                                .withIndexType(IndexType.FLAT)
                                .withMetricType(MetricType.L2)
                                .build());
        if (ret2.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(
                    "Failed to create index on vector field! Error: " + ret.getMessage());
        }

        ret2 =
                milvusClient.createIndex(
                        CreateIndexParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME_WITH_PARTITIONKEY)
                                .withFieldName(VECTOR_FIELD2)
                                .withIndexType(IndexType.FLAT)
                                .withMetricType(MetricType.L2)
                                .build());
        if (ret2.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(
                    "Failed to create index on vector field! Error: " + ret.getMessage());
        }
        ret2 =
                milvusClient.createIndex(
                        CreateIndexParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME_WITH_PARTITIONKEY)
                                .withFieldName(VECTOR_FIELD3)
                                .withIndexType(IndexType.BIN_FLAT)
                                .withMetricType(MetricType.HAMMING)
                                .build());
        if (ret2.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(
                    "Failed to create index on vector field! Error: " + ret.getMessage());
        }

        ret2 =
                milvusClient.createIndex(
                        CreateIndexParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME_WITH_PARTITIONKEY)
                                .withFieldName(VECTOR_FIELD4)
                                .withIndexType(IndexType.SPARSE_INVERTED_INDEX)
                                .withMetricType(MetricType.IP)
                                .build());
        if (ret2.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException(
                    "Failed to create index on vector field! Error: " + ret.getMessage());
        }

        // Call loadCollection() to enable automatically loading data into memory for searching
        milvusClient.loadCollection(
                LoadCollectionParam.newBuilder()
                        .withCollectionName(COLLECTION_NAME_WITH_PARTITIONKEY)
                        .build());

        log.info("Collection created");

        // Insert 10 records into the collection
        List<JsonObject> rows = new ArrayList<>();
        for (long i = 1L; i <= 10; ++i) {

            JsonObject row = new JsonObject();
            row.add(ID_FIELD, gson.toJsonTree(i));
            List<Float> vector = Arrays.asList((float) i, (float) i, (float) i, (float) i);
            row.add(VECTOR_FIELD, gson.toJsonTree(vector));
            Short[] shorts = {(short) i, (short) i, (short) i, (short) i};
            ByteBuffer shortByteBuffer = BufferUtils.toByteBuffer(shorts);
            row.add(VECTOR_FIELD2, gson.toJsonTree(shortByteBuffer.array()));
            ByteBuffer binaryByteBuffer = ByteBuffer.wrap(new byte[] {16});
            row.add(VECTOR_FIELD3, gson.toJsonTree(binaryByteBuffer.array()));
            HashMap<Long, Float> sparse = new HashMap<>();
            sparse.put(1L, 1.0f);
            sparse.put(2L, 2.0f);
            sparse.put(3L, 3.0f);
            sparse.put(4L, 4.0f);
            row.add(VECTOR_FIELD4, gson.toJsonTree(sparse));
            row.addProperty(TITLE_FIELD, "Tom and Jerry " + i);
            rows.add(row);
        }

        R<MutationResult> insertRet =
                milvusClient.insert(
                        InsertParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME)
                                .withRows(rows)
                                .build());

        R<MutationResult> insertRet2 =
                milvusClient.insert(
                        InsertParam.newBuilder()
                                .withCollectionName(COLLECTION_NAME_WITH_PARTITIONKEY)
                                .withRows(rows)
                                .build());

        if (insertRet.getStatus() != R.Status.Success.getCode()
                || insertRet2.getStatus() != R.Status.Success.getCode()) {
            throw new RuntimeException("Failed to insert! Error: " + insertRet.getMessage());
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        this.milvusClient.close();
        this.container.close();
        if (catalog != null) {
            catalog.close();
        }
    }

    @TestTemplate
    public void testMilvus(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/milvus-to-milvus.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // assert table exist
        R<Boolean> hasCollectionResponse =
                this.milvusClient.hasCollection(
                        HasCollectionParam.newBuilder()
                                .withDatabaseName("default")
                                .withCollectionName(COLLECTION_NAME)
                                .build());
        Assertions.assertTrue(hasCollectionResponse.getData());

        // check table fields
        R<DescribeCollectionResponse> describeCollectionResponseR =
                this.milvusClient.describeCollection(
                        DescribeCollectionParam.newBuilder()
                                .withDatabaseName("default")
                                .withCollectionName(COLLECTION_NAME)
                                .build());

        DescribeCollectionResponse data = describeCollectionResponseR.getData();
        List<String> fileds =
                data.getSchema().getFieldsList().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList());
        Assertions.assertTrue(fileds.contains(ID_FIELD));
        Assertions.assertTrue(fileds.contains(VECTOR_FIELD));
        Assertions.assertTrue(fileds.contains(VECTOR_FIELD2));
        Assertions.assertTrue(fileds.contains(VECTOR_FIELD3));
        Assertions.assertTrue(fileds.contains(VECTOR_FIELD4));
        Assertions.assertTrue(fileds.contains(TITLE_FIELD));
    }

    @TestTemplate
    public void testMilvusCdc(TestContainer container) throws Exception {
        createCdcCollection(CDC_SOURCE_COLLECTION, false);
        createCdcCollection(CDC_TARGET_COLLECTION, true);

        String pchannel = getCdcPhysicalChannel();
        String jobId = String.valueOf(System.currentTimeMillis());
        CompletableFuture<Container.ExecResult> jobFuture =
                executeCdcJobAsync(container, jobId, pchannel);

        try {
            waitForCdcJobRunning(container, jobId, jobFuture);

            insertCdcSourceRow();
            Awaitility.await()
                    .ignoreExceptions()
                    .atMost(2, TimeUnit.MINUTES)
                    .untilAsserted(this::assertCdcTargetRow);

            deleteCdcSourceRow();
            Awaitility.await()
                    .ignoreExceptions()
                    .atMost(2, TimeUnit.MINUTES)
                    .untilAsserted(() -> Assertions.assertEquals(0, queryCdcTargetRows().size()));
        } finally {
            cancelCdcJob(container, jobId, jobFuture);
        }

        Container.ExecResult jobResult = jobFuture.get(2, TimeUnit.MINUTES);
        Assertions.assertEquals(0, jobResult.getExitCode(), jobResult.getStderr());
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> Assertions.assertEquals("CANCELED", container.getJobStatus(jobId)));
    }

    private void createCdcCollection(String collectionName, boolean loadCollection) {
        List<FieldType> fields =
                Arrays.asList(
                        FieldType.newBuilder()
                                .withName(CDC_ID_FIELD)
                                .withDataType(DataType.Int64)
                                .withPrimaryKey(true)
                                .withAutoID(false)
                                .build(),
                        FieldType.newBuilder()
                                .withName(CDC_VECTOR_FIELD)
                                .withDataType(DataType.FloatVector)
                                .withDimension(VECTOR_DIM)
                                .build(),
                        FieldType.newBuilder()
                                .withName(CDC_TITLE_FIELD)
                                .withDataType(DataType.VarChar)
                                .withMaxLength(64)
                                .build());

        R<RpcStatus> createResponse =
                milvusClient.createCollection(
                        CreateCollectionParam.newBuilder()
                                .withCollectionName(collectionName)
                                .withShardsNum(1)
                                .withFieldTypes(fields)
                                .build());
        assertMilvusSuccess(createResponse, "create collection " + collectionName);

        if (!loadCollection) {
            return;
        }

        R<RpcStatus> indexResponse =
                milvusClient.createIndex(
                        CreateIndexParam.newBuilder()
                                .withCollectionName(collectionName)
                                .withFieldName(CDC_VECTOR_FIELD)
                                .withIndexType(IndexType.FLAT)
                                .withMetricType(MetricType.L2)
                                .build());
        assertMilvusSuccess(indexResponse, "create index for " + collectionName);

        R<RpcStatus> loadResponse =
                milvusClient.loadCollection(
                        LoadCollectionParam.newBuilder()
                                .withCollectionName(collectionName)
                                .build());
        assertMilvusSuccess(loadResponse, "load collection " + collectionName);
    }

    private String getCdcPhysicalChannel() {
        R<DescribeCollectionResponse> describeResponse =
                milvusClient.describeCollection(
                        DescribeCollectionParam.newBuilder()
                                .withCollectionName(CDC_SOURCE_COLLECTION)
                                .build());
        assertMilvusSuccess(describeResponse, "describe CDC source collection");

        List<String> virtualChannels = describeResponse.getData().getVirtualChannelNamesList();
        Assertions.assertEquals(
                1, virtualChannels.size(), "The CDC E2E source must use exactly one shard");
        String virtualChannel = virtualChannels.get(0);
        String physicalChannel = virtualChannel.replaceFirst("_\\d+v\\d+$", "");
        Assertions.assertNotEquals(
                virtualChannel,
                physicalChannel,
                "Unable to derive a physical channel from " + virtualChannel);
        return physicalChannel;
    }

    private CompletableFuture<Container.ExecResult> executeCdcJobAsync(
            TestContainer container, String jobId, String pchannel) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return container.executeJob(
                                "/milvus-cdc-to-milvus.conf", jobId, "cdc_pchannel=" + pchannel);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(e);
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    }
                });
    }

    private void waitForCdcJobRunning(
            TestContainer container,
            String jobId,
            CompletableFuture<Container.ExecResult> jobFuture) {
        Awaitility.await()
                .ignoreExceptions()
                .atMost(2, TimeUnit.MINUTES)
                .until(() -> jobFuture.isDone() || "RUNNING".equals(container.getJobStatus(jobId)));

        if (jobFuture.isDone()) {
            Container.ExecResult result = jobFuture.join();
            Assertions.fail(
                    "CDC job exited before reaching RUNNING. stdout="
                            + result.getStdout()
                            + ", stderr="
                            + result.getStderr());
        }
    }

    private void insertCdcSourceRow() {
        JsonObject row = new JsonObject();
        row.addProperty(CDC_ID_FIELD, CDC_RECORD_ID);
        row.add(CDC_VECTOR_FIELD, gson.toJsonTree(Arrays.asList(1.0F, 2.0F, 3.0F, 4.0F)));
        row.addProperty(CDC_TITLE_FIELD, CDC_RECORD_TITLE);

        R<MutationResult> insertResponse =
                milvusClient.insert(
                        InsertParam.newBuilder()
                                .withCollectionName(CDC_SOURCE_COLLECTION)
                                .withRows(Collections.singletonList(row))
                                .build());
        assertMilvusSuccess(insertResponse, "insert CDC source row");
    }

    private void deleteCdcSourceRow() {
        R<MutationResult> deleteResponse =
                milvusClient.delete(
                        DeleteParam.newBuilder()
                                .withCollectionName(CDC_SOURCE_COLLECTION)
                                .withExpr(CDC_ID_FIELD + " == " + CDC_RECORD_ID)
                                .build());
        assertMilvusSuccess(deleteResponse, "delete CDC source row");
    }

    private void assertCdcTargetRow() {
        List<QueryResultsWrapper.RowRecord> rows = queryCdcTargetRows();
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(CDC_RECORD_TITLE, rows.get(0).get(CDC_TITLE_FIELD));
    }

    private List<QueryResultsWrapper.RowRecord> queryCdcTargetRows() {
        R<QueryResults> queryResponse =
                milvusClient.query(
                        QueryParam.newBuilder()
                                .withCollectionName(CDC_TARGET_COLLECTION)
                                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                                .withExpr(CDC_ID_FIELD + " == " + CDC_RECORD_ID)
                                .withOutFields(Arrays.asList(CDC_ID_FIELD, CDC_TITLE_FIELD))
                                .build());
        assertMilvusSuccess(queryResponse, "query CDC target row");
        return new QueryResultsWrapper(queryResponse.getData()).getRowRecords();
    }

    private void cancelCdcJob(
            TestContainer container,
            String jobId,
            CompletableFuture<Container.ExecResult> jobFuture) {
        if (jobFuture.isDone()) {
            return;
        }
        try {
            Container.ExecResult cancelResult = container.cancelJob(jobId);
            if (cancelResult.getExitCode() != 0) {
                log.warn("Failed to cancel CDC E2E job {}: {}", jobId, cancelResult.getStderr());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            jobFuture.cancel(true);
            log.warn("Interrupted while canceling CDC E2E job {}", jobId, e);
        } catch (IOException e) {
            jobFuture.cancel(true);
            log.warn("Failed to cancel CDC E2E job {}", jobId, e);
        }
    }

    private void assertMilvusSuccess(R<?> response, String operation) {
        if (response.getStatus() == null || response.getStatus() != R.Status.Success.getCode()) {
            Exception exception = response.getException();
            String details =
                    exception == null ? "status=" + response.getStatus() : exception.getMessage();
            Assertions.fail(operation + " failed: " + details);
        }
    }

    @TestTemplate
    public void testNewTypesInCatalog(TestContainer container)
            throws TableAlreadyExistException, DatabaseAlreadyExistException,
                    TableNotExistException {
        // Test GEOMETRY, TIMESTAMP, and Struct types support in catalog

        // Create a table with new types
        TablePath tablePath = TablePath.of("default", COLLECTION_NAME_NEW_TYPES);

        // Create columns with GEOMETRY, TIMESTAMP, and Struct types
        PhysicalColumn idColumn =
                PhysicalColumn.builder()
                        .name("id")
                        .dataType(BasicType.LONG_TYPE)
                        .nullable(false)
                        .build();

        PhysicalColumn geometryColumn =
                PhysicalColumn.builder()
                        .name("location")
                        .dataType(GeometryType.GEOMETRY_TYPE)
                        .nullable(true)
                        .comment("Geometry field for spatial data")
                        .build();

        PhysicalColumn timestampColumn =
                PhysicalColumn.builder()
                        .name("created_at")
                        .dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE)
                        .nullable(true)
                        .comment("Timestamp field")
                        .build();

        PhysicalColumn structColumn =
                PhysicalColumn.builder()
                        .name("metadata")
                        .dataType(BasicType.STRING_TYPE)
                        .nullable(true)
                        .comment("Struct field stored as JSON")
                        .options(
                                Collections.singletonMap(
                                        org.apache.seatunnel.api.table.type.CommonOptions.JSON
                                                .getName(),
                                        true))
                        .build();

        PhysicalColumn vectorColumn =
                PhysicalColumn.builder()
                        .name("embedding")
                        .dataType(VectorType.VECTOR_FLOAT_TYPE)
                        .scale(4)
                        .nullable(true)
                        .build();

        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                Arrays.asList(
                                        idColumn,
                                        geometryColumn,
                                        timestampColumn,
                                        structColumn,
                                        vectorColumn))
                        .primaryKey(PrimaryKey.of("pk", Collections.singletonList("id")))
                        .build();

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(
                                "milvus", tablePath.getDatabaseName(), tablePath.getTableName()),
                        tableSchema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "Test table with new types");

        // Create the table
        catalog.createTable(tablePath, catalogTable, false);

        // Verify the table was created
        R<Boolean> hasCollectionResponse =
                this.milvusClient.hasCollection(
                        HasCollectionParam.newBuilder()
                                .withDatabaseName("default")
                                .withCollectionName(COLLECTION_NAME_NEW_TYPES)
                                .build());
        Assertions.assertTrue(
                hasCollectionResponse.getData(), "Collection should exist after creation");

        // Verify the schema
        R<DescribeCollectionResponse> describeResponse =
                this.milvusClient.describeCollection(
                        DescribeCollectionParam.newBuilder()
                                .withDatabaseName("default")
                                .withCollectionName(COLLECTION_NAME_NEW_TYPES)
                                .build());

        Assertions.assertEquals(
                R.Status.Success.getCode(),
                describeResponse.getStatus(),
                "Should successfully describe collection");

        DescribeCollectionResponse response = describeResponse.getData();
        List<FieldSchema> fields = response.getSchema().getFieldsList();

        // Verify fields
        Map<String, DataType> fieldMap =
                fields.stream()
                        .collect(Collectors.toMap(FieldSchema::getName, FieldSchema::getDataType));

        Assertions.assertTrue(fieldMap.containsKey("id"), "Should have id field");
        Assertions.assertEquals(DataType.Int64, fieldMap.get("id"), "ID field should be Int64");

        Assertions.assertTrue(fieldMap.containsKey("location"), "Should have location field");
        Assertions.assertEquals(
                DataType.Geometry,
                fieldMap.get("location"),
                "Location field should be Geometry type");

        Assertions.assertTrue(fieldMap.containsKey("created_at"), "Should have created_at field");
        Assertions.assertEquals(
                DataType.Timestamptz,
                fieldMap.get("created_at"),
                "Created_at field should be Timestamptz type");

        Assertions.assertTrue(fieldMap.containsKey("metadata"), "Should have metadata field");
        Assertions.assertEquals(
                DataType.Struct, fieldMap.get("metadata"), "Metadata field should be Struct type");

        Assertions.assertTrue(fieldMap.containsKey("embedding"), "Should have embedding field");
        Assertions.assertEquals(
                DataType.FloatVector,
                fieldMap.get("embedding"),
                "Embedding field should be FloatVector type");

        // Clean up - drop the test collection
        catalog.dropTable(tablePath, false);

        log.info("Test for GEOMETRY, TIMESTAMP, and Struct types completed successfully");
    }
}
