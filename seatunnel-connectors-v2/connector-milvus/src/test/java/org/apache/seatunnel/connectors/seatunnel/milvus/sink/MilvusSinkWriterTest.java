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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import io.milvus.v2.service.vector.request.InsertReq;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class MilvusSinkWriterTest {

    @ParameterizedTest
    @ValueSource(ints = {0, 2, 4})
    void cdcRejectsActualParallelismBeforeUsingClient(int parallelism) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusConnectorException error =
                assertThrows(
                        MilvusConnectorException.class,
                        () -> newWriter(client, "CDC", false, parallelism));
        assertTrue(error.getMessage().contains("sink parallelism = 1"));
        assertTrue(error.getMessage().contains("actual: " + parallelism));
        verifyNoInteractions(client);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cdcValidatesParallelismBeforeCreatingOrRestoringClient(boolean restore) {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        when(context.getNumberOfParallelSubtasks()).thenReturn(2);
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(Collections.singletonMap("write_mode", "CDC"));
        MilvusSink sink = new MilvusSink(config, null);
        MilvusConnectorException error =
                assertThrows(
                        MilvusConnectorException.class,
                        () -> {
                            if (restore) {
                                sink.restoreWriter(context, Collections.emptyList());
                            } else {
                                sink.createWriter(context);
                            }
                        });
        assertTrue(error.getMessage().contains("sink parallelism = 1"));
    }

    @Test
    void appendAllowsActualParallelismGreaterThanOne() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusSinkWriter writer = newWriter(client, "APPEND", false, 2);
        writer.write(row(1L, "p1"));
        writer.close();
        verify(client).insert(any(InsertReq.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {"CDC", "APPEND"})
    void closeRejectsNewPartitionWithoutCreatingWriter(String mode) throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusSinkWriter writer = newWriter(client, mode);
        writer.close();
        org.mockito.Mockito.clearInvocations(client);
        assertThrows(IllegalStateException.class, () -> writer.write(row(1L, "new_partition")));
        assertThrows(IOException.class, writer::prepareCommit);
        writer.close();
        verifyNoInteractions(client);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void collectionWriterPreservesGlobalDeleteOrder(boolean autoId) throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusSinkWriter writer = newWriter(client, "CDC", autoId);
        // Both partitions have already been used, so writer creation cannot mask reordering.
        writer.write(row(100L, "p1"));
        writer.write(row(200L, "_default"));
        writer.prepareCommit();
        org.mockito.Mockito.clearInvocations(client);

        writer.write(row(42L, "p1"));
        SeaTunnelRow delete = row(42L, null);
        delete.setRowKind(RowKind.DELETE);
        writer.write(delete);
        writer.write(row(99L, "_default"));
        writer.prepareCommit();

        InOrder order = inOrder(client);
        order.verify(client).insert(argThat(req -> "p1".equals(req.getPartitionName())));
        order.verify(client)
                .delete(
                        argThat(
                                req ->
                                        req.getPartitionName() == null
                                                || req.getPartitionName().isEmpty()));
        order.verify(client)
                .insert(argThat(req -> req.getData().get(0).get("id").getAsLong() == 99L));
        order.verifyNoMoreInteractions();
    }

    @Test
    void defaultAndGlobalDeletesHaveDifferentScopes() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusSinkWriter writer = newWriter(client);
        SeaTunnelRow scoped = row(42L, "_default");
        scoped.setRowKind(RowKind.DELETE);
        writer.write(scoped);
        SeaTunnelRow global = row(42L, null);
        global.setRowKind(RowKind.DELETE);
        writer.write(global);
        writer.prepareCommit();

        InOrder order = inOrder(client);
        order.verify(client).delete(argThat(req -> "_default".equals(req.getPartitionName())));
        order.verify(client)
                .delete(
                        argThat(
                                req ->
                                        req.getPartitionName() == null
                                                || req.getPartitionName().isEmpty()));
        verify(client, org.mockito.Mockito.never()).hasPartition(any(HasPartitionReq.class));
    }

    @Test
    void partitionSwitchKeepsSamePrimaryKeyInSeparateBatches() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusSinkWriter writer = newWriter(client);
        writer.write(row(42L, "a"));
        writer.write(row(42L, "b"));
        writer.write(row(42L, "a"));
        writer.prepareCommit();

        InOrder order = inOrder(client);
        order.verify(client).insert(argThat(req -> "a".equals(req.getPartitionName())));
        order.verify(client).insert(argThat(req -> "b".equals(req.getPartitionName())));
        order.verify(client).insert(argThat(req -> "a".equals(req.getPartitionName())));
    }

    @Test
    void failedPartitionSwitchDoesNotAcceptTheNextRow() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusSinkWriter writer = newWriter(client);
        when(client.insert(any(InsertReq.class)))
                .thenThrow(new IllegalStateException("ambiguous failure"))
                .thenReturn(null);
        writer.write(row(1L, "a"));
        assertThrows(RuntimeException.class, () -> writer.write(row(2L, "b")));
        writer.prepareCommit();
        verify(client, times(2)).insert(argThat(req -> "a".equals(req.getPartitionName())));
        verify(client, org.mockito.Mockito.never())
                .insert(argThat(req -> "b".equals(req.getPartitionName())));
        writer.write(row(2L, "b"));
        writer.prepareCommit();
        verify(client).insert(argThat(req -> "b".equals(req.getPartitionName())));
    }

    @Test
    void cdcCreatesPartitionsOnlyForWritesAndReusesThem() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusSinkWriter writer = newWriter(client);
        when(client.hasPartition(any(HasPartitionReq.class))).thenReturn(false);
        writer.write(row(1L, "a"));
        writer.prepareCommit();
        writer.write(row(2L, "a"));
        writer.prepareCommit();
        SeaTunnelRow delete = row(3L, "b");
        delete.setRowKind(RowKind.DELETE);
        writer.write(delete);
        writer.prepareCommit();
        verify(client).hasPartition(argThat(req -> "a".equals(req.getPartitionName())));
        verify(client).createPartition(argThat(req -> "a".equals(req.getPartitionName())));
        verify(client, times(1)).createPartition(any(CreatePartitionReq.class));
        verify(client, times(1)).hasPartition(any(HasPartitionReq.class));
        verify(client).delete(argThat(req -> "b".equals(req.getPartitionName())));
    }

    @Test
    void concurrentCloseDoesNotAcceptRowsIntoTheClosingBatch() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusSinkWriter writer = newWriter(client, "CDC", true);
        CountDownLatch flushing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(client.insert(any(InsertReq.class)))
                .thenAnswer(
                        invocation -> {
                            flushing.countDown();
                            assertTrue(release.await(5, TimeUnit.SECONDS));
                            return null;
                        });
        writer.write(row(1L, "_default"));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> closing =
                    executor.submit(
                            () -> {
                                writer.close();
                                return null;
                            });
            assertTrue(flushing.await(5, TimeUnit.SECONDS));
            Future<?> writing = executor.submit(() -> writer.write(row(2L, "_default")));
            release.countDown();
            closing.get(5, TimeUnit.SECONDS);
            ExecutionException error =
                    assertThrows(ExecutionException.class, () -> writing.get(5, TimeUnit.SECONDS));
            assertTrue(error.getCause() instanceof IllegalStateException);
            verify(client)
                    .insert(
                            argThat(
                                    req ->
                                            req.getData().size() == 1
                                                    && req.getData().get(0).get("id").getAsLong()
                                                            == 1L));
            verify(client).close(10);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void closeFlushesAllPartitionsBeforeClosingClientOnce() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        AtomicBoolean clientClosed = new AtomicBoolean();
        doAnswer(
                        invocation -> {
                            clientClosed.set(true);
                            return null;
                        })
                .when(client)
                .close(anyLong());
        when(client.insert(any(InsertReq.class)))
                .thenAnswer(
                        invocation -> {
                            if (clientClosed.get()) {
                                throw new IllegalStateException("client already closed");
                            }
                            return null;
                        });
        MilvusSinkWriter writer = newWriter(client);
        writer.write(row(1L, "a"));
        writer.write(row(2L, "b"));
        writer.write(row(3L, "a"));

        writer.close();
        writer.close();

        InOrder order = inOrder(client);
        order.verify(client, times(3)).insert(any(InsertReq.class));
        order.verify(client).close(10);
        verify(client, times(1)).close(anyLong());
    }

    @Test
    void closeReleasesClientWhenTheRemainingCdcBatchFails() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        AtomicBoolean failed = new AtomicBoolean();
        when(client.insert(any(InsertReq.class)))
                .thenAnswer(
                        invocation -> {
                            InsertReq request = invocation.getArgument(0);
                            if (request.getData().get(0).get("id").getAsLong() == 2L) {
                                failed.set(true);
                                throw new IllegalStateException("injected partition flush failure");
                            }
                            return null;
                        });
        MilvusSinkWriter writer = newWriter(client);
        writer.write(row(1L, "a"));
        writer.write(row(2L, "b"));

        IOException error = assertThrows(IOException.class, writer::close);

        assertTrue(failed.get());
        assertTrue(error.getCause().getMessage().contains("insert CDC data failed"));
        verify(client, times(2)).insert(any(InsertReq.class));
        verify(client).close(10);
        writer.close();
        verify(client, times(1)).close(anyLong());
    }

    @Test
    void closeReleasesClientWhenNoRowsWereWritten() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusSinkWriter writer = newWriter(client);

        writer.close();
        writer.close();

        verify(client, times(1)).close(10);
    }

    @Test
    void appendCloseAlsoFlushesAllPartitionsBeforeClosingSharedClient() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        AtomicBoolean clientClosed = new AtomicBoolean();
        doAnswer(
                        invocation -> {
                            clientClosed.set(true);
                            return null;
                        })
                .when(client)
                .close(anyLong());
        when(client.insert(any(InsertReq.class)))
                .thenAnswer(
                        invocation -> {
                            if (clientClosed.get()) {
                                throw new IllegalStateException("client already closed");
                            }
                            return null;
                        });
        MilvusSinkWriter writer = newWriter(client, "APPEND");
        writer.write(row(1L, "a"));
        writer.write(row(2L, "b"));
        writer.write(row(3L, "a"));

        writer.close();

        InOrder order = inOrder(client);
        order.verify(client, times(3)).insert(any(InsertReq.class));
        order.verify(client).close(10);
    }

    @Test
    void closePreservesAllFlushAndClientFailures() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusSinkWriter writer = newWriter(client);
        writer.write(row(1L, "a"));
        writer.write(row(2L, "b"));
        writer.write(row(3L, "a"));
        when(client.insert(any(InsertReq.class)))
                .thenThrow(new IllegalStateException("injected flush failure"));
        doThrow(new IllegalStateException("injected client close failure"))
                .when(client)
                .close(anyLong());

        IOException error = assertThrows(IOException.class, writer::close);

        assertEquals(1, error.getSuppressed().length);
        verify(client, times(3)).insert(any(InsertReq.class));
        verify(client).close(10);
    }

    @Test
    void autoIdCheckpointFailurePropagatesAndRetainsPendingInsert() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        when(client.insert(any(InsertReq.class)))
                .thenThrow(new IllegalStateException("ambiguous insert failure"))
                .thenReturn(null);
        MilvusSinkWriter writer = newWriter(client, "CDC", true);
        writer.write(row(42L, "_default"));
        IOException error = assertThrows(IOException.class, writer::prepareCommit);
        assertTrue(error.getCause().getMessage().contains("insert CDC data failed"));
        writer.prepareCommit();
        writer.close();
        verify(client, times(2)).insert(any(InsertReq.class));
    }

    private static MilvusSinkWriter newWriter(MilvusClientV2 client) {
        return newWriter(client, "CDC");
    }

    private static MilvusSinkWriter newWriter(MilvusClientV2 client, String writeMode) {
        return newWriter(client, writeMode, false);
    }

    private static MilvusSinkWriter newWriter(
            MilvusClientV2 client, String writeMode, boolean autoId) {
        return newWriter(client, writeMode, autoId, 1);
    }

    private static MilvusSinkWriter newWriter(
            MilvusClientV2 client, String writeMode, boolean autoId, int parallelism) {
        CreateCollectionReq.FieldSchema id =
                CreateCollectionReq.FieldSchema.builder()
                        .name("id")
                        .dataType(DataType.Int64)
                        .isPrimaryKey(true)
                        .build();
        CreateCollectionReq.CollectionSchema schema =
                CreateCollectionReq.CollectionSchema.builder()
                        .fieldSchemaList(Collections.singletonList(id))
                        .functionList(Collections.emptyList())
                        .build();
        when(client.describeCollection(any()))
                .thenReturn(
                        DescribeCollectionResp.builder()
                                .collectionName("review")
                                .primaryFieldName("id")
                                .autoID(autoId)
                                .properties(
                                        Collections.singletonMap("allow_insert_auto_id", "true"))
                                .collectionSchema(schema)
                                .build());
        when(client.hasPartition(any(HasPartitionReq.class))).thenReturn(true);
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("test", TablePath.of("default", "review")),
                        TableSchema.builder()
                                .columns(
                                        Collections.singletonList(
                                                PhysicalColumn.builder()
                                                        .name("id")
                                                        .dataType(BasicType.LONG_TYPE)
                                                        .build()))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "");
        Map<String, Object> config = new HashMap<>();
        config.put("write_mode", writeMode);
        config.put("batch_size", 100);
        config.put("cdc_batch_flush_interval_ms", Long.MAX_VALUE);
        // The runtime value must win over an apparently valid raw configuration.
        config.put("parallelism", 1);
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        when(context.getNumberOfParallelSubtasks()).thenReturn(parallelism);
        return new MilvusSinkWriter(
                context, table, ReadonlyConfig.fromMap(config), Collections.emptyList(), client);
    }

    private static SeaTunnelRow row(long id, String partition) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id});
        row.setRowKind(RowKind.INSERT);
        row.setPartitionName(partition);
        return row;
    }

    @Test
    void rejectsRuntimeSchemaChanges() {
        MilvusSinkWriter writer = mock(MilvusSinkWriter.class, CALLS_REAL_METHODS);
        SchemaChangeEvent event = mock(SchemaChangeEvent.class);

        IOException exception =
                assertThrows(IOException.class, () -> writer.applySchemaChange(event));

        assertTrue(exception.getMessage().contains("does not support runtime schema changes"));
    }
}
