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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.UpsertReq;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MilvusSinkWriterTest {

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
        when(client.upsert(any(UpsertReq.class)))
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
        order.verify(client, times(3)).upsert(any(UpsertReq.class));
        order.verify(client).close(10);
        verify(client, times(1)).close(anyLong());
    }

    @Test
    void closeAttemptsOtherPartitionsAndClosesClientWhenOneFlushFails() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        AtomicBoolean failed = new AtomicBoolean();
        when(client.upsert(any(UpsertReq.class)))
                .thenAnswer(
                        invocation -> {
                            UpsertReq request = invocation.getArgument(0);
                            if (request.getData().get(0).get("id").getAsLong() == 2L) {
                                failed.set(true);
                                throw new IllegalStateException("injected partition flush failure");
                            }
                            return null;
                        });
        MilvusSinkWriter writer = newWriter(client);
        writer.write(row(1L, "a"));
        writer.write(row(2L, "b"));
        writer.write(row(3L, "a"));

        IOException error = assertThrows(IOException.class, writer::close);

        assertTrue(failed.get());
        assertTrue(error.getCause().getMessage().contains("upsert CDC data failed"));
        verify(client, times(3)).upsert(any(UpsertReq.class));
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
        when(client.upsert(any(UpsertReq.class)))
                .thenThrow(new IllegalStateException("injected flush failure"));
        doThrow(new IllegalStateException("injected client close failure"))
                .when(client)
                .close(anyLong());

        IOException error = assertThrows(IOException.class, writer::close);

        assertEquals(2, error.getSuppressed().length);
        verify(client, times(3)).upsert(any(UpsertReq.class));
        verify(client).close(10);
    }

    private static MilvusSinkWriter newWriter(MilvusClientV2 client) {
        return newWriter(client, "CDC");
    }

    private static MilvusSinkWriter newWriter(MilvusClientV2 client, String writeMode) {
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
                                .autoID(false)
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
        return new MilvusSinkWriter(
                null, table, ReadonlyConfig.fromMap(config), Collections.emptyList(), client);
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
