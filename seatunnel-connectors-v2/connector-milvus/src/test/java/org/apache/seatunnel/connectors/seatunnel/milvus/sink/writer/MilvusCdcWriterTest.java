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
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class MilvusCdcWriterTest {

    private static final String CURRENT_MESSAGE_ID = "MilvusCdcCurrentMessageId";
    private static final String MESSAGE_END = "MilvusCdcMessageEnd";

    @Test
    void metadataFreeRowsFlushAtConfiguredBatchSize() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client, 2);

        writer.write(rowWithoutMilvusMetadata(1L, "alice", RowKind.INSERT));
        verify(client, never()).upsert(any(UpsertReq.class));

        writer.write(rowWithoutMilvusMetadata(2L, "bob", RowKind.INSERT));

        ArgumentCaptor<UpsertReq> captor = ArgumentCaptor.forClass(UpsertReq.class);
        verify(client).upsert(captor.capture());
        assertEquals(2, captor.getValue().getData().size());

        writer.write(rowWithoutMilvusMetadata(3L, "carol", RowKind.INSERT));
        verify(client, times(1)).upsert(any(UpsertReq.class));
    }

    @Test
    void metadataFreeRowKindChangePreservesOperationOrder() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client, 10);

        writer.write(rowWithoutMilvusMetadata(1L, "alice", RowKind.INSERT));
        writer.write(rowWithoutMilvusMetadata(1L, "alice", RowKind.DELETE));

        verify(client).upsert(any(UpsertReq.class));
        verify(client, never()).delete(any(DeleteReq.class));

        writer.commit(true);

        org.mockito.InOrder order = inOrder(client);
        order.verify(client).upsert(any(UpsertReq.class));
        order.verify(client).delete(any(DeleteReq.class));
    }

    @Test
    void commitFlushesPendingInsertRows() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client);

        writer.write(rowWithoutMilvusMetadata(1L, "alice", RowKind.INSERT));
        writer.write(rowWithoutMilvusMetadata(2L, "bob", RowKind.INSERT));
        verify(client, never()).upsert(any(UpsertReq.class));

        writer.commit(true);

        ArgumentCaptor<UpsertReq> captor = ArgumentCaptor.forClass(UpsertReq.class);
        verify(client).upsert(captor.capture());
        assertEquals(2, captor.getValue().getData().size());
        verify(client, never()).delete(any(DeleteReq.class));
    }

    @Test
    void commitFlushesPendingDeleteRows() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client);
        SeaTunnelRow first = rowWithoutMilvusMetadata(1L, "alice", RowKind.DELETE);
        SeaTunnelRow second = rowWithoutMilvusMetadata(2L, "bob", RowKind.DELETE);

        writer.write(first);
        writer.write(second);
        verify(client, never()).delete(any(DeleteReq.class));

        writer.commit(true);

        ArgumentCaptor<DeleteReq> captor = ArgumentCaptor.forClass(DeleteReq.class);
        verify(client).delete(captor.capture());
        assertEquals(Arrays.asList(1L, 2L), captor.getValue().getIds());
        verify(client, never()).upsert(any(UpsertReq.class));
    }

    @Test
    void milvusReaderMetadataDoesNotAffectBatching() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client, 10);

        writer.write(cdcRow(1L, "alice", "message-1", false));
        writer.write(cdcRow(2L, "bob", "message-2", true));
        verify(client, never()).upsert(any(UpsertReq.class));

        writer.commit(true);

        ArgumentCaptor<UpsertReq> captor = ArgumentCaptor.forClass(UpsertReq.class);
        verify(client, times(1)).upsert(captor.capture());
        assertEquals(2, captor.getValue().getData().size());
    }

    @Test
    void writeTriggeredIntervalFlushesPendingRowsIncludingCurrentRow() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        AtomicLong currentTimeMillis = new AtomicLong(0L);
        MilvusCdcWriter writer = newWriter(client, 10, 1000L, currentTimeMillis);

        writer.write(rowWithoutMilvusMetadata(1L, "alice", RowKind.INSERT));
        currentTimeMillis.set(999L);
        writer.write(rowWithoutMilvusMetadata(2L, "bob", RowKind.INSERT));
        verify(client, never()).upsert(any(UpsertReq.class));

        currentTimeMillis.set(1000L);
        writer.write(rowWithoutMilvusMetadata(3L, "carol", RowKind.INSERT));

        ArgumentCaptor<UpsertReq> captor = ArgumentCaptor.forClass(UpsertReq.class);
        verify(client).upsert(captor.capture());
        assertEquals(3, captor.getValue().getData().size());
    }

    @Test
    void successfulIntervalFlushResetsTheNextInterval() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        AtomicLong currentTimeMillis = new AtomicLong(0L);
        MilvusCdcWriter writer = newWriter(client, 10, 1000L, currentTimeMillis);

        currentTimeMillis.set(1000L);
        writer.write(rowWithoutMilvusMetadata(1L, "alice", RowKind.INSERT));
        currentTimeMillis.set(1999L);
        writer.write(rowWithoutMilvusMetadata(2L, "bob", RowKind.INSERT));
        verify(client, times(1)).upsert(any(UpsertReq.class));

        currentTimeMillis.set(2000L);
        writer.write(rowWithoutMilvusMetadata(3L, "carol", RowKind.INSERT));

        verify(client, times(2)).upsert(any(UpsertReq.class));
    }

    @Test
    void systemTimeRollbackFlushesPendingRows() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        AtomicLong currentTimeMillis = new AtomicLong(1000L);
        MilvusCdcWriter writer = newWriter(client, 10, 1000L, currentTimeMillis);

        writer.write(rowWithoutMilvusMetadata(1L, "alice", RowKind.INSERT));
        currentTimeMillis.set(900L);
        writer.write(rowWithoutMilvusMetadata(2L, "bob", RowKind.INSERT));

        ArgumentCaptor<UpsertReq> captor = ArgumentCaptor.forClass(UpsertReq.class);
        verify(client).upsert(captor.capture());
        assertEquals(2, captor.getValue().getData().size());

        writer.write(rowWithoutMilvusMetadata(3L, "carol", RowKind.INSERT));
        verify(client, times(1)).upsert(any(UpsertReq.class));
    }

    @Test
    void writeDeleteRowUsesMappedPrimaryKeyField() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        Map<String, Object> fieldSchema = new HashMap<>();
        fieldSchema.put("source_field_name", "source_id");
        fieldSchema.put("field_name", "id");
        Map<String, Object> config = new HashMap<>();
        config.put("field_schema", Collections.singletonList(fieldSchema));
        MilvusCdcWriter writer =
                new MilvusCdcWriter(
                        renamedPrimaryKeyCatalogTable(),
                        ReadonlyConfig.fromMap(config),
                        client,
                        describeCollectionResp(),
                        "_default");
        SeaTunnelRow row = rowWithoutMilvusMetadata(7L, "alice", RowKind.DELETE);

        writer.write(row);
        writer.commit(true);

        ArgumentCaptor<DeleteReq> captor = ArgumentCaptor.forClass(DeleteReq.class);
        verify(client).delete(captor.capture());
        assertEquals(Collections.singletonList(7L), captor.getValue().getIds());
    }

    @Test
    void updateBeforeAndUpdateAfterAreAppliedAsSingleUpsert() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client);

        writer.write(rowWithoutMilvusMetadata(1L, "before", RowKind.UPDATE_BEFORE));
        writer.write(rowWithoutMilvusMetadata(1L, "after", RowKind.UPDATE_AFTER));
        writer.commit(true);

        ArgumentCaptor<UpsertReq> captor = ArgumentCaptor.forClass(UpsertReq.class);
        verify(client, never()).delete(any(DeleteReq.class));
        verify(client).upsert(captor.capture());
        assertEquals(1, captor.getValue().getData().size());
        assertEquals("after", captor.getValue().getData().get(0).get("name").getAsString());
    }

    @Test
    void updateBeforeDoesNotCreatePendingMutation() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client);

        writer.write(rowWithoutMilvusMetadata(1L, "before", RowKind.UPDATE_BEFORE));

        assertEquals(0, pendingRowCount(writer));
        assertFalse(writer.needCommit());
        writer.commit(true);
        verify(client, never()).delete(any(DeleteReq.class));
        verify(client, never()).upsert(any(UpsertReq.class));
    }

    @Test
    void nullRowKindIsRejected() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client);
        SeaTunnelRow row = rowWithoutMilvusMetadata(1L, "alice", null);

        MilvusConnectorException exception =
                assertThrows(MilvusConnectorException.class, () -> writer.write(row));

        assertTrue(exception.getMessage().contains("CDC row kind must not be null"));
        assertFalse(writer.needCommit());
        verify(client, never()).delete(any(DeleteReq.class));
        verify(client, never()).upsert(any(UpsertReq.class));
    }

    @Test
    void primaryKeyChangeAsDeleteThenInsertRemovesOldKeyAndUpsertsNewKey() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client);

        writer.write(rowWithoutMilvusMetadata(1L, "before", RowKind.DELETE));
        writer.write(rowWithoutMilvusMetadata(2L, "after", RowKind.INSERT));

        ArgumentCaptor<DeleteReq> deleteCaptor = ArgumentCaptor.forClass(DeleteReq.class);
        ArgumentCaptor<UpsertReq> upsertCaptor = ArgumentCaptor.forClass(UpsertReq.class);
        org.mockito.InOrder order = inOrder(client);
        order.verify(client).delete(deleteCaptor.capture());
        order.verify(client).upsert(upsertCaptor.capture());
        assertEquals(Collections.singletonList(1L), deleteCaptor.getValue().getIds());
        assertEquals(2L, upsertCaptor.getValue().getData().get(0).get("id").getAsLong());
        assertFalse(writer.needCommit());
    }

    @Test
    void deleteFlushedByIntervalStillFlushesFollowingInsertImmediately() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        AtomicLong currentTimeMillis = new AtomicLong(0L);
        MilvusCdcWriter writer = newWriter(client, 10, 1000L, currentTimeMillis);

        currentTimeMillis.set(1000L);
        writer.write(rowWithoutMilvusMetadata(1L, "before", RowKind.DELETE));
        verify(client).delete(any(DeleteReq.class));
        assertFalse(writer.needCommit());

        currentTimeMillis.set(1001L);
        writer.write(rowWithoutMilvusMetadata(2L, "after", RowKind.INSERT));
        verify(client).upsert(any(UpsertReq.class));
        assertFalse(writer.needCommit());
    }

    @Test
    void insertAndUpdateAfterShareTheSameUpsertBatch() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client, 10);

        writer.write(rowWithoutMilvusMetadata(1L, "insert", RowKind.INSERT));
        writer.write(rowWithoutMilvusMetadata(2L, "update", RowKind.UPDATE_AFTER));
        verify(client, never()).upsert(any(UpsertReq.class));

        writer.commit(true);

        ArgumentCaptor<UpsertReq> captor = ArgumentCaptor.forClass(UpsertReq.class);
        verify(client).upsert(captor.capture());
        assertEquals(2, captor.getValue().getData().size());
    }

    @Test
    void duplicatePrimaryKeysAreDeduplicatedWhilePending() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client, 10);

        writer.write(rowWithoutMilvusMetadata(1L, "first", RowKind.INSERT));
        writer.write(rowWithoutMilvusMetadata(1L, "last", RowKind.UPDATE_AFTER));

        assertEquals(1, pendingRowCount(writer));
        verify(client, never()).upsert(any(UpsertReq.class));
    }

    @Test
    void duplicatePrimaryKeysInUpsertBatchKeepTheLastRow() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client, 2);

        writer.write(rowWithoutMilvusMetadata(1L, "first", RowKind.INSERT));
        writer.write(rowWithoutMilvusMetadata(1L, "last", RowKind.UPDATE_AFTER));

        ArgumentCaptor<UpsertReq> captor = ArgumentCaptor.forClass(UpsertReq.class);
        verify(client).upsert(captor.capture());
        assertEquals(1, captor.getValue().getData().size());
        assertEquals("last", captor.getValue().getData().get(0).get("name").getAsString());
    }

    private static int pendingRowCount(MilvusCdcWriter writer) throws Exception {
        Field pendingRowsField = MilvusCdcWriter.class.getDeclaredField("pendingRows");
        pendingRowsField.setAccessible(true);
        Object pendingRows = pendingRowsField.get(writer);
        if (pendingRows instanceof Map) {
            return ((Map<?, ?>) pendingRows).size();
        }
        return ((List<?>) pendingRows).size();
    }

    private static MilvusCdcWriter newWriter(MilvusClientV2 client) {
        return newWriter(client, 1000);
    }

    private static MilvusCdcWriter newWriter(MilvusClientV2 client, int batchSize) {
        return newWriter(client, batchSize, 1000L, new AtomicLong(0L));
    }

    private static MilvusCdcWriter newWriter(
            MilvusClientV2 client,
            int batchSize,
            long cdcBatchFlushIntervalMs,
            AtomicLong currentTimeMillis) {
        Map<String, Object> config = new HashMap<>();
        config.put("batch_size", batchSize);
        config.put("cdc_batch_flush_interval_ms", cdcBatchFlushIntervalMs);
        return new MilvusCdcWriter(
                catalogTable(),
                ReadonlyConfig.fromMap(config),
                client,
                describeCollectionResp(),
                "_default",
                currentTimeMillis::get);
    }

    private static SeaTunnelRow rowWithoutMilvusMetadata(
            long id, String name, RowKind rowKind) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name});
        row.setRowKind(rowKind);
        return row;
    }

    private static SeaTunnelRow cdcRow(
            long id, String name, String currentMessageId, boolean messageEnd) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, name});
        row.getOptions().put(CURRENT_MESSAGE_ID, currentMessageId);
        if (messageEnd) {
            row.getOptions().put(MESSAGE_END, true);
        }
        return row;
    }

    private static CatalogTable catalogTable() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.builder().name("id").dataType(BasicType.LONG_TYPE).build(),
                        PhysicalColumn.builder()
                                .name("name")
                                .dataType(BasicType.STRING_TYPE)
                                .build());

        return CatalogTable.of(
                TableIdentifier.of("test", TablePath.of("default", "test_collection")),
                TableSchema.builder().columns(columns).build(),
                new HashMap<>(),
                Collections.emptyList(),
                "");
    }

    private static CatalogTable renamedPrimaryKeyCatalogTable() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.builder()
                                .name("source_id")
                                .dataType(BasicType.LONG_TYPE)
                                .build(),
                        PhysicalColumn.builder()
                                .name("name")
                                .dataType(BasicType.STRING_TYPE)
                                .build());

        return CatalogTable.of(
                TableIdentifier.of("test", TablePath.of("default", "test_collection")),
                TableSchema.builder().columns(columns).build(),
                new HashMap<>(),
                Collections.emptyList(),
                "");
    }

    private static DescribeCollectionResp describeCollectionResp() {
        CreateCollectionReq.FieldSchema idField =
                CreateCollectionReq.FieldSchema.builder()
                        .name("id")
                        .dataType(DataType.Int64)
                        .isPrimaryKey(true)
                        .build();
        CreateCollectionReq.FieldSchema nameField =
                CreateCollectionReq.FieldSchema.builder()
                        .name("name")
                        .dataType(DataType.VarChar)
                        .maxLength(128)
                        .build();
        CreateCollectionReq.CollectionSchema schema =
                CreateCollectionReq.CollectionSchema.builder()
                        .enableDynamicField(true)
                        .fieldSchemaList(Arrays.asList(idField, nameField))
                        .functionList(new ArrayList<>())
                        .build();
        return DescribeCollectionResp.builder()
                .collectionName("test_collection")
                .primaryFieldName("id")
                .enableDynamicField(true)
                .autoID(false)
                .collectionSchema(schema)
                .build();
    }
}
