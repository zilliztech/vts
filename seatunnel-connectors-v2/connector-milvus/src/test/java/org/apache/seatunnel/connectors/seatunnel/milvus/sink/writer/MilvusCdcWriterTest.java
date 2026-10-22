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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.shaded.io.grpc.Channel;
import io.milvus.shaded.io.grpc.Status;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.client.RetryConfig;
import io.milvus.v2.common.DataType;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import io.milvus.v2.service.vector.VectorService;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.response.DeleteResp;
import io.milvus.v2.service.vector.response.InsertResp;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.seatunnel.api.table.type.CommonOptions.EVENT_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.calls;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MilvusCdcWriterTest {

    private static final String CURRENT_MESSAGE_ID = "MilvusCdcCurrentMessageId";
    private static final String MESSAGE_END = "MilvusCdcMessageEnd";

    @Test
    void autoIdUpsertRetriesWholeOperationWithRealSdk() throws Exception {
        MilvusClientV2 client = new MilvusClientV2(null);
        client.setBlockingStub(MilvusServiceGrpc.newBlockingStub(mock(Channel.class)));
        VectorService vectors = mock(VectorService.class);
        Field field = MilvusClientV2.class.getDeclaredField("vectorService");
        field.setAccessible(true);
        field.set(client, vectors);
        List<Long> storedIds = new ArrayList<>(Collections.singletonList(42L));
        List<String> operations = new ArrayList<>();
        AtomicLong inserts = new AtomicLong();
        when(vectors.delete(any(), any(DeleteReq.class)))
                .thenAnswer(
                        call -> {
                            operations.add("delete");
                            storedIds.removeAll(call.<DeleteReq>getArgument(1).getIds());
                            return DeleteResp.builder().build();
                        });
        when(vectors.insert(any(), any(InsertReq.class)))
                .thenAnswer(
                        call -> {
                            operations.add("insert");
                            call.<InsertReq>getArgument(1)
                                    .getData()
                                    .forEach(row -> storedIds.add(row.get("id").getAsLong()));
                            if (inserts.incrementAndGet() == 1) {
                                throw Status.UNAVAILABLE
                                        .withDescription("response lost after apply")
                                        .asRuntimeException();
                            }
                            return InsertResp.builder().InsertCnt(1L).build();
                        });
        MilvusCdcWriter writer = strategyWriter(client, autoIdDescription(), true, null, 1000);
        writer.write(rowWithoutMilvusMetadata(42L, "updated", RowKind.INSERT));
        writer.commit(false);

        assertEquals(Arrays.asList("delete", "insert", "delete", "insert"), operations);
        assertEquals(Collections.singletonList(42L), storedIds);
        assertFalse(writer.needCommit());
    }

    @Test
    void autoIdUpsertConvertsWholeBatchBeforeDeleting() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        DescribeCollectionResp description = autoIdDescription();
        description.getCollectionSchema().getFieldSchemaList().get(1).setDataType(DataType.Int64);
        MilvusCdcWriter writer = strategyWriter(client, description, true, null, 1000);
        writer.write(rowWithoutMilvusMetadata(42L, "123", RowKind.INSERT));
        writer.write(rowWithoutMilvusMetadata(43L, "not-a-number", RowKind.INSERT));

        assertThrows(NumberFormatException.class, () -> writer.commit(false));
        assertThrows(NumberFormatException.class, () -> writer.commit(false));
        verify(client, never()).delete(any(DeleteReq.class));
        verify(client, never()).insert(any(InsertReq.class));
        assertEquals(2L, writer.getWriteCache());
        assertTrue(writer.needCommit());
    }

    @Test
    void exhaustedAutoIdUpsertRetainsBatchAndRetriesFromDeleteOnNextCommit() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        when(client.insert(any(InsertReq.class)))
                .thenThrow(
                        new MilvusClientException(
                                ErrorCode.RPC_ERROR, Status.UNAVAILABLE.asRuntimeException()));
        MilvusCdcWriter writer =
                new MilvusCdcWriter(
                        catalogTable(),
                        ReadonlyConfig.fromMap(Collections.singletonMap("enable_upsert", true)),
                        client,
                        autoIdDescription(),
                        () -> 0L);
        RetryConfig policy = retryConfig(writer);
        policy.setMaxRetryTimes(2);
        policy.setInitialBackOffMs(0);
        policy.setMaxBackOffMs(0);
        writer.write(rowWithoutMilvusMetadata(42L, "pending", RowKind.INSERT));

        assertThrows(MilvusConnectorException.class, () -> writer.commit(false));
        assertEquals(1L, writer.getWriteCache());
        assertTrue(writer.needCommit());
        verify(client, times(2)).delete(any(DeleteReq.class));
        verify(client, times(2)).insert(any(InsertReq.class));
        verify(client).retryConfig(policy);

        org.mockito.Mockito.doReturn(null).when(client).insert(any(InsertReq.class));
        writer.commit(false);
        verify(client, times(3)).delete(any(DeleteReq.class));
        verify(client, times(3)).insert(any(InsertReq.class));
        verify(client, times(2)).retryConfig(policy);
        assertFalse(writer.needCommit());
    }

    @ParameterizedTest
    @EnumSource(
            value = RowKind.class,
            names = {"DELETE", "UPDATE_BEFORE"})
    void deleteFirstSkipsMissingPartitionAndFollowingInsertCreatesIt(RowKind rowKind) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        Set<String> partitions = new HashSet<>(Collections.singleton("_default"));
        when(client.hasPartition(any(HasPartitionReq.class)))
                .thenAnswer(
                        call ->
                                partitions.contains(
                                        call.<HasPartitionReq>getArgument(0).getPartitionName()));
        doAnswer(
                        call -> {
                            partitions.add(
                                    call.<CreatePartitionReq>getArgument(0).getPartitionName());
                            return null;
                        })
                .when(client)
                .createPartition(any(CreatePartitionReq.class));
        when(client.delete(any(DeleteReq.class)))
                .thenAnswer(
                        call -> {
                            if (!partitions.contains(
                                    call.<DeleteReq>getArgument(0).getPartitionName())) {
                                throw missingPartitionError();
                            }
                            return null;
                        });
        MilvusCdcWriter writer = newWriter(client, 1);
        SeaTunnelRow delete = rowWithoutMilvusMetadata(42L, null, rowKind);
        delete.setPartitionName("new-partition");
        writer.write(delete);
        assertFalse(writer.needCommit());
        verify(client).delete(any(DeleteReq.class));
        verify(client, never()).createPartition(any(CreatePartitionReq.class));

        SeaTunnelRow insert = rowWithoutMilvusMetadata(42L, "alice", RowKind.INSERT);
        insert.setPartitionName("new-partition");
        writer.write(insert);
        writer.write(delete);
        org.mockito.InOrder order = inOrder(client);
        order.verify(client).createPartition(any(CreatePartitionReq.class));
        order.verify(client).insert(any(InsertReq.class));
        ArgumentCaptor<DeleteReq> deletes = ArgumentCaptor.forClass(DeleteReq.class);
        order.verify(client).delete(deletes.capture());
        assertEquals("new_partition", deletes.getValue().getPartitionName());
        assertFalse(writer.needCommit());
    }

    @Test
    void partitionLookupFailureRetainsPendingDeleteForRetry() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        when(client.hasPartition(any(HasPartitionReq.class)))
                .thenThrow(new IllegalStateException("lookup unavailable"))
                .thenReturn(false);
        when(client.delete(any(DeleteReq.class))).thenThrow(missingPartitionError());
        MilvusCdcWriter writer = newWriter(client, 1);
        SeaTunnelRow delete = rowWithoutMilvusMetadata(42L, null, RowKind.DELETE);
        delete.setPartitionName("missing");

        assertThrows(MilvusConnectorException.class, () -> writer.write(delete));
        assertTrue(writer.needCommit());
        assertEquals(1L, writer.getWriteCache());
        writer.commit(false);
        assertFalse(writer.needCommit());
        verify(client, times(2)).delete(any(DeleteReq.class));
    }

    @Test
    void existingPartitionDeleteFailureRetainsPendingBatchForRetry() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        when(client.delete(any(DeleteReq.class)))
                .thenThrow(new IllegalStateException("delete unavailable"))
                .thenReturn(null);
        MilvusCdcWriter writer = newWriter(client, 1);
        SeaTunnelRow delete = rowWithoutMilvusMetadata(42L, null, RowKind.DELETE);
        delete.setPartitionName("existing");

        assertThrows(MilvusConnectorException.class, () -> writer.write(delete));
        assertTrue(writer.needCommit());
        writer.commit(false);
        assertFalse(writer.needCommit());
        verify(client, times(2)).delete(any(DeleteReq.class));
        verify(client, never()).hasPartition(any(HasPartitionReq.class));
    }

    @ParameterizedTest
    @EnumSource(
            value = RowKind.class,
            names = {"INSERT", "UPDATE_AFTER"})
    void upsertWindowIncludesBoundaryAndReevaluatesEveryRow(RowKind kind) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = windowWriter(client, describeCollectionResp(), 100L, 1000);
        writer.write(timedRow(1L, "before", kind, 99L));
        writer.write(timedRow(2L, "boundary", kind, 100L));
        writer.write(timedRow(3L, "after", kind, 101L));
        writer.write(timedRow(4L, "late-channel", kind, 98L));
        writer.commit(false);

        org.mockito.InOrder order = inOrder(client);
        ArgumentCaptor<UpsertReq> upserts = ArgumentCaptor.forClass(UpsertReq.class);
        ArgumentCaptor<InsertReq> inserts = ArgumentCaptor.forClass(InsertReq.class);
        order.verify(client).upsert(upserts.capture());
        order.verify(client).insert(inserts.capture());
        order.verify(client).upsert(upserts.capture());
        assertEquals(2, upserts.getAllValues().get(0).getData().size());
        assertEquals(
                "boundary",
                upserts.getAllValues().get(0).getData().get(1).get("name").getAsString());
        assertEquals(3L, inserts.getValue().getData().get(0).get("id").getAsLong());
        assertEquals("late-channel", upserts.getValue().getData().get(0).get("name").getAsString());
        assertEquals("test_collection", upserts.getValue().getCollectionName());
        assertEquals("_default", upserts.getValue().getPartitionName());
        order.verifyNoMoreInteractions();
    }

    @ParameterizedTest
    @EnumSource(
            value = RowKind.class,
            names = {"INSERT", "UPDATE_AFTER"})
    void autoIdWindowUsesDeleteInsertOnlyAtOrBeforeCutoff(RowKind kind) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = windowWriter(client, autoIdDescription(), 100L, 1000);
        writer.write(timedRow(1L, "before", kind, 99L));
        writer.write(timedRow(2L, "boundary", kind, 100L));
        writer.write(timedRow(3L, "outside", kind, 101L));
        writer.write(timedRow(4L, "late-channel", kind, 98L));
        writer.commit(false);

        org.mockito.InOrder order = inOrder(client);
        ArgumentCaptor<DeleteReq> deletes = ArgumentCaptor.forClass(DeleteReq.class);
        ArgumentCaptor<InsertReq> inserts = ArgumentCaptor.forClass(InsertReq.class);
        order.verify(client).retryConfig(any(RetryConfig.class));
        order.verify(client).delete(deletes.capture());
        order.verify(client, calls(1)).insert(inserts.capture());
        order.verify(client).retryConfig(any(RetryConfig.class));
        order.verify(client, calls(1)).insert(inserts.capture());
        order.verify(client).retryConfig(any(RetryConfig.class));
        order.verify(client).delete(deletes.capture());
        order.verify(client, calls(1)).insert(inserts.capture());
        order.verify(client).retryConfig(any(RetryConfig.class));
        assertEquals(Arrays.asList(1L, 2L), deletes.getAllValues().get(0).getIds());
        assertEquals(Collections.singletonList(4L), deletes.getValue().getIds());
        assertEquals(2, inserts.getAllValues().get(0).getData().size());
        assertEquals(3L, inserts.getAllValues().get(1).getData().get(0).get("id").getAsLong());
        assertEquals(4L, inserts.getValue().getData().get(0).get("id").getAsLong());
        order.verifyNoMoreInteractions();
        assertFalse(writer.needCommit());
    }

    @ParameterizedTest
    @EnumSource(
            value = RowKind.class,
            names = {"DELETE", "UPDATE_BEFORE"})
    void windowPreservesDeleteThenUpsertAndPrimaryKeyChanges(RowKind kind) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = windowWriter(client, describeCollectionResp(), 100L, 1000);
        writer.write(rowWithoutMilvusMetadata(1L, null, kind));
        writer.write(timedRow(2L, "new-key", RowKind.UPDATE_AFTER, 99L));
        assertFalse(writer.needCommit());
        org.mockito.InOrder order = inOrder(client);
        ArgumentCaptor<DeleteReq> deletes = ArgumentCaptor.forClass(DeleteReq.class);
        ArgumentCaptor<UpsertReq> upserts = ArgumentCaptor.forClass(UpsertReq.class);
        order.verify(client).delete(deletes.capture());
        order.verify(client).upsert(upserts.capture());
        assertEquals(Collections.singletonList(1L), deletes.getValue().getIds());
        assertEquals(2L, upserts.getValue().getData().get(0).get("id").getAsLong());
    }

    @Test
    void windowKeepsRepeatedPrimaryKeysInSeparateOrderedRequests() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = windowWriter(client, describeCollectionResp(), 100L, 1000);
        writer.write(timedRow(1L, "first", RowKind.INSERT, 98L));
        writer.write(timedRow(1L, "second", RowKind.INSERT, 99L));
        writer.write(timedRow(1L, "outside", RowKind.INSERT, 101L));
        writer.commit(false);
        org.mockito.InOrder order = inOrder(client);
        ArgumentCaptor<UpsertReq> upserts = ArgumentCaptor.forClass(UpsertReq.class);
        order.verify(client, times(2)).upsert(upserts.capture());
        order.verify(client).insert(any(InsertReq.class));
        assertEquals(
                "first", upserts.getAllValues().get(0).getData().get(0).get("name").getAsString());
        assertEquals(
                "second", upserts.getAllValues().get(1).getData().get(0).get("name").getAsString());
    }

    @Test
    void failedWindowFlushRetainsOperationAndRowsBeforeBoundarySwitch() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        when(client.upsert(any(UpsertReq.class)))
                .thenThrow(new IllegalStateException("unknown outcome"))
                .thenReturn(null);
        MilvusCdcWriter writer = windowWriter(client, describeCollectionResp(), 100L, 1000);
        writer.write(timedRow(1L, "inside", RowKind.INSERT, 99L));
        SeaTunnelRow after = timedRow(2L, "outside", RowKind.INSERT, 101L);
        assertThrows(MilvusConnectorException.class, () -> writer.write(after));
        assertEquals(1L, writer.getWriteCache());
        verify(client, never()).insert(any(InsertReq.class));
        writer.write(after);
        writer.commit(false);
        ArgumentCaptor<UpsertReq> upserts = ArgumentCaptor.forClass(UpsertReq.class);
        verify(client, times(2)).upsert(upserts.capture());
        assertEquals(
                upserts.getAllValues().get(0).getData(), upserts.getAllValues().get(1).getData());
        verify(client).insert(any(InsertReq.class));
        assertFalse(writer.needCommit());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void windowRejectsMissingOrNegativeEventTimeBeforeChangingPendingBatch(boolean autoId) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer =
                windowWriter(
                        client,
                        autoId ? autoIdDescription() : describeCollectionResp(),
                        100L,
                        1000);
        writer.write(timedRow(1L, "pending", RowKind.INSERT, 99L));
        assertThrows(
                MilvusConnectorException.class,
                () -> writer.write(rowWithoutMilvusMetadata(2L, "missing", RowKind.INSERT)));
        assertThrows(
                MilvusConnectorException.class,
                () -> writer.write(timedRow(2L, "invalid", RowKind.INSERT, -1L)));
        SeaTunnelRow invalid = rowWithoutMilvusMetadata(2L, "invalid-type", RowKind.INSERT);
        invalid.getOptions().put(EVENT_TIME.getName(), "99");
        MilvusConnectorException error =
                assertThrows(MilvusConnectorException.class, () -> writer.write(invalid));
        assertTrue(error.getMessage().contains("EventTime"));
        assertEquals(1L, writer.getWriteCache());
        writer.commit(false);
        if (autoId) {
            ArgumentCaptor<DeleteReq> deletes = ArgumentCaptor.forClass(DeleteReq.class);
            verify(client).delete(deletes.capture());
            assertEquals(Collections.singletonList(1L), deletes.getValue().getIds());
            ArgumentCaptor<InsertReq> inserts = ArgumentCaptor.forClass(InsertReq.class);
            verify(client).insert(inserts.capture());
            assertEquals(1, inserts.getValue().getData().size());
            verify(client, never()).upsert(any(UpsertReq.class));
        } else {
            ArgumentCaptor<UpsertReq> upserts = ArgumentCaptor.forClass(UpsertReq.class);
            verify(client).upsert(upserts.capture());
            assertEquals(1, upserts.getValue().getData().size());
            verify(client, never()).insert(any(InsertReq.class));
        }
    }

    @Test
    void windowRejectsNegativeCutoff() {
        assertThrows(
                MilvusConnectorException.class,
                () -> windowWriter(mock(MilvusClientV2.class), describeCollectionResp(), -1L, 1));
    }

    @Test
    void windowEnsuresAndSeparatesExplicitPartitions() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = windowWriter(client, describeCollectionResp(), 100L, 1000);
        SeaTunnelRow first = timedRow(1L, "first", RowKind.INSERT, 99L);
        first.setPartitionName("part-a");
        SeaTunnelRow second = timedRow(2L, "second", RowKind.INSERT, 99L);
        second.setPartitionName("part-b");
        writer.write(first);
        writer.write(second);
        writer.commit(false);
        ArgumentCaptor<UpsertReq> upserts = ArgumentCaptor.forClass(UpsertReq.class);
        verify(client, times(2)).upsert(upserts.capture());
        verify(client, times(2)).createPartition(any(CreatePartitionReq.class));
        assertEquals("part_a", upserts.getAllValues().get(0).getPartitionName());
        assertEquals("part_b", upserts.getAllValues().get(1).getPartitionName());
    }

    @Test
    void recreatedWriterReusesConfiguredCutoff() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        SeaTunnelRow replay = timedRow(1L, "replay", RowKind.INSERT, 100L);
        windowWriter(client, describeCollectionResp(), 100L, 1).write(replay);
        windowWriter(client, describeCollectionResp(), 100L, 1).write(replay);
        verify(client, times(2)).upsert(any(UpsertReq.class));
        verify(client, never()).insert(any(InsertReq.class));
    }

    @Test
    void windowUpsertsPartitionKeyRowsWithoutPhysicalPartitionName() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        DescribeCollectionResp description = describeCollectionResp();
        description.getCollectionSchema().getFieldSchemaList().get(1).setIsPartitionKey(true);
        MilvusCdcWriter writer = windowWriter(client, description, 100L, 1000);
        SeaTunnelRow first = timedRow(1L, "tenant_a", RowKind.INSERT, 99L);
        first.setPartitionName("physical_a");
        SeaTunnelRow second = timedRow(2L, "tenant_b", RowKind.INSERT, 100L);
        second.setPartitionName("physical_b");
        writer.write(first);
        writer.write(second);
        writer.commit(false);
        ArgumentCaptor<UpsertReq> upserts = ArgumentCaptor.forClass(UpsertReq.class);
        verify(client).upsert(upserts.capture());
        assertEquals("", upserts.getValue().getPartitionName());
        assertEquals(2, upserts.getValue().getData().size());
        verify(client, never()).hasPartition(any(HasPartitionReq.class));
        verify(client, never()).createPartition(any(CreatePartitionReq.class));
    }

    @Test
    void epochCutoffIsInclusiveAndCloseFlushesRemainingInsert() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = windowWriter(client, describeCollectionResp(), 0L, 1000);
        writer.write(timedRow(1L, "epoch", RowKind.INSERT, 0L));
        writer.write(timedRow(2L, "after", RowKind.INSERT, 1L));
        writer.close();
        org.mockito.InOrder order = inOrder(client);
        order.verify(client).upsert(any(UpsertReq.class));
        order.verify(client).insert(any(InsertReq.class));
        assertFalse(writer.needCommit());
    }

    @ParameterizedTest
    @EnumSource(
            value = RowKind.class,
            names = {"INSERT", "UPDATE_AFTER"})
    void enabledUpsertDoesNotRequireCutoffOrEventTime(RowKind kind) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = strategyWriter(client, describeCollectionResp(), true, null, 1000);
        writer.write(rowWithoutMilvusMetadata(42L, "always-upsert", kind));
        writer.commit(false);
        ArgumentCaptor<UpsertReq> upserts = ArgumentCaptor.forClass(UpsertReq.class);
        verify(client).upsert(upserts.capture());
        assertEquals(42L, upserts.getValue().getData().get(0).get("id").getAsLong());
        assertEquals(
                "always-upsert", upserts.getValue().getData().get(0).get("name").getAsString());
        verify(client, never()).insert(any(InsertReq.class));
    }

    @ParameterizedTest
    @EnumSource(
            value = RowKind.class,
            names = {"INSERT", "UPDATE_AFTER"})
    void enabledUpsertOverridesWindowWithoutValidatingEventTime(RowKind kind) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = strategyWriter(client, describeCollectionResp(), true, 100L, 1000);
        writer.write(timedRow(1L, "inside", kind, 99L));
        writer.write(timedRow(2L, "outside", kind, 101L));
        writer.write(rowWithoutMilvusMetadata(3L, "missing-time", kind));
        writer.write(timedRow(4L, "invalid-time", kind, -1L));
        writer.commit(false);
        ArgumentCaptor<UpsertReq> upserts = ArgumentCaptor.forClass(UpsertReq.class);
        verify(client).upsert(upserts.capture());
        assertEquals(4, upserts.getValue().getData().size());
        verify(client, never()).insert(any(InsertReq.class));
    }

    @ParameterizedTest
    @EnumSource(
            value = RowKind.class,
            names = {"INSERT", "UPDATE_AFTER"})
    void disabledUpsertWithoutCutoffInsertsWithoutEventTime(RowKind kind) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = strategyWriter(client, describeCollectionResp(), false, null, 1);
        writer.write(rowWithoutMilvusMetadata(42L, "insert", kind));
        verify(client).insert(any(InsertReq.class));
        verify(client, never()).upsert(any(UpsertReq.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void enabledUpsertStillPreservesAutoIdPrimaryKeys(boolean withCutoff) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer =
                strategyWriter(client, autoIdDescription(), true, withCutoff ? 100L : null, 1000);
        writer.write(rowWithoutMilvusMetadata(42L, "insert", RowKind.INSERT));
        writer.write(rowWithoutMilvusMetadata(43L, "after", RowKind.UPDATE_AFTER));
        writer.write(timedRow(44L, "outside", RowKind.INSERT, 101L));
        writer.write(timedRow(45L, "invalid-time", RowKind.UPDATE_AFTER, -1L));
        writer.commit(false);
        ArgumentCaptor<InsertReq> inserts = ArgumentCaptor.forClass(InsertReq.class);
        ArgumentCaptor<DeleteReq> deletes = ArgumentCaptor.forClass(DeleteReq.class);
        org.mockito.InOrder order = inOrder(client);
        order.verify(client).delete(deletes.capture());
        order.verify(client).insert(inserts.capture());
        assertEquals(Arrays.asList(42L, 43L, 44L, 45L), deletes.getValue().getIds());
        assertEquals("_default", deletes.getValue().getPartitionName());
        assertEquals("_default", inserts.getValue().getPartitionName());
        assertEquals(4, inserts.getValue().getData().size());
        assertEquals(42L, inserts.getValue().getData().get(0).get("id").getAsLong());
        assertEquals(43L, inserts.getValue().getData().get(1).get("id").getAsLong());
        verify(client, never()).upsert(any(UpsertReq.class));
    }

    @ParameterizedTest
    @EnumSource(
            value = RowKind.class,
            names = {"DELETE", "UPDATE_BEFORE"})
    void enabledUpsertPreservesDeleteBeforeWriteAndPrimaryKeyChanges(RowKind kind) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = strategyWriter(client, describeCollectionResp(), true, null, 1000);
        writer.write(rowWithoutMilvusMetadata(1L, null, kind));
        writer.write(rowWithoutMilvusMetadata(2L, "new-key", RowKind.UPDATE_AFTER));
        assertFalse(writer.needCommit());
        org.mockito.InOrder order = inOrder(client);
        ArgumentCaptor<DeleteReq> deletes = ArgumentCaptor.forClass(DeleteReq.class);
        ArgumentCaptor<UpsertReq> upserts = ArgumentCaptor.forClass(UpsertReq.class);
        order.verify(client).delete(deletes.capture());
        order.verify(client).upsert(upserts.capture());
        assertEquals(Collections.singletonList(1L), deletes.getValue().getIds());
        assertEquals(2L, upserts.getValue().getData().get(0).get("id").getAsLong());
        order.verifyNoMoreInteractions();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void autoIdUpsertDeleteFailureRetainsBatchWithoutInserting(boolean enableUpsert) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        when(client.delete(any(DeleteReq.class)))
                .thenThrow(new IllegalStateException("delete unavailable"))
                .thenReturn(null);
        MilvusCdcWriter writer =
                strategyWriter(client, autoIdDescription(), enableUpsert, 100L, 1000);
        writer.write(timedRow(42L, "first", RowKind.INSERT, 99L));
        writer.write(timedRow(43L, "second", RowKind.UPDATE_AFTER, 100L));

        assertThrows(MilvusConnectorException.class, () -> writer.commit(false));
        assertEquals(2L, writer.getWriteCache());
        assertTrue(writer.needCommit());
        verify(client, never()).insert(any(InsertReq.class));
        writer.commit(false);

        org.mockito.InOrder order = inOrder(client);
        ArgumentCaptor<DeleteReq> deletes = ArgumentCaptor.forClass(DeleteReq.class);
        order.verify(client).retryConfig(any(RetryConfig.class));
        order.verify(client).delete(deletes.capture());
        order.verify(client, times(2)).retryConfig(any(RetryConfig.class));
        order.verify(client).delete(deletes.capture());
        ArgumentCaptor<InsertReq> inserts = ArgumentCaptor.forClass(InsertReq.class);
        order.verify(client).insert(inserts.capture());
        order.verify(client).retryConfig(any(RetryConfig.class));
        assertEquals(Arrays.asList(42L, 43L), deletes.getAllValues().get(0).getIds());
        assertEquals(deletes.getAllValues().get(0).getIds(), deletes.getValue().getIds());
        assertEquals(2, inserts.getValue().getData().size());
        assertFalse(writer.needCommit());
        assertEquals(0L, writer.getWriteCache());
        order.verifyNoMoreInteractions();
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void autoIdUpsertRetriesDeleteInsertAfterInsertFailure(
            boolean enableUpsert, boolean appliedBeforeFailure) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        // Model visible copies to distinguish failure before apply from a lost response after
        // apply.
        List<Long> storedIds = new ArrayList<>(Arrays.asList(42L, 42L, 43L));
        when(client.delete(any(DeleteReq.class)))
                .thenAnswer(
                        call -> {
                            storedIds.removeAll(call.<DeleteReq>getArgument(0).getIds());
                            return null;
                        });
        AtomicLong attempts = new AtomicLong();
        when(client.insert(any(InsertReq.class)))
                .thenAnswer(
                        call -> {
                            boolean first = attempts.incrementAndGet() == 1;
                            if (!first || appliedBeforeFailure) {
                                call.<InsertReq>getArgument(0)
                                        .getData()
                                        .forEach(row -> storedIds.add(row.get("id").getAsLong()));
                            }
                            if (first) {
                                throw new IllegalStateException("insert response unavailable");
                            }
                            return null;
                        });
        MilvusCdcWriter writer =
                strategyWriter(client, autoIdDescription(), enableUpsert, 100L, 1000);
        writer.write(timedRow(42L, "first", RowKind.INSERT, 99L));
        writer.write(timedRow(43L, "second", RowKind.UPDATE_AFTER, 100L));
        assertThrows(MilvusConnectorException.class, () -> writer.commit(false));
        assertEquals(
                appliedBeforeFailure ? Arrays.asList(42L, 43L) : Collections.emptyList(),
                storedIds);
        assertEquals(2L, writer.getWriteCache());
        assertTrue(writer.needCommit());
        writer.commit(false);

        org.mockito.InOrder order = inOrder(client);
        ArgumentCaptor<DeleteReq> deletes = ArgumentCaptor.forClass(DeleteReq.class);
        ArgumentCaptor<InsertReq> inserts = ArgumentCaptor.forClass(InsertReq.class);
        order.verify(client).retryConfig(any(RetryConfig.class));
        order.verify(client).delete(deletes.capture());
        order.verify(client).insert(inserts.capture());
        order.verify(client, times(2)).retryConfig(any(RetryConfig.class));
        order.verify(client).delete(deletes.capture());
        order.verify(client).insert(inserts.capture());
        order.verify(client).retryConfig(any(RetryConfig.class));
        assertEquals(deletes.getAllValues().get(0).getIds(), deletes.getValue().getIds());
        assertEquals(inserts.getAllValues().get(0).getData(), inserts.getValue().getData());
        assertEquals(Arrays.asList(42L, 43L), storedIds);
        assertFalse(writer.needCommit());
        assertEquals(0L, writer.getWriteCache());
        writer.commit(false);
        order.verifyNoMoreInteractions();
    }

    @Test
    void autoIdFailedUpsertIsRetriedBeforeAcceptingOutsideWindowRow() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        when(client.insert(any(InsertReq.class)))
                .thenThrow(new IllegalStateException("timeout"))
                .thenReturn(null);
        MilvusCdcWriter writer = windowWriter(client, autoIdDescription(), 100L, 1000);
        writer.write(timedRow(42L, "inside", RowKind.INSERT, 100L));
        SeaTunnelRow outside = timedRow(43L, "outside", RowKind.INSERT, 101L);
        assertThrows(MilvusConnectorException.class, () -> writer.write(outside));
        assertEquals(1L, writer.getWriteCache());
        writer.write(outside);
        writer.commit(false);

        org.mockito.InOrder order = inOrder(client);
        ArgumentCaptor<DeleteReq> deletes = ArgumentCaptor.forClass(DeleteReq.class);
        ArgumentCaptor<InsertReq> inserts = ArgumentCaptor.forClass(InsertReq.class);
        order.verify(client).retryConfig(any(RetryConfig.class));
        order.verify(client).delete(deletes.capture());
        order.verify(client, calls(1)).insert(inserts.capture());
        order.verify(client, times(2)).retryConfig(any(RetryConfig.class));
        order.verify(client).delete(deletes.capture());
        order.verify(client, calls(1)).insert(inserts.capture());
        order.verify(client).retryConfig(any(RetryConfig.class));
        order.verify(client, calls(1)).insert(inserts.capture());
        for (DeleteReq request : deletes.getAllValues()) {
            assertEquals(Collections.singletonList(42L), request.getIds());
        }
        assertEquals(
                inserts.getAllValues().get(0).getData(), inserts.getAllValues().get(1).getData());
        assertEquals(43L, inserts.getValue().getData().get(0).get("id").getAsLong());
        assertFalse(writer.needCommit());
        order.verifyNoMoreInteractions();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void autoIdUpsertPreservesPartitionScopeAndRepeatedKeyOrder(boolean partitionKey) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        DescribeCollectionResp description = autoIdDescription();
        if (partitionKey) {
            description.getCollectionSchema().getFieldSchemaList().get(1).setIsPartitionKey(true);
        } else {
            when(client.hasPartition(any(HasPartitionReq.class))).thenReturn(true);
        }
        MilvusCdcWriter writer = strategyWriter(client, description, true, null, 1000);
        SeaTunnelRow first = rowWithoutMilvusMetadata(42L, "first", RowKind.INSERT);
        SeaTunnelRow second = rowWithoutMilvusMetadata(42L, "second", RowKind.UPDATE_AFTER);
        first.setPartitionName("part-a");
        second.setPartitionName("part-a");
        writer.write(first);
        writer.write(second);
        writer.commit(false);

        org.mockito.InOrder order = inOrder(client);
        if (!partitionKey) {
            order.verify(client).hasPartition(any(HasPartitionReq.class));
        }
        ArgumentCaptor<DeleteReq> deletes = ArgumentCaptor.forClass(DeleteReq.class);
        ArgumentCaptor<InsertReq> inserts = ArgumentCaptor.forClass(InsertReq.class);
        order.verify(client).retryConfig(any(RetryConfig.class));
        order.verify(client).delete(deletes.capture());
        order.verify(client).insert(inserts.capture());
        order.verify(client, times(2)).retryConfig(any(RetryConfig.class));
        order.verify(client).delete(deletes.capture());
        order.verify(client).insert(inserts.capture());
        order.verify(client).retryConfig(any(RetryConfig.class));
        for (DeleteReq request : deletes.getAllValues()) {
            assertEquals(Collections.singletonList(42L), request.getIds());
            assertEquals("test_collection", request.getCollectionName());
            if (partitionKey) {
                assertTrue(
                        request.getPartitionName() == null || request.getPartitionName().isEmpty());
            } else {
                assertEquals("part_a", request.getPartitionName());
            }
        }
        for (InsertReq request : inserts.getAllValues()) {
            assertEquals(42L, request.getData().get(0).get("id").getAsLong());
            assertEquals("test_collection", request.getCollectionName());
            assertEquals(partitionKey ? "" : "part_a", request.getPartitionName());
        }
        assertEquals(
                "first", inserts.getAllValues().get(0).getData().get(0).get("name").getAsString());
        assertEquals("second", inserts.getValue().getData().get(0).get("name").getAsString());
        order.verifyNoMoreInteractions();
    }

    private static MilvusCdcWriter windowWriter(
            MilvusClientV2 client, DescribeCollectionResp description, long cutoff, int batchSize) {
        return strategyWriter(client, description, false, cutoff, batchSize);
    }

    private static MilvusCdcWriter strategyWriter(
            MilvusClientV2 client,
            DescribeCollectionResp description,
            boolean enableUpsert,
            Long cutoff,
            int batchSize) {
        Map<String, Object> config = new HashMap<>();
        config.put("enable_upsert", enableUpsert);
        if (cutoff != null) {
            config.put("cdc_overlap_upsert_cutoff_ms", cutoff);
        }
        config.put("batch_size", batchSize);
        return new MilvusCdcWriter(
                catalogTable(), ReadonlyConfig.fromMap(config), client, description, () -> 0L);
    }

    private static SeaTunnelRow timedRow(long id, String name, RowKind kind, long eventTime) {
        SeaTunnelRow row = rowWithoutMilvusMetadata(id, name, kind);
        MetadataUtil.setEventTime(row, eventTime);
        return row;
    }

    private static MilvusClientException missingPartitionError() {
        return new MilvusClientException(ErrorCode.SERVER_ERROR, "partition not found", 200, 1);
    }

    @Test
    void successfulNamedPartitionDeletesDoNotCheckPartitionExistence() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client, 1);
        SeaTunnelRow delete = rowWithoutMilvusMetadata(42L, null, RowKind.DELETE);
        delete.setPartitionName("existing");
        writer.write(delete);
        writer.write(delete);

        verify(client, times(2)).delete(any(DeleteReq.class));
        verify(client, never()).hasPartition(any(HasPartitionReq.class));
    }

    @Test
    void partitionNotFoundErrorRequiresConfirmationBeforeSkippingDelete() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        when(client.delete(any(DeleteReq.class))).thenThrow(missingPartitionError());
        when(client.hasPartition(any(HasPartitionReq.class))).thenReturn(true);
        MilvusCdcWriter writer = newWriter(client, 1);
        SeaTunnelRow delete = rowWithoutMilvusMetadata(42L, null, RowKind.DELETE);
        delete.setPartitionName("existing");

        assertThrows(MilvusConnectorException.class, () -> writer.write(delete));
        assertTrue(writer.needCommit());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "_default"})
    void unscopedAndDefaultPartitionDeleteErrorsAreNotSkipped(String partitionName) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        when(client.delete(any(DeleteReq.class))).thenThrow(missingPartitionError());
        MilvusCdcWriter writer = newWriter(client, 1);
        SeaTunnelRow delete = rowWithoutMilvusMetadata(42L, null, RowKind.DELETE);
        delete.setPartitionName(partitionName);

        assertThrows(MilvusConnectorException.class, () -> writer.write(delete));
        assertTrue(writer.needCommit());
        verify(client, never()).hasPartition(any(HasPartitionReq.class));
    }

    @ParameterizedTest
    @CsvSource({"false,1", "false,2", "false,10", "true,1", "true,2", "true,10"})
    void duplicateInsertsPreserveEveryRowAcrossBatchSizes(boolean autoId, int batchSize) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = insertWriter(client, autoId, batchSize);
        writer.write(rowWithoutMilvusMetadata(1L, "first", RowKind.INSERT));
        writer.write(rowWithoutMilvusMetadata(2L, "other", RowKind.INSERT));
        writer.write(rowWithoutMilvusMetadata(1L, "last", RowKind.UPDATE_AFTER));
        writer.commit(true);

        ArgumentCaptor<InsertReq> captor = ArgumentCaptor.forClass(InsertReq.class);
        verify(client, times(batchSize == 1 ? 3 : 2)).insert(captor.capture());
        List<String> names = new ArrayList<>();
        for (InsertReq request : captor.getAllValues()) {
            Set<Long> keys = new HashSet<>();
            request.getData()
                    .forEach(
                            row -> {
                                assertTrue(keys.add(row.get("id").getAsLong()));
                                names.add(row.get("name").getAsString());
                            });
        }
        assertEquals(Arrays.asList("first", "other", "last"), names);
        verify(client, never()).upsert(any(UpsertReq.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void duplicateInsertFlushFailureRetainsOldBatchWithoutAcceptingNewRow(boolean autoId) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = insertWriter(client, autoId, 10);
        when(client.insert(any(InsertReq.class)))
                .thenThrow(new IllegalStateException("ambiguous failure"))
                .thenReturn(null);
        writer.write(rowWithoutMilvusMetadata(1L, "first", RowKind.INSERT));
        SeaTunnelRow next = rowWithoutMilvusMetadata(1L, "last", RowKind.INSERT);
        assertThrows(MilvusConnectorException.class, () -> writer.write(next));
        assertEquals(1L, writer.getWriteCache());
        assertTrue(writer.needCommit());

        writer.write(next);
        assertEquals(1L, writer.getWriteCache());
        writer.commit(true);
        ArgumentCaptor<InsertReq> captor = ArgumentCaptor.forClass(InsertReq.class);
        verify(client, times(3)).insert(captor.capture());
        assertEquals(
                "first", captor.getAllValues().get(0).getData().get(0).get("name").getAsString());
        assertEquals(
                "first", captor.getAllValues().get(1).getData().get(0).get("name").getAsString());
        assertEquals(
                "last", captor.getAllValues().get(2).getData().get(0).get("name").getAsString());
        assertFalse(writer.needCommit());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void duplicateDeletesRemainInOneBatch(boolean autoId) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = insertWriter(client, autoId, 10);
        writer.write(rowWithoutMilvusMetadata(1L, "before", RowKind.DELETE));
        writer.write(rowWithoutMilvusMetadata(2L, "before", RowKind.DELETE));
        writer.write(rowWithoutMilvusMetadata(1L, "before", RowKind.DELETE));
        verify(client, never()).delete(any(DeleteReq.class));
        writer.commit(true);
        ArgumentCaptor<DeleteReq> captor = ArgumentCaptor.forClass(DeleteReq.class);
        verify(client).delete(captor.capture());
        assertEquals(2, captor.getValue().getIds().size());
        assertTrue(captor.getValue().getIds().containsAll(Arrays.asList(1L, 2L)));
        verify(client, never()).insert(any(InsertReq.class));
    }

    private static MilvusCdcWriter insertWriter(
            MilvusClientV2 client, boolean autoId, int batchSize) {
        Map<String, Object> config = new HashMap<>();
        config.put("batch_size", batchSize);
        return new MilvusCdcWriter(
                catalogTable(),
                ReadonlyConfig.fromMap(config),
                client,
                autoId ? autoIdDescription() : describeCollectionResp(),
                () -> 0L);
    }

    @Test
    void closeFlushesPendingRowsWithoutClosingSharedClient() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client, 10);
        writer.write(rowWithoutMilvusMetadata(1L, "alice", RowKind.INSERT));

        writer.close();

        verify(client).insert(any(InsertReq.class));
        verify(client, never()).close(anyLong());
        assertFalse(writer.needCommit());
    }

    @Test
    void metadataFreeRowsFlushAtConfiguredBatchSize() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client, 2);

        writer.write(rowWithoutMilvusMetadata(1L, "alice", RowKind.INSERT));
        verify(client, never()).insert(any(InsertReq.class));

        writer.write(rowWithoutMilvusMetadata(2L, "bob", RowKind.INSERT));

        ArgumentCaptor<InsertReq> captor = ArgumentCaptor.forClass(InsertReq.class);
        verify(client).insert(captor.capture());
        assertEquals(2, captor.getValue().getData().size());

        writer.write(rowWithoutMilvusMetadata(3L, "carol", RowKind.INSERT));
        verify(client, times(1)).insert(any(InsertReq.class));
    }

    @Test
    void metadataFreeRowKindChangePreservesOperationOrder() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client, 10);

        writer.write(rowWithoutMilvusMetadata(1L, "alice", RowKind.INSERT));
        writer.write(rowWithoutMilvusMetadata(1L, "alice", RowKind.DELETE));

        verify(client).insert(any(InsertReq.class));
        verify(client, never()).delete(any(DeleteReq.class));

        writer.commit(true);

        org.mockito.InOrder order = inOrder(client);
        order.verify(client).insert(any(InsertReq.class));
        order.verify(client).delete(any(DeleteReq.class));
    }

    @Test
    void commitFlushesPendingInsertRows() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client);

        writer.write(rowWithoutMilvusMetadata(1L, "alice", RowKind.INSERT));
        writer.write(rowWithoutMilvusMetadata(2L, "bob", RowKind.INSERT));
        verify(client, never()).insert(any(InsertReq.class));

        writer.commit(true);

        ArgumentCaptor<InsertReq> captor = ArgumentCaptor.forClass(InsertReq.class);
        verify(client).insert(captor.capture());
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
        verify(client, never()).insert(any(InsertReq.class));
    }

    @Test
    void milvusReaderMetadataDoesNotAffectBatching() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client, 10);

        writer.write(cdcRow(1L, "alice", "message-1", false));
        writer.write(cdcRow(2L, "bob", "message-2", true));
        verify(client, never()).insert(any(InsertReq.class));

        writer.commit(true);

        ArgumentCaptor<InsertReq> captor = ArgumentCaptor.forClass(InsertReq.class);
        verify(client, times(1)).insert(captor.capture());
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
        verify(client, never()).insert(any(InsertReq.class));

        currentTimeMillis.set(1000L);
        writer.write(rowWithoutMilvusMetadata(3L, "carol", RowKind.INSERT));

        ArgumentCaptor<InsertReq> captor = ArgumentCaptor.forClass(InsertReq.class);
        verify(client).insert(captor.capture());
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
        verify(client, times(1)).insert(any(InsertReq.class));

        currentTimeMillis.set(2000L);
        writer.write(rowWithoutMilvusMetadata(3L, "carol", RowKind.INSERT));

        verify(client, times(2)).insert(any(InsertReq.class));
    }

    @Test
    void systemTimeRollbackFlushesPendingRows() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        AtomicLong currentTimeMillis = new AtomicLong(1000L);
        MilvusCdcWriter writer = newWriter(client, 10, 1000L, currentTimeMillis);

        writer.write(rowWithoutMilvusMetadata(1L, "alice", RowKind.INSERT));
        currentTimeMillis.set(900L);
        writer.write(rowWithoutMilvusMetadata(2L, "bob", RowKind.INSERT));

        ArgumentCaptor<InsertReq> captor = ArgumentCaptor.forClass(InsertReq.class);
        verify(client).insert(captor.capture());
        assertEquals(2, captor.getValue().getData().size());

        writer.write(rowWithoutMilvusMetadata(3L, "carol", RowKind.INSERT));
        verify(client, times(1)).insert(any(InsertReq.class));
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
                        describeCollectionResp());
        SeaTunnelRow row = rowWithoutMilvusMetadata(7L, "alice", RowKind.DELETE);

        writer.write(row);
        writer.commit(true);

        ArgumentCaptor<DeleteReq> captor = ArgumentCaptor.forClass(DeleteReq.class);
        verify(client).delete(captor.capture());
        assertEquals(Collections.singletonList(7L), captor.getValue().getIds());
    }

    @ParameterizedTest
    @ValueSource(longs = {1L, 2L})
    void updateBeforeAndUpdateAfterDeleteOldKeyThenInsertNewKey(long newPrimaryKey) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client);

        writer.write(rowWithoutMilvusMetadata(1L, "before", RowKind.UPDATE_BEFORE));
        writer.write(rowWithoutMilvusMetadata(newPrimaryKey, "after", RowKind.UPDATE_AFTER));
        writer.commit(true);

        ArgumentCaptor<InsertReq> captor = ArgumentCaptor.forClass(InsertReq.class);
        ArgumentCaptor<DeleteReq> deletes = ArgumentCaptor.forClass(DeleteReq.class);
        org.mockito.InOrder order = inOrder(client);
        order.verify(client).delete(deletes.capture());
        order.verify(client).insert(captor.capture());
        assertEquals(Collections.singletonList(1L), deletes.getValue().getIds());
        assertEquals(newPrimaryKey, captor.getValue().getData().get(0).get("id").getAsLong());
        assertEquals(1, captor.getValue().getData().size());
        assertEquals("after", captor.getValue().getData().get(0).get("name").getAsString());
    }

    @Test
    void updateBeforeCreatesPendingDelete() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client);

        writer.write(rowWithoutMilvusMetadata(1L, "before", RowKind.UPDATE_BEFORE));

        assertEquals(1, pendingRowCount(writer));
        assertTrue(writer.needCommit());
        writer.commit(true);
        ArgumentCaptor<DeleteReq> captor = ArgumentCaptor.forClass(DeleteReq.class);
        verify(client).delete(captor.capture());
        assertEquals(Collections.singletonList(1L), captor.getValue().getIds());
        verify(client, never()).insert(any(InsertReq.class));
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
        verify(client, never()).insert(any(InsertReq.class));
    }

    @Test
    void primaryKeyChangeAsDeleteThenInsertRemovesOldKeyAndInsertsNewKey() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client);

        writer.write(rowWithoutMilvusMetadata(1L, "before", RowKind.DELETE));
        writer.write(rowWithoutMilvusMetadata(2L, "after", RowKind.INSERT));

        ArgumentCaptor<DeleteReq> deleteCaptor = ArgumentCaptor.forClass(DeleteReq.class);
        ArgumentCaptor<InsertReq> insertCaptor = ArgumentCaptor.forClass(InsertReq.class);
        org.mockito.InOrder order = inOrder(client);
        order.verify(client).delete(deleteCaptor.capture());
        order.verify(client).insert(insertCaptor.capture());
        assertEquals(Collections.singletonList(1L), deleteCaptor.getValue().getIds());
        assertEquals(2L, insertCaptor.getValue().getData().get(0).get("id").getAsLong());
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
        verify(client).insert(any(InsertReq.class));
        assertFalse(writer.needCommit());
    }

    @Test
    void insertAndUpdateAfterShareTheSameInsertBatch() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client, 10);

        writer.write(rowWithoutMilvusMetadata(1L, "insert", RowKind.INSERT));
        writer.write(rowWithoutMilvusMetadata(2L, "update", RowKind.UPDATE_AFTER));
        verify(client, never()).insert(any(InsertReq.class));

        writer.commit(true);

        ArgumentCaptor<InsertReq> captor = ArgumentCaptor.forClass(InsertReq.class);
        verify(client).insert(captor.capture());
        assertEquals(2, captor.getValue().getData().size());
    }

    @Test
    void duplicateInsertFlushesOldRowAndLeavesNewRowPending() throws Exception {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client, 10);

        writer.write(rowWithoutMilvusMetadata(1L, "first", RowKind.INSERT));
        writer.write(rowWithoutMilvusMetadata(1L, "last", RowKind.UPDATE_AFTER));

        assertEquals(1, pendingRowCount(writer));
        ArgumentCaptor<InsertReq> captor = ArgumentCaptor.forClass(InsertReq.class);
        verify(client).insert(captor.capture());
        assertEquals("first", captor.getValue().getData().get(0).get("name").getAsString());
        assertTrue(writer.needCommit());
    }

    @Test
    void duplicateInsertStartsANewBatchSizeCount() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = newWriter(client, 2);

        writer.write(rowWithoutMilvusMetadata(1L, "first", RowKind.INSERT));
        writer.write(rowWithoutMilvusMetadata(1L, "last", RowKind.UPDATE_AFTER));
        assertEquals(1L, writer.getWriteCache());
        verify(client).insert(any(InsertReq.class));
        writer.write(rowWithoutMilvusMetadata(2L, "other", RowKind.INSERT));

        ArgumentCaptor<InsertReq> captor = ArgumentCaptor.forClass(InsertReq.class);
        verify(client, times(2)).insert(captor.capture());
        assertEquals(1, captor.getAllValues().get(0).getData().size());
        assertEquals(2, captor.getAllValues().get(1).getData().size());
        assertEquals(
                "last", captor.getAllValues().get(1).getData().get(0).get("name").getAsString());
        assertFalse(writer.needCommit());
    }

    @Test
    void autoIdInsertsPreserveMappedPrimaryKeysAndPartition() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        Map<String, Object> config = new HashMap<>();
        config.put("field_schema", "[{\"source_field_name\":\"source_id\",\"field_name\":\"id\"}]");
        MilvusCdcWriter writer =
                new MilvusCdcWriter(
                        renamedPrimaryKeyCatalogTable(),
                        ReadonlyConfig.fromMap(config),
                        client,
                        autoIdDescription());
        SeaTunnelRow row = rowWithoutMilvusMetadata(42L, "alice", RowKind.INSERT);
        row.setPartitionName("partition_a");
        writer.write(row);
        writer.commit(true);

        ArgumentCaptor<InsertReq> captor = ArgumentCaptor.forClass(InsertReq.class);
        verify(client).insert(captor.capture());
        assertEquals(42L, captor.getValue().getData().get(0).get("id").getAsLong());
        assertFalse(captor.getValue().getData().get(0).has("source_id"));
        assertEquals("test_collection", captor.getValue().getCollectionName());
        assertEquals("partition_a", captor.getValue().getPartitionName());
        verify(client, never()).upsert(any(UpsertReq.class));
    }

    @Test
    void autoIdDeleteThenInsertPreservesOrderAndPrimaryKey() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = autoIdWriter(client, autoIdDescription());
        writer.write(rowWithoutMilvusMetadata(42L, "before", RowKind.DELETE));
        writer.write(rowWithoutMilvusMetadata(42L, "after", RowKind.INSERT));

        ArgumentCaptor<DeleteReq> deletes = ArgumentCaptor.forClass(DeleteReq.class);
        ArgumentCaptor<InsertReq> inserts = ArgumentCaptor.forClass(InsertReq.class);
        org.mockito.InOrder order = inOrder(client);
        order.verify(client).delete(deletes.capture());
        order.verify(client).insert(inserts.capture());
        assertEquals(Collections.singletonList(42L), deletes.getValue().getIds());
        assertEquals(42L, inserts.getValue().getData().get(0).get("id").getAsLong());
        assertEquals("_default", inserts.getValue().getPartitionName());
        assertFalse(writer.needCommit());
    }

    @Test
    void autoIdPartitionKeyInsertDoesNotSetExplicitPartition() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        DescribeCollectionResp description = autoIdDescription();
        description.getCollectionSchema().getFieldSchemaList().get(1).setIsPartitionKey(true);
        MilvusCdcWriter writer = autoIdWriter(client, description);
        SeaTunnelRow row = rowWithoutMilvusMetadata(42L, "key", RowKind.INSERT);
        row.setPartitionName("partition_a");
        writer.write(row);
        writer.commit(true);
        ArgumentCaptor<InsertReq> captor = ArgumentCaptor.forClass(InsertReq.class);
        verify(client).insert(captor.capture());
        assertTrue(
                captor.getValue().getPartitionName() == null
                        || captor.getValue().getPartitionName().isEmpty());
        assertEquals("key", captor.getValue().getData().get(0).get("name").getAsString());
    }

    @Test
    void partitionKeyTargetBatchesAcrossPhysicalPartitionsAndDeletesBeforeInsert() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        DescribeCollectionResp description = autoIdDescription();
        description.getCollectionSchema().getFieldSchemaList().get(1).setIsPartitionKey(true);
        MilvusCdcWriter writer = autoIdWriter(client, description);
        SeaTunnelRow first = rowWithoutMilvusMetadata(1L, "tenant_a", RowKind.INSERT);
        first.setPartitionName("physical_a");
        SeaTunnelRow second = rowWithoutMilvusMetadata(2L, "tenant_b", RowKind.INSERT);
        second.setPartitionName("physical_b");
        writer.write(first);
        writer.write(second);
        verify(client, never()).insert(any(InsertReq.class));

        SeaTunnelRow delete = rowWithoutMilvusMetadata(1L, null, RowKind.DELETE);
        writer.write(delete);
        SeaTunnelRow replacement = rowWithoutMilvusMetadata(1L, "tenant_c", RowKind.INSERT);
        replacement.setPartitionName("physical_c");
        writer.write(replacement);

        ArgumentCaptor<InsertReq> inserts = ArgumentCaptor.forClass(InsertReq.class);
        ArgumentCaptor<DeleteReq> deletes = ArgumentCaptor.forClass(DeleteReq.class);
        org.mockito.InOrder order = inOrder(client);
        order.verify(client).insert(inserts.capture());
        order.verify(client).delete(deletes.capture());
        order.verify(client).insert(inserts.capture());
        order.verifyNoMoreInteractions();
        assertEquals(2, inserts.getAllValues().get(0).getData().size());
        assertEquals(
                "tenant_c",
                inserts.getAllValues().get(1).getData().get(0).get("name").getAsString());
        for (InsertReq request : inserts.getAllValues()) {
            assertTrue(request.getPartitionName() == null || request.getPartitionName().isEmpty());
        }
        assertTrue(
                deletes.getValue().getPartitionName() == null
                        || deletes.getValue().getPartitionName().isEmpty());
        assertFalse(writer.needCommit());
    }

    @Test
    void autoIdInsertFailureRetainsPendingRowsAndPropagatesError() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        when(client.insert(any(InsertReq.class)))
                .thenThrow(new IllegalStateException("timeout"))
                .thenReturn(null);
        MilvusCdcWriter writer = autoIdWriter(client, autoIdDescription());
        writer.write(rowWithoutMilvusMetadata(42L, "alice", RowKind.INSERT));
        MilvusConnectorException error =
                assertThrows(MilvusConnectorException.class, () -> writer.commit(true));
        assertTrue(error.getMessage().contains("insert CDC data failed"));
        assertTrue(writer.needCommit());
        assertEquals(1L, writer.getWriteCache());
        writer.commit(true);
        ArgumentCaptor<InsertReq> captor = ArgumentCaptor.forClass(InsertReq.class);
        verify(client, times(2)).insert(captor.capture());
        for (InsertReq request : captor.getAllValues()) {
            assertEquals(42L, request.getData().get(0).get("id").getAsLong());
        }
        assertFalse(writer.needCommit());
    }

    @ParameterizedTest
    @ValueSource(longs = {42L, 43L})
    void insertModeAppliesUpdateBeforeAsDeleteThenUpdateAfterAsInsert(long newPrimaryKey) {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = autoIdWriter(client, autoIdDescription());
        writer.write(rowWithoutMilvusMetadata(42L, "before", RowKind.UPDATE_BEFORE));
        writer.write(rowWithoutMilvusMetadata(newPrimaryKey, "after", RowKind.UPDATE_AFTER));

        ArgumentCaptor<DeleteReq> deletes = ArgumentCaptor.forClass(DeleteReq.class);
        ArgumentCaptor<InsertReq> inserts = ArgumentCaptor.forClass(InsertReq.class);
        org.mockito.InOrder order = inOrder(client);
        order.verify(client).delete(deletes.capture());
        order.verify(client).insert(inserts.capture());
        assertEquals(Collections.singletonList(42L), deletes.getValue().getIds());
        assertEquals(newPrimaryKey, inserts.getValue().getData().get(0).get("id").getAsLong());
        assertEquals("after", inserts.getValue().getData().get(0).get("name").getAsString());
        assertFalse(writer.needCommit());
        verify(client, never()).upsert(any(UpsertReq.class));
    }

    @Test
    void insertModeAppliesStandaloneUpdateBeforeAsDelete() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = autoIdWriter(client, autoIdDescription());
        writer.write(rowWithoutMilvusMetadata(42L, "before", RowKind.UPDATE_BEFORE));
        writer.commit(true);

        ArgumentCaptor<DeleteReq> deletes = ArgumentCaptor.forClass(DeleteReq.class);
        verify(client).delete(deletes.capture());
        assertEquals(Collections.singletonList(42L), deletes.getValue().getIds());
        verify(client, never()).insert(any(InsertReq.class));
        verify(client, never()).upsert(any(UpsertReq.class));
        assertFalse(writer.needCommit());
    }

    @Test
    void insertModeAppliesStandaloneUpdateAfterAsInsert() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = autoIdWriter(client, autoIdDescription());
        writer.write(rowWithoutMilvusMetadata(42L, "after", RowKind.UPDATE_AFTER));
        writer.commit(true);

        ArgumentCaptor<InsertReq> inserts = ArgumentCaptor.forClass(InsertReq.class);
        verify(client).insert(inserts.capture());
        assertEquals(42L, inserts.getValue().getData().get(0).get("id").getAsLong());
        verify(client, never()).delete(any(DeleteReq.class));
        verify(client, never()).upsert(any(UpsertReq.class));
        assertFalse(writer.needCommit());
    }

    @Test
    void autoIdRejectsNullPrimaryKeyBeforeBuffering() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusCdcWriter writer = autoIdWriter(client, autoIdDescription());
        assertThrows(
                MilvusConnectorException.class,
                () -> writer.write(new SeaTunnelRow(new Object[] {null, "alice"})));
        assertFalse(writer.needCommit());
        verify(client, never()).insert(any(InsertReq.class));
    }

    private static MilvusCdcWriter autoIdWriter(
            MilvusClientV2 client, DescribeCollectionResp description) {
        return new MilvusCdcWriter(
                catalogTable(),
                ReadonlyConfig.fromMap(new HashMap<>()),
                client,
                description,
                () -> 0L);
    }

    // Keep failure tests bounded without adding retry-policy injection to production constructors.
    static RetryConfig retryConfig(MilvusCdcWriter writer) throws Exception {
        Field field = MilvusCdcWriter.class.getDeclaredField("clientRetryConfig");
        field.setAccessible(true);
        return (RetryConfig) field.get(writer);
    }

    static DescribeCollectionResp autoIdDescription() {
        DescribeCollectionResp base = describeCollectionResp();
        base.getCollectionSchema().getFieldSchemaList().get(0).setAutoID(true);
        return DescribeCollectionResp.builder()
                .collectionName(base.getCollectionName())
                .primaryFieldName("id")
                .enableDynamicField(true)
                .autoID(true)
                .collectionSchema(base.getCollectionSchema())
                .properties(Collections.singletonMap("allow_insert_auto_id", "true"))
                .build();
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
                currentTimeMillis::get);
    }

    private static SeaTunnelRow rowWithoutMilvusMetadata(long id, String name, RowKind rowKind) {
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

    static CatalogTable catalogTable() {
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
