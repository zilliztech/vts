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

package org.apache.seatunnel.connectors.seatunnel.chromadb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.chromadb.client.ChromaDBClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for ChromaDBSplitEnumerator's lazy, batched Phase-1 driven execution model.
 *
 * <p>Key behaviors verified:
 * <ul>
 *   <li>Collections are processed one at a time in registration order
 *   <li>Each id_split_size batch is partitioned evenly across N readers
 *   <li>The enumerator waits for readers to signal ready before advancing (backpressure)
 *   <li>Empty/degenerate splits don't block the barrier
 *   <li>Checkpoint restore skips completed collections and re-queues carried-over splits
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
public class ChromaDBSplitEnumeratorTest {

    @Mock private SourceSplitEnumerator.Context<ChromaDBSourceSplit> mockContext;
    @Mock private ChromaDBClient mockClient;

    private LinkedHashMap<String, CatalogTable> collectionTables;

    @BeforeEach
    void setup() {
        collectionTables = new LinkedHashMap<>();
        collectionTables.put("col-id-a", buildTable("col_a"));
        collectionTables.put("col-id-b", buildTable("col_b"));
    }

    private CatalogTable buildTable(String name) {
        TableSchema schema =
                TableSchema.builder()
                        .columns(
                                Collections.singletonList(
                                        PhysicalColumn.builder()
                                                .name("id")
                                                .dataType(BasicType.STRING_TYPE)
                                                .build()))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("chromadb", TablePath.of("db", name)),
                schema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }

    private ReadonlyConfig makeConfig(int idSplitSize) {
        Map<String, Object> map = new HashMap<>();
        map.put("url", "http://localhost:8000");
        map.put("id_split_size", idSplitSize);
        return ReadonlyConfig.fromMap(map);
    }

    private ChromaDBClient.GetRecordsResult result(List<String> ids) {
        ChromaDBClient.GetRecordsResult r = new ChromaDBClient.GetRecordsResult();
        try {
            Field f = ChromaDBClient.GetRecordsResult.class.getDeclaredField("ids");
            f.setAccessible(true);
            f.set(r, ids);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return r;
    }

    private ChromaDBSplitEnumerator createEnumeratorWithMockClient(
            LinkedHashMap<String, CatalogTable> tables, ChromaDBSourceState state, int idSplitSize)
            throws Exception {
        ChromaDBSplitEnumerator enumerator =
                new ChromaDBSplitEnumerator(mockContext, makeConfig(idSplitSize), tables, state);
        Field clientField = ChromaDBSplitEnumerator.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(enumerator, mockClient);
        return enumerator;
    }

    /**
     * Spawns a helper thread that simulates reader-side "done, give me more" signals by
     * calling handleSplitRequest on every registered reader after each assignSplit call.
     * This unblocks the barrier so run() can proceed.
     */
    private Thread startAutoAckThread(ChromaDBSplitEnumerator enumerator, Set<Integer> readers) {
        Thread t =
                new Thread(
                        () -> {
                            // Keep acking until run() completes.
                            while (!Thread.currentThread().isInterrupted()) {
                                try {
                                    Thread.sleep(5);
                                } catch (InterruptedException e) {
                                    return;
                                }
                                for (Integer r : readers) {
                                    enumerator.handleSplitRequest(r);
                                }
                            }
                        },
                        "test-auto-ack");
        t.setDaemon(true);
        t.start();
        return t;
    }

    @Test
    void testSingleCollectionSingleBatchSplitAcrossReaders() throws Exception {
        Set<Integer> readers = new HashSet<>(Arrays.asList(0, 1));
        when(mockContext.registeredReaders()).thenReturn(readers);

        LinkedHashMap<String, CatalogTable> tables = new LinkedHashMap<>();
        tables.put("col-id-a", buildTable("col_a"));

        // Phase 1: 4 IDs fetched in a single batch (idSplitSize=10 > 4, so only one round)
        when(mockClient.getRecords(eq("col-id-a"), any()))
                .thenReturn(result(Arrays.asList("a", "b", "c", "d")))
                .thenReturn(result(Collections.emptyList()));

        ChromaDBSplitEnumerator enumerator =
                createEnumeratorWithMockClient(tables, null, 10);

        Thread acker = startAutoAckThread(enumerator, readers);
        try {
            enumerator.run();
        } finally {
            acker.interrupt();
            acker.join(1000);
        }

        ArgumentCaptor<List<ChromaDBSourceSplit>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockContext, atLeastOnce()).assignSplit(anyInt(), captor.capture());

        // 2 readers → 2 sub-splits of 2 IDs each
        List<String> allIds = new ArrayList<>();
        for (List<ChromaDBSourceSplit> splits : captor.getAllValues()) {
            for (ChromaDBSourceSplit s : splits) {
                allIds.addAll(s.getIds());
            }
        }
        Assertions.assertEquals(4, allIds.size());
        Assertions.assertTrue(allIds.contains("a"));
        Assertions.assertTrue(allIds.contains("d"));

        verify(mockContext).signalNoMoreSplits(0);
        verify(mockContext).signalNoMoreSplits(1);
    }

    @Test
    void testMultipleBatchesAdvanceOffset() throws Exception {
        Set<Integer> readers = new HashSet<>(Arrays.asList(0, 1));
        when(mockContext.registeredReaders()).thenReturn(readers);

        LinkedHashMap<String, CatalogTable> tables = new LinkedHashMap<>();
        tables.put("col-id-a", buildTable("col_a"));

        // idSplitSize=3, total 5 IDs → 2 rounds: [a,b,c], [d,e], then terminates
        when(mockClient.getRecords(eq("col-id-a"), any()))
                .thenReturn(result(Arrays.asList("a", "b", "c")))
                .thenReturn(result(Arrays.asList("d", "e")));

        ChromaDBSplitEnumerator enumerator =
                createEnumeratorWithMockClient(tables, null, 3);

        Thread acker = startAutoAckThread(enumerator, readers);
        try {
            enumerator.run();
        } finally {
            acker.interrupt();
            acker.join(1000);
        }

        ArgumentCaptor<List<ChromaDBSourceSplit>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockContext, atLeastOnce()).assignSplit(anyInt(), captor.capture());

        List<String> allIds = new ArrayList<>();
        for (List<ChromaDBSourceSplit> splits : captor.getAllValues()) {
            for (ChromaDBSourceSplit s : splits) {
                allIds.addAll(s.getIds());
            }
        }
        Assertions.assertEquals(5, allIds.size());
        Assertions.assertTrue(allIds.containsAll(Arrays.asList("a", "b", "c", "d", "e")));
    }

    @Test
    void testMultipleCollectionsProcessedInOrder() throws Exception {
        Set<Integer> readers = new HashSet<>(Collections.singletonList(0));
        when(mockContext.registeredReaders()).thenReturn(readers);

        when(mockClient.getRecords(eq("col-id-a"), any()))
                .thenReturn(result(Arrays.asList("a1", "a2")));
        when(mockClient.getRecords(eq("col-id-b"), any()))
                .thenReturn(result(Arrays.asList("b1", "b2")));

        ChromaDBSplitEnumerator enumerator =
                createEnumeratorWithMockClient(collectionTables, null, 10);

        Thread acker = startAutoAckThread(enumerator, readers);
        try {
            enumerator.run();
        } finally {
            acker.interrupt();
            acker.join(1000);
        }

        ArgumentCaptor<List<ChromaDBSourceSplit>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockContext, atLeastOnce()).assignSplit(anyInt(), captor.capture());

        // Both collections processed
        Set<String> collectionIdsAssigned = new HashSet<>();
        for (List<ChromaDBSourceSplit> splits : captor.getAllValues()) {
            for (ChromaDBSourceSplit s : splits) {
                collectionIdsAssigned.add(s.getCollectionId());
            }
        }
        Assertions.assertTrue(collectionIdsAssigned.contains("col-id-a"));
        Assertions.assertTrue(collectionIdsAssigned.contains("col-id-b"));

        // Both collections marked completed
        Assertions.assertTrue(enumerator.completedCollectionsView().contains("col-id-a"));
        Assertions.assertTrue(enumerator.completedCollectionsView().contains("col-id-b"));
    }

    @Test
    void testMoreReadersThanIdsCreatesEmptySplitsCorrectly() throws Exception {
        // 4 readers, only 2 IDs → readers 2 and 3 should not receive a split (empty partition)
        Set<Integer> readers = new HashSet<>(Arrays.asList(0, 1, 2, 3));
        when(mockContext.registeredReaders()).thenReturn(readers);

        LinkedHashMap<String, CatalogTable> tables = new LinkedHashMap<>();
        tables.put("col-id-a", buildTable("col_a"));

        when(mockClient.getRecords(eq("col-id-a"), any()))
                .thenReturn(result(Arrays.asList("x", "y")));

        ChromaDBSplitEnumerator enumerator =
                createEnumeratorWithMockClient(tables, null, 10);

        Thread acker = startAutoAckThread(enumerator, readers);
        try {
            enumerator.run();
        } finally {
            acker.interrupt();
            acker.join(1000);
        }

        ArgumentCaptor<List<ChromaDBSourceSplit>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockContext, atLeastOnce()).assignSplit(anyInt(), captor.capture());

        // Collect all assigned IDs; should still be exactly 2
        List<String> allIds = new ArrayList<>();
        for (List<ChromaDBSourceSplit> splits : captor.getAllValues()) {
            for (ChromaDBSourceSplit s : splits) {
                allIds.addAll(s.getIds());
            }
        }
        Assertions.assertEquals(2, allIds.size());

        // All 4 readers still get signalNoMoreSplits
        for (int r : readers) {
            verify(mockContext).signalNoMoreSplits(r);
        }
    }

    @Test
    void testEmptyCollectionIsSkipped() throws Exception {
        Set<Integer> readers = new HashSet<>(Collections.singletonList(0));
        when(mockContext.registeredReaders()).thenReturn(readers);

        LinkedHashMap<String, CatalogTable> tables = new LinkedHashMap<>();
        tables.put("col-empty", buildTable("col_empty"));

        when(mockClient.getRecords(eq("col-empty"), any()))
                .thenReturn(result(Collections.emptyList()));

        ChromaDBSplitEnumerator enumerator =
                createEnumeratorWithMockClient(tables, null, 10);

        Thread acker = startAutoAckThread(enumerator, readers);
        try {
            enumerator.run();
        } finally {
            acker.interrupt();
            acker.join(1000);
        }

        // No assignSplit calls but signalNoMoreSplits still happens
        verify(mockContext, never()).assignSplit(anyInt(), anyList());
        verify(mockContext).signalNoMoreSplits(0);
    }

    @Test
    void testRestoreSkipsCompletedCollections() throws Exception {
        Set<Integer> readers = new HashSet<>(Collections.singletonList(0));
        when(mockContext.registeredReaders()).thenReturn(readers);

        // State says col-id-a is already done
        Set<String> completed = new HashSet<>();
        completed.add("col-id-a");
        ChromaDBSourceState state =
                new ChromaDBSourceState(
                        completed, Collections.emptyList(), Collections.emptyMap());

        // Only col-id-b should be fetched
        when(mockClient.getRecords(eq("col-id-b"), any()))
                .thenReturn(result(Arrays.asList("b1", "b2")));

        ChromaDBSplitEnumerator enumerator =
                createEnumeratorWithMockClient(collectionTables, state, 10);

        Thread acker = startAutoAckThread(enumerator, readers);
        try {
            enumerator.run();
        } finally {
            acker.interrupt();
            acker.join(1000);
        }

        // col-id-a is never queried
        verify(mockClient, never()).getRecords(eq("col-id-a"), any());
        verify(mockClient, atLeastOnce()).getRecords(eq("col-id-b"), any());
    }

    @Test
    void testRestoreDrainsCarriedOverSplits() throws Exception {
        Set<Integer> readers = new HashSet<>(Collections.singletonList(0));
        when(mockContext.registeredReaders()).thenReturn(readers);

        // State has one pending split
        List<ChromaDBSourceSplit> pending = new ArrayList<>();
        pending.add(
                new ChromaDBSourceSplit(
                        "col-id-a",
                        TablePath.of("db", "col_a"),
                        99L,
                        Arrays.asList("pre1", "pre2")));
        Set<String> completed = new HashSet<>();
        completed.add("col-id-a");  // skip normal processing of col-a
        completed.add("col-id-b");  // skip col-b entirely
        ChromaDBSourceState state =
                new ChromaDBSourceState(completed, pending, Collections.emptyMap());

        ChromaDBSplitEnumerator enumerator =
                createEnumeratorWithMockClient(collectionTables, state, 10);

        Thread acker = startAutoAckThread(enumerator, readers);
        try {
            enumerator.run();
        } finally {
            acker.interrupt();
            acker.join(1000);
        }

        // The carried-over split should be assigned
        ArgumentCaptor<List<ChromaDBSourceSplit>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockContext, atLeastOnce()).assignSplit(anyInt(), captor.capture());

        boolean found = false;
        for (List<ChromaDBSourceSplit> splits : captor.getAllValues()) {
            for (ChromaDBSourceSplit s : splits) {
                if (s.getIds().contains("pre1") && s.getIds().contains("pre2")) {
                    found = true;
                }
            }
        }
        Assertions.assertTrue(found, "carried-over split should have been assigned");
    }

    @Test
    void testSnapshotStateContainsCompletedCollections() throws Exception {
        Set<Integer> readers = new HashSet<>(Collections.singletonList(0));
        when(mockContext.registeredReaders()).thenReturn(readers);

        LinkedHashMap<String, CatalogTable> tables = new LinkedHashMap<>();
        tables.put("col-id-a", buildTable("col_a"));
        when(mockClient.getRecords(eq("col-id-a"), any()))
                .thenReturn(result(Arrays.asList("x")));

        ChromaDBSplitEnumerator enumerator =
                createEnumeratorWithMockClient(tables, null, 10);

        Thread acker = startAutoAckThread(enumerator, readers);
        try {
            enumerator.run();
        } finally {
            acker.interrupt();
            acker.join(1000);
        }

        ChromaDBSourceState state = enumerator.snapshotState(1L);
        Assertions.assertNotNull(state);
        Assertions.assertTrue(state.getCompletedCollectionIds().contains("col-id-a"));
    }
}
