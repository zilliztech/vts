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
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.chromadb.client.ChromaDBClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChromaDBSplitEnumeratorTest {
    private SourceSplitEnumerator.Context<ChromaDBSourceSplit> context;
    private ChromaDBClient client;
    private final Map<String, CatalogTable> tables = new LinkedHashMap<>();
    private final Set<Integer> readers = new HashSet<>();
    private final Map<Integer, List<ChromaDBSourceSplit>> assigned = new LinkedHashMap<>();

    @BeforeEach
    void setup() {
        context = mock(SourceSplitEnumerator.Context.class);
        client = mock(ChromaDBClient.class);
        addTable("a");
        when(context.registeredReaders()).thenAnswer(ignored -> new HashSet<>(readers));
        doAnswer(
                        invocation -> {
                            int reader = invocation.getArgument(0);
                            List<ChromaDBSourceSplit> splits = invocation.getArgument(1);
                            assigned.computeIfAbsent(reader, ignored -> new ArrayList<>())
                                    .addAll(splits);
                            return null;
                        })
                .when(context)
                .assignSplit(anyInt(), anyList());
    }

    private void addTable(String id) {
        TablePath path = TablePath.of("db", id);
        TableSchema schema =
                TableSchema.builder()
                        .columns(
                                Collections.singletonList(
                                        PhysicalColumn.builder()
                                                .name("id")
                                                .dataType(BasicType.STRING_TYPE)
                                                .build()))
                        .build();
        tables.put(
                id,
                CatalogTable.of(
                        TableIdentifier.of("chromadb", path),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        ""));
    }

    private ChromaDBSplitEnumerator enumerator(int parallelism, ChromaDBSourceState state)
            throws Exception {
        when(context.currentParallelism()).thenReturn(parallelism);
        ChromaDBSplitEnumerator result =
                new ChromaDBSplitEnumerator(
                        context,
                        ReadonlyConfig.fromMap(
                                Collections.singletonMap("url", "http://localhost:8000")),
                        tables,
                        state);
        Field field = ChromaDBSplitEnumerator.class.getDeclaredField("client");
        field.setAccessible(true);
        field.set(result, client);
        return result;
    }

    @Test
    void rangesCoverCollectionExactlyAndAreAssignedInBatches() throws Exception {
        readers.addAll(Arrays.asList(0, 1, 2));
        when(client.countRecords("a")).thenReturn(10L);
        ChromaDBSplitEnumerator enumerator = enumerator(3, null);
        assertTimeoutPreemptively(Duration.ofSeconds(2), enumerator::run);
        assertEquals(0, assigned.get(0).get(0).getStartOffset());
        assertEquals(3, assigned.get(0).get(0).getEndOffset());
        assertEquals(3, assigned.get(1).get(0).getStartOffset());
        assertEquals(6, assigned.get(1).get(0).getEndOffset());
        assertEquals(6, assigned.get(2).get(0).getStartOffset());
        assertEquals(10, assigned.get(2).get(0).getEndOffset());
        verify(context, times(3)).assignSplit(anyInt(), anyList());
        verify(context, times(3)).signalNoMoreSplits(anyInt());
        assertTrue(enumerator.snapshotState(1).getPendingSplits().isEmpty());
        enumerator.run();
        verify(client, times(1)).countRecords("a");
    }

    @Test
    void singleReaderUsesTheSameCountAndRangeProtocol() throws Exception {
        readers.add(0);
        when(client.countRecords("a")).thenReturn(17L);
        ChromaDBSplitEnumerator enumerator = enumerator(1, null);
        enumerator.run();
        assertEquals(
                new ChromaDBSourceSplit("a", tables.get("a").getTablePath(), 0, 17),
                assigned.get(0).get(0));
        verify(client).countRecords("a");
    }

    @Test
    void fewerRowsThanReadersSkipsEmptyRanges() throws Exception {
        readers.addAll(Arrays.asList(0, 1, 2, 3));
        when(client.countRecords("a")).thenReturn(2L);
        ChromaDBSplitEnumerator enumerator = enumerator(4, null);
        enumerator.run();
        assertEquals(2, assigned.values().stream().mapToInt(List::size).sum());
        assertEquals(0, assigned.get(0).get(0).getStartOffset());
        assertEquals(1, assigned.get(1).get(0).getStartOffset());
        verify(context, times(4)).signalNoMoreSplits(anyInt());
    }

    @Test
    void emptyCollectionsAndMultipleCollectionsShareReaderPool() throws Exception {
        addTable("b");
        addTable("empty");
        readers.addAll(Arrays.asList(0, 1));
        when(client.countRecords("a")).thenReturn(4L);
        when(client.countRecords("b")).thenReturn(6L);
        ChromaDBSplitEnumerator enumerator = enumerator(2, null);
        enumerator.run();
        assertEquals(2, assigned.size());
        for (List<ChromaDBSourceSplit> splits : assigned.values()) {
            assertEquals(
                    Arrays.asList("a", "b"),
                    splits.stream()
                            .map(ChromaDBSourceSplit::getCollectionId)
                            .collect(Collectors.toList()));
        }
        verify(context, times(2)).assignSplit(anyInt(), anyList());
        assertEquals(tables.keySet(), enumerator.snapshotState(1).getEnumeratedCollectionIds());
    }

    @Test
    void lateReadersDoNotBlockOrReceivePrematureCompletion() throws Exception {
        readers.add(0);
        when(client.countRecords("a")).thenReturn(4L);
        ChromaDBSplitEnumerator enumerator = enumerator(2, null);
        assertTimeoutPreemptively(Duration.ofSeconds(2), enumerator::run);
        verify(context, never()).assignSplit(anyInt(), anyList());
        verify(context, never()).signalNoMoreSplits(anyInt());
        assertEquals(2, enumerator.currentUnassignedSplitSize());
        readers.add(1);
        enumerator.registerReader(1);
        assertEquals(2, assigned.size());
        verify(context, times(2)).signalNoMoreSplits(anyInt());
    }

    @Test
    void pendingCheckpointRestoresOriginalBoundsWithoutRecounting() throws Exception {
        when(client.countRecords("a")).thenReturn(10L);
        ChromaDBSplitEnumerator original = enumerator(3, null);
        original.run();
        DefaultSerializer<ChromaDBSourceState> serializer = new DefaultSerializer<>();
        ChromaDBSourceState snapshot =
                serializer.deserialize(serializer.serialize(original.snapshotState(1)));
        readers.addAll(Arrays.asList(0, 1));
        ChromaDBSplitEnumerator restored = enumerator(2, snapshot);
        restored.run();
        verify(client, times(1)).countRecords("a");
        assertEquals(3, assigned.values().stream().mapToInt(List::size).sum());
        assertEquals(10, assigned.get(0).get(1).getEndOffset());
        assertEquals(3, snapshot.getPendingSplits().size());
        assertEquals(0, restored.currentUnassignedSplitSize());
    }

    @Test
    void assignedSplitsAreRestoredOnlyFromReaderState() throws Exception {
        readers.add(0);
        when(client.countRecords("a")).thenReturn(5L);
        ChromaDBSplitEnumerator original = enumerator(1, null);
        original.run();
        ChromaDBSourceState checkpoint = original.snapshotState(1);
        ChromaDBSourceSplit readerCheckpoint =
                assigned.get(0)
                        .get(0)
                        .withPendingIds(Arrays.asList("0", "1", "2", "3", "4"))
                        .advance(2)
                        .copy();
        assigned.clear();
        readers.clear();
        ChromaDBSplitEnumerator restored = enumerator(1, checkpoint);
        restored.addSplitsBack(Collections.singletonList(readerCheckpoint), 0);
        restored.addSplitsBack(Collections.singletonList(readerCheckpoint), 0);
        readers.add(0);
        restored.registerReader(0);
        restored.run();
        assertEquals(Collections.singletonList(readerCheckpoint), assigned.get(0));
        verify(client, times(1)).countRecords("a");
        assertTrue(restored.snapshotState(2).getPendingSplits().isEmpty());
    }

    @Test
    void failedAssignmentRemainsPendingAndDoesNotSignalCompletion() throws Exception {
        readers.add(0);
        when(client.countRecords("a")).thenReturn(5L);
        doThrow(new IllegalStateException("assignment failed"))
                .when(context)
                .assignSplit(anyInt(), anyList());
        ChromaDBSplitEnumerator enumerator = enumerator(1, null);
        assertThrows(IllegalStateException.class, enumerator::run);
        assertEquals(1, enumerator.snapshotState(1).getPendingSplits().size());
        verify(context, never()).signalNoMoreSplits(anyInt());
    }

    @Test
    void countAndRangeArithmeticDoNotOverflow() throws Exception {
        readers.addAll(Arrays.asList(0, 1, 2));
        when(client.countRecords("a")).thenReturn(Long.MAX_VALUE);
        enumerator(3, null).run();
        assertEquals(0, assigned.get(0).get(0).getStartOffset());
        assertEquals(
                assigned.get(0).get(0).getEndOffset(), assigned.get(1).get(0).getStartOffset());
        assertEquals(
                assigned.get(1).get(0).getEndOffset(), assigned.get(2).get(0).getStartOffset());
        assertEquals(Long.MAX_VALUE, assigned.get(2).get(0).getEndOffset());
    }

    @Test
    void checkpointDuringCountingDoesNotRecountAlreadyEnumeratedCollections() throws Exception {
        addTable("b");
        when(client.countRecords("a")).thenReturn(2L);
        ChromaDBSplitEnumerator enumerator = enumerator(1, null);
        AtomicReference<ChromaDBSourceState> checkpoint = new AtomicReference<>();
        when(client.countRecords("b"))
                .thenAnswer(
                        ignored -> {
                            checkpoint.set(enumerator.snapshotState(1));
                            return 3L;
                        });
        enumerator.run();
        assertEquals(Collections.singleton("a"), checkpoint.get().getEnumeratedCollectionIds());
        doReturn(3L).when(client).countRecords("b");
        readers.add(0);
        enumerator(1, checkpoint.get()).run();
        verify(client, times(1)).countRecords("a");
        verify(client, times(2)).countRecords("b");
        assertEquals(2, assigned.get(0).size());
    }
}
