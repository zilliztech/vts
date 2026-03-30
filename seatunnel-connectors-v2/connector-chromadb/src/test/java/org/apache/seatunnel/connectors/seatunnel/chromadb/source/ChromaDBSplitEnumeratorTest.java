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

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ChromaDBSplitEnumeratorTest {

    @Mock private SourceSplitEnumerator.Context<ChromaDBSourceSplit> mockContext;

    private LinkedHashMap<String, CatalogTable> collectionTables;

    @BeforeEach
    void setup() {
        collectionTables = new LinkedHashMap<>();
        collectionTables.put("col-id-a", buildTable("col_a"));
        collectionTables.put("col-id-b", buildTable("col_b"));
        collectionTables.put("col-id-c", buildTable("col_c"));
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

    @Test
    void testSplitsAssignedToRegisteredReaders() throws Exception {
        Set<Integer> readers = new HashSet<>();
        readers.add(0);
        readers.add(1);
        when(mockContext.registeredReaders()).thenReturn(readers);

        ChromaDBSplitEnumerator enumerator =
                new ChromaDBSplitEnumerator(mockContext, collectionTables, null);
        enumerator.run();

        // 3 splits assigned to 2 readers (round-robin)
        ArgumentCaptor<List<ChromaDBSourceSplit>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockContext, times(3)).assignSplit(anyInt(), captor.capture());

        List<String> assignedIds = new ArrayList<>();
        for (List<ChromaDBSourceSplit> splits : captor.getAllValues()) {
            for (ChromaDBSourceSplit s : splits) {
                assignedIds.add(s.getCollectionId());
            }
        }
        Assertions.assertEquals(3, assignedIds.size());
        Assertions.assertTrue(assignedIds.contains("col-id-a"));
        Assertions.assertTrue(assignedIds.contains("col-id-b"));
        Assertions.assertTrue(assignedIds.contains("col-id-c"));

        // All readers signaled no more splits
        verify(mockContext, times(2)).signalNoMoreSplits(anyInt());
    }

    @Test
    void testSnapshotState() throws Exception {
        Set<Integer> readers = new HashSet<>();
        readers.add(0);
        when(mockContext.registeredReaders()).thenReturn(readers);

        ChromaDBSplitEnumerator enumerator =
                new ChromaDBSplitEnumerator(mockContext, collectionTables, null);
        enumerator.run();

        ChromaDBSourceState state = enumerator.snapshotState(1L);
        Assertions.assertNotNull(state);
        // All splits assigned, pending should be empty
        Assertions.assertTrue(state.getPendingSplits().isEmpty());
    }

    @Test
    void testRestoreFromState() throws Exception {
        // Create state with 1 pending split
        List<ChromaDBSourceSplit> pending = new ArrayList<>();
        pending.add(new ChromaDBSourceSplit("col-id-a", TablePath.of("db", "col_a")));
        ChromaDBSourceState state = new ChromaDBSourceState(pending, Collections.emptyMap());

        Set<Integer> readers = new HashSet<>();
        readers.add(0);
        when(mockContext.registeredReaders()).thenReturn(readers);

        ChromaDBSplitEnumerator enumerator =
                new ChromaDBSplitEnumerator(mockContext, collectionTables, state);
        enumerator.run();

        // Only the 1 pending split should be assigned (not all 3)
        ArgumentCaptor<List<ChromaDBSourceSplit>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockContext, times(1)).assignSplit(anyInt(), captor.capture());
        Assertions.assertEquals("col-id-a", captor.getValue().get(0).getCollectionId());
    }

    @Test
    void testAddSplitsBack() throws Exception {
        Set<Integer> readers = new HashSet<>();
        readers.add(0);
        when(mockContext.registeredReaders()).thenReturn(readers);

        ChromaDBSplitEnumerator enumerator =
                new ChromaDBSplitEnumerator(mockContext, collectionTables, null);
        enumerator.run();

        // Add a split back
        List<ChromaDBSourceSplit> returned = new ArrayList<>();
        returned.add(new ChromaDBSourceSplit("col-id-b", TablePath.of("db", "col_b")));
        enumerator.addSplitsBack(returned, 0);

        // Should reassign
        verify(mockContext, times(4)).assignSplit(anyInt(), anyList());
    }

    @Test
    void testEmptyCollectionTables() throws Exception {
        Set<Integer> readers = new HashSet<>();
        readers.add(0);
        when(mockContext.registeredReaders()).thenReturn(readers);

        ChromaDBSplitEnumerator enumerator =
                new ChromaDBSplitEnumerator(mockContext, new LinkedHashMap<>(), null);
        enumerator.run();

        // No splits to assign
        verify(mockContext, never()).assignSplit(anyInt(), anyList());
        verify(mockContext).signalNoMoreSplits(0);
    }

    @Test
    void testCurrentUnassignedSplitSize() {
        ChromaDBSplitEnumerator enumerator =
                new ChromaDBSplitEnumerator(mockContext, collectionTables, null);
        Assertions.assertEquals(3, enumerator.currentUnassignedSplitSize());
    }
}
