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

package org.apache.seatunnel.connectors.seatunnel.milvus.source;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MilvusSourceSplitEnumeratorTest {

    @Test
    void assignsCollidingSplitIdsEvenly() throws Exception {
        MilvusSourceSplitEnumerator enumerator = newEnumerator(4, null);
        // "Aa" and "BB" have identical Java hash codes.
        addSplits(enumerator, Arrays.asList(split("Aa"), split("BB")));

        Map<Integer, List<MilvusSourceSplit>> assignments =
                enumerator.snapshotState(1).getPendingSplits();
        assertEquals(1, assignments.get(0).size());
        assertEquals(1, assignments.get(1).size());
    }

    @Test
    void continuesRoundRobinAcrossCollectionsAndRestore() throws Exception {
        MilvusSourceSplitEnumerator enumerator = newEnumerator(3, null);
        addSplits(enumerator, Collections.singletonList(split("first-collection")));
        addSplits(enumerator, Collections.singletonList(split("second-collection")));

        DefaultSerializer<MilvusSourceState> serializer = new DefaultSerializer<>();
        MilvusSourceState state =
                serializer.deserialize(serializer.serialize(enumerator.snapshotState(1)));
        assertEquals(2L, state.getNextAssignment());
        assertEquals("first-collection", state.getPendingSplits().get(0).get(0).splitId());
        assertEquals("second-collection", state.getPendingSplits().get(1).get(0).splitId());

        MilvusSourceSplitEnumerator restored = newEnumerator(3, state);
        addSplits(restored, Collections.singletonList(split("third-collection")));
        assertEquals(
                "third-collection",
                restored.snapshotState(2).getPendingSplits().get(2).get(0).splitId());
    }

    @Test
    void restoresCheckpointWrittenBeforeRoundRobinAssignment() throws Exception {
        // Serialized with the unmodified state class from commit 5c7a2c4fb9 (no explicit UID).
        URL fixture = getClass().getResource("/milvus-source-state-before-round-robin.ser");
        assertNotNull(fixture);
        MilvusSourceState state =
                new DefaultSerializer<MilvusSourceState>()
                        .deserialize(Files.readAllBytes(Paths.get(fixture.toURI())));

        assertEquals(0L, state.getNextAssignment());
        assertEquals(
                Collections.singletonList(TablePath.of("default", "remaining_collection")),
                state.getPendingTables());
        assertEquals(Collections.singleton(2), state.getPendingSplits().keySet());
        assertEquals(1, state.getPendingSplits().get(2).size());
        MilvusSourceSplit pending = state.getPendingSplits().get(2).get(0);
        assertEquals("source_collection-offset-250-limit-250", pending.splitId());
        assertEquals(TablePath.of("default", "source_collection"), pending.getTablePath());
        assertEquals("source_collection", pending.getCollectionName());
        assertEquals("partition_a", pending.getPartitionName());
        assertEquals(Long.valueOf(250), pending.getOffset());
        assertEquals(Long.valueOf(250), pending.getLimit());

        MilvusSourceSplitEnumerator restored = newEnumerator(3, state);
        addSplits(restored, Collections.singletonList(split("new-split")));
        MilvusSourceState nextState = restored.snapshotState(2);
        assertEquals("new-split", nextState.getPendingSplits().get(0).get(0).splitId());
        assertEquals(state.getPendingSplits().get(2), nextState.getPendingSplits().get(2));
        assertEquals(state.getPendingTables(), nextState.getPendingTables());
    }

    private static MilvusSourceSplitEnumerator newEnumerator(
            int readerCount, MilvusSourceState state) {
        @SuppressWarnings("unchecked")
        SourceSplitEnumerator.Context<MilvusSourceSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(readerCount);
        return new MilvusSourceSplitEnumerator(context, null, Collections.emptyMap(), state);
    }

    private static MilvusSourceSplit split(String id) {
        return MilvusSourceSplit.builder().splitId(id).build();
    }

    private static void addSplits(
            MilvusSourceSplitEnumerator enumerator, Collection<MilvusSourceSplit> splits)
            throws Exception {
        Method method =
                MilvusSourceSplitEnumerator.class.getDeclaredMethod(
                        "addPendingSplit", Collection.class);
        method.setAccessible(true);
        method.invoke(enumerator, splits);
    }
}
