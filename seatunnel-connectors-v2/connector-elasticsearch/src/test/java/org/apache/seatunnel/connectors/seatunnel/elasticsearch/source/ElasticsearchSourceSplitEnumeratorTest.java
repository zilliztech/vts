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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.IndexDocsCount;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ShardInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the enumerator's key algorithms and lifecycle edges. Covers:
 * <ul>
 *   <li>{@code interleaveByBucket} — the datanode-bucketed interleave that
 *       spreads same-datanode splits across readers;</li>
 *   <li>reader distribution after {@code i % readerCount} push-assignment —
 *       verifies the "first N readers hit up to N distinct datanodes" property;</li>
 *   <li>{@code snapshotState} isolation — deep-copy invariant;</li>
 *   <li>{@code Split} preference assembly, including empty-{@code nodeId}
 *       normalization;</li>
 *   <li>scale-up / scale-down savepoint restore via re-interleave;</li>
 *   <li>dispatch retry + exhaustion behavior.</li>
 * </ul>
 */
public class ElasticsearchSourceSplitEnumeratorTest {

    // ==================== interleaveByBucket ====================

    @Test
    public void testInterleaveEmpty() {
        Map<String, List<ElasticsearchSourceSplit>> empty = new LinkedHashMap<>();
        Assertions.assertTrue(
                ElasticsearchSourceSplitEnumerator.interleaveByBucket(empty).isEmpty());
    }

    @Test
    public void testInterleaveSingleBucketPreservesOrder() {
        Map<String, List<ElasticsearchSourceSplit>> byNode = new LinkedHashMap<>();
        byNode.put("nodeA", Arrays.asList(split("A-1"), split("A-2"), split("A-3")));

        List<ElasticsearchSourceSplit> out =
                ElasticsearchSourceSplitEnumerator.interleaveByBucket(byNode);

        Assertions.assertEquals(Arrays.asList("A-1", "A-2", "A-3"), ids(out));
    }

    @Test
    public void testInterleaveRoundRobinsAcrossBuckets() {
        // A:3, B:2, C:1 — expected interleave: A1,B1,C1, A2,B2, A3
        Map<String, List<ElasticsearchSourceSplit>> byNode = new LinkedHashMap<>();
        byNode.put("nodeA", Arrays.asList(split("A-1"), split("A-2"), split("A-3")));
        byNode.put("nodeB", Arrays.asList(split("B-1"), split("B-2")));
        byNode.put("nodeC", Collections.singletonList(split("C-1")));

        List<ElasticsearchSourceSplit> out =
                ElasticsearchSourceSplitEnumerator.interleaveByBucket(byNode);

        Assertions.assertEquals(
                Arrays.asList("A-1", "B-1", "C-1", "A-2", "B-2", "A-3"), ids(out));
    }

    @Test
    public void testInterleaveHeavyImbalance() {
        // A:6, B:2, C:2 — canonical imbalance. Expected interleave:
        //   A1,B1,C1, A2,B2,C2, A3, A4, A5, A6
        Map<String, List<ElasticsearchSourceSplit>> byNode = new LinkedHashMap<>();
        byNode.put(
                "nodeA",
                Arrays.asList(
                        split("A-1"), split("A-2"), split("A-3"),
                        split("A-4"), split("A-5"), split("A-6")));
        byNode.put("nodeB", Arrays.asList(split("B-1"), split("B-2")));
        byNode.put("nodeC", Arrays.asList(split("C-1"), split("C-2")));

        List<ElasticsearchSourceSplit> out =
                ElasticsearchSourceSplitEnumerator.interleaveByBucket(byNode);

        Assertions.assertEquals(
                Arrays.asList(
                        "A-1", "B-1", "C-1", "A-2", "B-2", "C-2", "A-3", "A-4", "A-5", "A-6"),
                ids(out));
    }

    @Test
    public void testInterleaveLosesNoSplits() {
        Map<String, List<ElasticsearchSourceSplit>> byNode = new LinkedHashMap<>();
        byNode.put("n1", Arrays.asList(split("x-1"), split("x-2"), split("x-3")));
        byNode.put("n2", Arrays.asList(split("y-1"), split("y-2")));
        byNode.put("n3", Collections.singletonList(split("z-1")));

        int total = byNode.values().stream().mapToInt(List::size).sum();
        Assertions.assertEquals(
                total,
                ElasticsearchSourceSplitEnumerator.interleaveByBucket(byNode).size());
    }

    // ==================== reader distribution after interleave + i%N ====================

    @Test
    public void testReaderDistributionHeavyImbalance() {
        // Same 6/2/2 scenario with 4 readers. Apply i%4 to the interleaved list:
        //   A1 B1 C1 A2  B2 C2 A3 A4  A5 A6
        //   R0 R1 R2 R3  R0 R1 R2 R3  R0 R1
        List<String> interleavedIds =
                Arrays.asList(
                        "A-1", "B-1", "C-1", "A-2", "B-2", "C-2", "A-3", "A-4", "A-5", "A-6");
        List<List<String>> perReader = roundRobinIds(interleavedIds, 4);

        Assertions.assertEquals(Arrays.asList("A-1", "B-2", "A-5"), perReader.get(0));
        Assertions.assertEquals(Arrays.asList("B-1", "C-2", "A-6"), perReader.get(1));
        Assertions.assertEquals(Arrays.asList("C-1", "A-3"), perReader.get(2));
        Assertions.assertEquals(Arrays.asList("A-2", "A-4"), perReader.get(3));

        // Opening batch (first pick per reader): A, B, C, A — three distinct
        // datanodes hit simultaneously instead of the hash-based worst case
        // where all four readers could start pounding the same datanode.
        Assertions.assertEquals(
                Arrays.asList("A", "B", "C", "A"), firstLetters(perReader));

        for (List<String> readerSplits : perReader) {
            Assertions.assertFalse(readerSplits.isEmpty());
            Assertions.assertTrue(readerSplits.size() <= 3);
        }
    }

    @Test
    public void testReaderDistributionSingleDatanodeFallback() {
        // wan_only or nodes/http failure — all splits in one bucket. Interleave
        // degenerates to insertion order; i%N still distributes evenly.
        List<String> interleaved = Arrays.asList("s-1", "s-2", "s-3", "s-4", "s-5", "s-6", "s-7");
        List<List<String>> perReader = roundRobinIds(interleaved, 3);

        Assertions.assertEquals(Arrays.asList("s-1", "s-4", "s-7"), perReader.get(0));
        Assertions.assertEquals(Arrays.asList("s-2", "s-5"), perReader.get(1));
        Assertions.assertEquals(Arrays.asList("s-3", "s-6"), perReader.get(2));
    }

    @Test
    public void testReaderDistributionSameDatanodeSpreadsAcrossReaders() {
        // If all shards are on one datanode but parallelism > 1, each reader
        // should still get roughly equal work.
        Map<String, List<ElasticsearchSourceSplit>> byNode = new LinkedHashMap<>();
        byNode.put(
                "onlyNode",
                Arrays.asList(
                        split("s-1"), split("s-2"), split("s-3"), split("s-4"), split("s-5")));

        List<ElasticsearchSourceSplit> out =
                ElasticsearchSourceSplitEnumerator.interleaveByBucket(byNode);
        List<List<String>> perReader = roundRobinIds(ids(out), 3);

        int max = perReader.stream().mapToInt(List::size).max().orElse(0);
        int min = perReader.stream().mapToInt(List::size).min().orElse(0);
        Assertions.assertTrue(max - min <= 1, "load spread too wide: " + perReader);
    }

    // ==================== ES enumeration / wan_only ====================

    @Test
    @SuppressWarnings("unchecked")
    public void runAttachesDataNodeAddressWhenWanOnlyFalse() throws Exception {
        SourceSplitEnumerator.Context<ElasticsearchSourceSplit> ctx =
                (SourceSplitEnumerator.Context<ElasticsearchSourceSplit>)
                        mock(SourceSplitEnumerator.Context.class);
        when(ctx.currentParallelism()).thenReturn(1);
        when(ctx.registeredReaders()).thenReturn(new HashSet<>(Collections.singletonList(0)));

        EsRestClient client = mock(EsRestClient.class);
        when(client.getNodesHttpAddresses())
                .thenReturn(Collections.singletonMap("node-1", "10.0.0.1:9200"));
        when(client.getIndexDocsCount("idx-*"))
                .thenReturn(Collections.singletonList(indexDocsCount("idx-a", 10L)));
        when(client.getSearchShards("idx-a"))
                .thenReturn(Collections.singletonList(new ShardInfo("idx-a", 0, "node-1")));

        ElasticsearchSourceSplitEnumerator enumerator =
                new ElasticsearchSourceSplitEnumerator(
                        ctx,
                        ReadonlyConfig.fromMap(Collections.emptyMap()),
                        Collections.singletonList(sourceConfig("idx-*")));
        injectClient(enumerator, client);

        enumerator.run();

        ArgumentCaptor<List<ElasticsearchSourceSplit>> captor =
                ArgumentCaptor.forClass(List.class);
        verify(ctx).assignSplit(eq(0), captor.capture());
        ElasticsearchSourceSplit split = captor.getValue().get(0);
        Assertions.assertEquals("idx-a", split.getConcreteIndex());
        Assertions.assertEquals("node-1", split.getNodeId());
        Assertions.assertEquals("10.0.0.1:9200", split.getDataNodeAddress());
        Assertions.assertEquals("_shards:0|_prefer_nodes:node-1", split.getPreference());
        verify(ctx).signalNoMoreSplits(0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void runSkipsDataNodeAddressLookupWhenWanOnlyTrue() throws Exception {
        SourceSplitEnumerator.Context<ElasticsearchSourceSplit> ctx =
                (SourceSplitEnumerator.Context<ElasticsearchSourceSplit>)
                        mock(SourceSplitEnumerator.Context.class);
        when(ctx.currentParallelism()).thenReturn(1);
        when(ctx.registeredReaders()).thenReturn(new HashSet<>(Collections.singletonList(0)));

        EsRestClient client = mock(EsRestClient.class);
        when(client.getIndexDocsCount("idx-*"))
                .thenReturn(Collections.singletonList(indexDocsCount("idx-a", 10L)));
        when(client.getSearchShards("idx-a"))
                .thenReturn(Collections.singletonList(new ShardInfo("idx-a", 0, "node-1")));

        Map<String, Object> config = new HashMap<>();
        config.put("wan_only", true);
        ElasticsearchSourceSplitEnumerator enumerator =
                new ElasticsearchSourceSplitEnumerator(
                        ctx,
                        ReadonlyConfig.fromMap(config),
                        Collections.singletonList(sourceConfig("idx-*")));
        injectClient(enumerator, client);

        enumerator.run();

        ArgumentCaptor<List<ElasticsearchSourceSplit>> captor =
                ArgumentCaptor.forClass(List.class);
        verify(ctx).assignSplit(eq(0), captor.capture());
        ElasticsearchSourceSplit split = captor.getValue().get(0);
        Assertions.assertEquals("node-1", split.getNodeId());
        Assertions.assertNull(split.getDataNodeAddress());
        verify(client, never()).getNodesHttpAddresses();
        verify(ctx).signalNoMoreSplits(0);
    }

    // ==================== snapshotState isolation ====================

    /**
     * Regression guard for the deep-copy invariant in {@link
     * ElasticsearchSourceSplitEnumerator#snapshotState(long)}: a snapshot must not
     * alias the live {@code pendingSplit} inner lists. If someone ever collapses
     * the deep copy back to {@code new HashMap<>(pendingSplit)}, this test fails.
     */
    @Test
    public void snapshotStateIsolatedFromLaterPendingMutation() throws Exception {
        @SuppressWarnings("unchecked")
        SourceSplitEnumerator.Context<ElasticsearchSourceSplit> ctx =
                (SourceSplitEnumerator.Context<ElasticsearchSourceSplit>)
                        mock(SourceSplitEnumerator.Context.class);
        when(ctx.currentParallelism()).thenReturn(4);
        when(ctx.registeredReaders()).thenReturn(new HashSet<>());

        ElasticsearchSourceSplitEnumerator enumerator =
                new ElasticsearchSourceSplitEnumerator(
                        ctx,
                        ReadonlyConfig.fromMap(Collections.emptyMap()),
                        Collections.emptyList());

        // Seed live pendingSplit directly via reflection (private). We can't use
        // addSplitsBack because its drain-and-dispatch path would empty pendingSplit
        // before we could snapshot it.
        Field pendingField =
                ElasticsearchSourceSplitEnumerator.class.getDeclaredField("pendingSplit");
        pendingField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Integer, List<ElasticsearchSourceSplit>> livePending =
                (Map<Integer, List<ElasticsearchSourceSplit>>) pendingField.get(enumerator);
        livePending.computeIfAbsent(0, k -> new ArrayList<>()).add(split("A"));

        ElasticsearchSourceState snap = enumerator.snapshotState(1L);
        Assertions.assertEquals(1, snap.getPendingSplit().get(0).size());

        // Mutate the live inner list directly. A shallow snapshot would pick this up.
        livePending.get(0).add(split("B"));

        Assertions.assertEquals(
                1,
                snap.getPendingSplit().get(0).size(),
                "snapshot must be decoupled from live pendingSplit inner lists");
        Assertions.assertEquals(
                "A", snap.getPendingSplit().get(0).get(0).getConcreteIndex());
    }

    // ==================== Split preference assembly ====================

    /**
     * Unassigned-primary defense: _search_shards on an unassigned shard returns no
     * {@code node} field. Jackson's MissingNode.asText() yields "" (not null), so
     * without the normalization in Split the preference would become
     * "_shards:0|_prefer_nodes:" (no value) and ES rejects it with HTTP 400.
     */
    @Test
    public void splitNormalizesEmptyNodeIdAndOmitsPreferNodes() {
        ElasticsearchSourceSplit split =
                new ElasticsearchSourceSplit(
                        new SourceConfig(),
                        /* concreteIndex */ "idx",
                        /* shardId */ 0,
                        /* nodeId */ "",
                        /* dataNodeAddress */ null);
        Assertions.assertNull(split.getNodeId());
        Assertions.assertEquals("_shards:0", split.getPreference());
    }

    @Test
    public void splitAppendsPreferNodesWhenNodeIdPresent() {
        ElasticsearchSourceSplit split =
                new ElasticsearchSourceSplit(
                        new SourceConfig(),
                        /* concreteIndex */ "idx",
                        /* shardId */ 3,
                        /* nodeId */ "node-abc",
                        /* dataNodeAddress */ null);
        Assertions.assertEquals("node-abc", split.getNodeId());
        Assertions.assertEquals("_shards:3|_prefer_nodes:node-abc", split.getPreference());
    }

    // ==================== restore across parallelism change ====================

    /**
     * Scale-down restore: old parallelism=8, new=4. Splits from old owners 4..7 must
     * land on valid readers (0..3), not stay orphaned in dead queues. We don't check
     * exact mapping — only that (a) no owner ≥ current parallelism remains, (b) no
     * splits are lost.
     */
    @Test
    public void restoreScaleDownDoesNotStrandSplits() {
        Map<Integer, List<ElasticsearchSourceSplit>> oldState = new HashMap<>();
        for (int oldOwner = 0; oldOwner < 8; oldOwner++) {
            List<ElasticsearchSourceSplit> queue = new ArrayList<>();
            for (int j = 0; j < 3; j++) {
                queue.add(splitOn("node-" + (oldOwner % 3), "s-" + oldOwner + "-" + j));
            }
            oldState.put(oldOwner, queue);
        }
        ElasticsearchSourceState savepoint =
                new ElasticsearchSourceState(/* shouldEnumerate */ false, oldState);

        int newParallelism = 4;
        Map<Integer, List<ElasticsearchSourceSplit>> restored =
                restoreAndReadPending(savepoint, newParallelism);

        int totalRestored =
                restored.values().stream().mapToInt(List::size).sum();
        Assertions.assertEquals(8 * 3, totalRestored, "no split may be lost on scale-down");
        for (Integer owner : restored.keySet()) {
            Assertions.assertTrue(
                    owner < newParallelism,
                    "owner " + owner + " ≥ parallelism " + newParallelism);
        }
    }

    /**
     * Scale-up restore: old parallelism=2, new=6. Without rebalancing, readers 2..5
     * would sit idle while readers 0..1 drained their (old) queues. With the restore
     * rebalance, all readers should get some work when there are enough splits.
     */
    @Test
    public void restoreScaleUpRebalancesAcrossNewReaders() {
        Map<Integer, List<ElasticsearchSourceSplit>> oldState = new HashMap<>();
        for (int oldOwner = 0; oldOwner < 2; oldOwner++) {
            List<ElasticsearchSourceSplit> queue = new ArrayList<>();
            for (int j = 0; j < 6; j++) {
                queue.add(splitOn("node-" + (j % 3), "s-" + oldOwner + "-" + j));
            }
            oldState.put(oldOwner, queue);
        }
        ElasticsearchSourceState savepoint =
                new ElasticsearchSourceState(/* shouldEnumerate */ false, oldState);

        int newParallelism = 6;
        Map<Integer, List<ElasticsearchSourceSplit>> restored =
                restoreAndReadPending(savepoint, newParallelism);

        int totalRestored =
                restored.values().stream().mapToInt(List::size).sum();
        Assertions.assertEquals(2 * 6, totalRestored, "no split may be lost on scale-up");
        Assertions.assertEquals(
                newParallelism, restored.size(), "every new reader should receive some work");
    }

    @SuppressWarnings("unchecked")
    private Map<Integer, List<ElasticsearchSourceSplit>> restoreAndReadPending(
            ElasticsearchSourceState savepoint, int newParallelism) {
        SourceSplitEnumerator.Context<ElasticsearchSourceSplit> ctx =
                (SourceSplitEnumerator.Context<ElasticsearchSourceSplit>)
                        mock(SourceSplitEnumerator.Context.class);
        when(ctx.currentParallelism()).thenReturn(newParallelism);
        when(ctx.registeredReaders()).thenReturn(new HashSet<>());

        ElasticsearchSourceSplitEnumerator enumerator =
                new ElasticsearchSourceSplitEnumerator(
                        ctx,
                        savepoint,
                        ReadonlyConfig.fromMap(Collections.emptyMap()),
                        Collections.emptyList());
        try {
            Field f =
                    ElasticsearchSourceSplitEnumerator.class.getDeclaredField("pendingSplit");
            f.setAccessible(true);
            return (Map<Integer, List<ElasticsearchSourceSplit>>) f.get(enumerator);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    // ==================== dispatch retry / rollback ====================

    /**
     * Dispatch must retry on transient assignSplit failures instead of leaving
     * splits orphaned. First assignSplit call throws, second succeeds — the
     * overall addSplitsBack (which drains and dispatches) completes cleanly.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void dispatchRetriesTransientAssignSplitFailure() {
        SourceSplitEnumerator.Context<ElasticsearchSourceSplit> ctx =
                (SourceSplitEnumerator.Context<ElasticsearchSourceSplit>)
                        mock(SourceSplitEnumerator.Context.class);
        when(ctx.currentParallelism()).thenReturn(1);
        when(ctx.registeredReaders()).thenReturn(new HashSet<>(Collections.singletonList(0)));
        // First call throws, second succeeds.
        doThrow(new RuntimeException("transient"))
                .doNothing()
                .when(ctx)
                .assignSplit(anyInt(), anyList());

        ElasticsearchSourceSplitEnumerator enumerator =
                new ElasticsearchSourceSplitEnumerator(
                        ctx,
                        ReadonlyConfig.fromMap(Collections.emptyMap()),
                        Collections.emptyList());

        enumerator.addSplitsBack(Collections.singletonList(split("A")), 0);

        // Two attempts: one failure, one success.
        verify(ctx, times(2)).assignSplit(eq(0), anyList());
    }

    /**
     * addSplitsBack must NOT throw when dispatch exhausts its retry budget —
     * the returning reader is typically mid-restart and will registerReader
     * shortly. Rolling the splits back into pendingSplit lets the later
     * registerReader pick them up. If this test regresses to throw-on-exhaust,
     * a crashed reader whose restart takes &gt;1.5s would cause the job to
     * enter an infinite restart loop.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void addSplitsBackRollsBackWhenDispatchExhausted() throws Exception {
        SourceSplitEnumerator.Context<ElasticsearchSourceSplit> ctx =
                (SourceSplitEnumerator.Context<ElasticsearchSourceSplit>)
                        mock(SourceSplitEnumerator.Context.class);
        when(ctx.currentParallelism()).thenReturn(1);
        when(ctx.registeredReaders()).thenReturn(new HashSet<>(Collections.singletonList(0)));
        doThrow(new RuntimeException("persistent"))
                .when(ctx)
                .assignSplit(anyInt(), anyList());

        ElasticsearchSourceSplitEnumerator enumerator =
                new ElasticsearchSourceSplitEnumerator(
                        ctx,
                        ReadonlyConfig.fromMap(Collections.emptyMap()),
                        Collections.emptyList());

        // addSplitsBack must succeed (no throw) — failure is absorbed and
        // splits land in pendingSplit awaiting the next registerReader.
        enumerator.addSplitsBack(Collections.singletonList(split("A")), 0);

        // Retry budget used.
        verify(ctx, times(3)).assignSplit(eq(0), anyList());

        // Split must be back in pendingSplit[0] for registerReader to drain.
        Field f = ElasticsearchSourceSplitEnumerator.class.getDeclaredField("pendingSplit");
        f.setAccessible(true);
        Map<Integer, List<ElasticsearchSourceSplit>> pending =
                (Map<Integer, List<ElasticsearchSourceSplit>>) f.get(enumerator);
        Assertions.assertNotNull(pending.get(0));
        Assertions.assertEquals(1, pending.get(0).size());
        Assertions.assertEquals("A", pending.get(0).get(0).getConcreteIndex());
    }

    /**
     * run() MUST throw when dispatch exhausts its retry budget. The path is
     * followed by {@code signalNoMoreSplits} — rolling back would silently
     * block the reader (told to stop waiting while splits remain pending).
     * Locking this invariant so a future refactor that "unifies" failure
     * handling across paths can't silently break run().
     */
    @Test
    @SuppressWarnings("unchecked")
    public void runThrowsWhenDispatchExhausted() throws Exception {
        SourceSplitEnumerator.Context<ElasticsearchSourceSplit> ctx =
                (SourceSplitEnumerator.Context<ElasticsearchSourceSplit>)
                        mock(SourceSplitEnumerator.Context.class);
        when(ctx.currentParallelism()).thenReturn(1);
        when(ctx.registeredReaders()).thenReturn(new HashSet<>(Collections.singletonList(0)));
        doThrow(new RuntimeException("persistent"))
                .when(ctx)
                .assignSplit(anyInt(), anyList());

        ElasticsearchSourceSplitEnumerator enumerator =
                new ElasticsearchSourceSplitEnumerator(
                        ctx,
                        ReadonlyConfig.fromMap(Collections.emptyMap()),
                        Collections.emptyList());

        // Seed state so run() skips HTTP enumeration and goes straight to
        // drain/dispatch of reader 0's pending splits.
        seedPendingForReader(enumerator, 0, split("A"));

        Assertions.assertThrows(
                ElasticsearchConnectorException.class, enumerator::run);

        // Retry budget fully used.
        verify(ctx, times(3)).assignSplit(eq(0), anyList());
        // signalNoMoreSplits MUST NOT be called when dispatch failed.
        verify(ctx, org.mockito.Mockito.never()).signalNoMoreSplits(anyInt());
    }

    /**
     * registerReader MUST throw when dispatch exhausts. A reader may not trigger
     * another registerReader/handleSplitRequest cycle after it has already registered,
     * so rolling back without failing the job can leave pending splits stranded forever.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void registerReaderThrowsWhenDispatchExhausted() throws Exception {
        SourceSplitEnumerator.Context<ElasticsearchSourceSplit> ctx =
                (SourceSplitEnumerator.Context<ElasticsearchSourceSplit>)
                        mock(SourceSplitEnumerator.Context.class);
        when(ctx.currentParallelism()).thenReturn(1);
        doThrow(new RuntimeException("persistent"))
                .when(ctx)
                .assignSplit(anyInt(), anyList());

        ElasticsearchSourceSplitEnumerator enumerator =
                new ElasticsearchSourceSplitEnumerator(
                        ctx,
                        ReadonlyConfig.fromMap(Collections.emptyMap()),
                        Collections.emptyList());
        seedPendingForReader(enumerator, 0, split("A"));

        Assertions.assertThrows(
                ElasticsearchConnectorException.class,
                () -> enumerator.registerReader(0));

        verify(ctx, times(3)).assignSplit(eq(0), anyList());
        // signalNoMoreSplits MUST NOT fire when dispatch failed.
        verify(ctx, org.mockito.Mockito.never()).signalNoMoreSplits(anyInt());

        // Rolled back before throwing so a restart/checkpoint restore can keep the split.
        Field f = ElasticsearchSourceSplitEnumerator.class.getDeclaredField("pendingSplit");
        f.setAccessible(true);
        Map<Integer, List<ElasticsearchSourceSplit>> pending =
                (Map<Integer, List<ElasticsearchSourceSplit>>) f.get(enumerator);
        Assertions.assertNotNull(pending.get(0));
        Assertions.assertEquals(1, pending.get(0).size());
        Assertions.assertEquals("A", pending.get(0).get(0).getConcreteIndex());
    }

    /**
     * Set the enumerator into "enumeration complete" state and put one split
     * into pendingSplit for the given reader. Used by tests that want to drive
     * drain/dispatch paths without going through the HTTP-bound
     * {@code getElasticsearchSplit}.
     */
    @SuppressWarnings("unchecked")
    private static void seedPendingForReader(
            ElasticsearchSourceSplitEnumerator enumerator, int reader, ElasticsearchSourceSplit s)
            throws Exception {
        Field shouldEnumerateField =
                ElasticsearchSourceSplitEnumerator.class.getDeclaredField("shouldEnumerate");
        shouldEnumerateField.setAccessible(true);
        shouldEnumerateField.setBoolean(enumerator, false);

        Field pendingField =
                ElasticsearchSourceSplitEnumerator.class.getDeclaredField("pendingSplit");
        pendingField.setAccessible(true);
        Map<Integer, List<ElasticsearchSourceSplit>> pending =
                (Map<Integer, List<ElasticsearchSourceSplit>>) pendingField.get(enumerator);
        pending.computeIfAbsent(reader, k -> new ArrayList<>()).add(s);
    }

    private static ElasticsearchSourceSplit splitOn(String dataNodeAddress, String id) {
        // Use the synthetic test id as concreteIndex so each split is unique and
        // identifiable via getConcreteIndex() — the actual splitId is computed
        // by the constructor as "<id>-shard0".
        return new ElasticsearchSourceSplit(
                new SourceConfig(),
                /* concreteIndex */ id,
                /* shardId */ 0,
                /* nodeId */ null,
                /* dataNodeAddress */ dataNodeAddress);
    }

    private static SourceConfig sourceConfig(String index) {
        SourceConfig config = new SourceConfig();
        config.setIndex(index);
        return config;
    }

    private static IndexDocsCount indexDocsCount(String index, Long docsCount) {
        IndexDocsCount info = new IndexDocsCount();
        info.setIndex(index);
        info.setDocsCount(docsCount);
        return info;
    }

    private static void injectClient(
            ElasticsearchSourceSplitEnumerator enumerator, EsRestClient client)
            throws Exception {
        Field clientField =
                ElasticsearchSourceSplitEnumerator.class.getDeclaredField("esRestClient");
        clientField.setAccessible(true);
        clientField.set(enumerator, client);
    }

    // ==================== helpers ====================

    private static List<List<String>> roundRobinIds(List<String> flat, int numReaders) {
        List<List<String>> perReader = new ArrayList<>(numReaders);
        for (int i = 0; i < numReaders; i++) {
            perReader.add(new ArrayList<>());
        }
        for (int i = 0; i < flat.size(); i++) {
            perReader.get(i % numReaders).add(flat.get(i));
        }
        return perReader;
    }

    private static List<String> firstLetters(List<List<String>> perReader) {
        List<String> out = new ArrayList<>();
        for (List<String> reader : perReader) {
            if (reader.isEmpty()) {
                out.add("");
            } else {
                out.add(reader.get(0).substring(0, 1));
            }
        }
        return out;
    }

    private static ElasticsearchSourceSplit split(String id) {
        // Use the synthetic test id as concreteIndex so each split is unique
        // and identifiable via getConcreteIndex() — the actual splitId is
        // computed by the constructor as "<id>-shard0".
        return new ElasticsearchSourceSplit(
                new SourceConfig(),
                /* concreteIndex */ id,
                /* shardId */ 0,
                /* nodeId */ null,
                /* dataNodeAddress */ null);
    }

    private static List<String> ids(List<ElasticsearchSourceSplit> splits) {
        List<String> out = new ArrayList<>(splits.size());
        for (ElasticsearchSourceSplit s : splits) {
            // Tests use concreteIndex as the synthetic identifier.
            out.add(s.getConcreteIndex());
        }
        return out;
    }
}
