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

package org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class MilvusCdcSplitEnumeratorTest {

    @Test
    void assignSplitsRoundRobinWhenSplitCountExceedsParallelism() throws Exception {
        TestEnumeratorContext context = new TestEnumeratorContext(2, 0, 1);
        MilvusCdcSplitEnumerator enumerator = enumerator(context, splits(5));

        enumerator.run();

        Assertions.assertEquals(
                Arrays.asList("pchannel-0", "pchannel-2", "pchannel-4"),
                splitIds(context.assigned(0)));
        Assertions.assertEquals(
                Arrays.asList("pchannel-1", "pchannel-3"), splitIds(context.assigned(1)));
        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void keepSplitsPendingUntilEnumeratorRuns() {
        TestEnumeratorContext context = new TestEnumeratorContext(3, 0);
        MilvusCdcSplitEnumerator enumerator = enumerator(context, splits(6));

        enumerator.registerReader(0);

        Assertions.assertTrue(context.assigned(0).isEmpty());
        Assertions.assertEquals(6, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void distributeToReadersRegisteredBeforeRun() throws Exception {
        TestEnumeratorContext context = new TestEnumeratorContext(3, 0, 1, 2);
        MilvusCdcSplitEnumerator enumerator = enumerator(context, splits(6));

        enumerator.run();

        Assertions.assertEquals(
                Arrays.asList("pchannel-0", "pchannel-3"), splitIds(context.assigned(0)));
        Assertions.assertEquals(
                Arrays.asList("pchannel-1", "pchannel-4"), splitIds(context.assigned(1)));
        Assertions.assertEquals(
                Arrays.asList("pchannel-2", "pchannel-5"), splitIds(context.assigned(2)));
        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void restoreRedistributesPendingSplits() throws Exception {
        MilvusCdcSourceState checkpointState =
                new MilvusCdcSourceState(pendingSplits(splits(5), 2));
        TestEnumeratorContext restoredContext = new TestEnumeratorContext(2, 0, 1);
        MilvusCdcSplitEnumerator restoredEnumerator = enumerator(restoredContext, checkpointState);

        restoredEnumerator.run();

        Assertions.assertEquals(
                Arrays.asList("pchannel-0", "pchannel-4", "pchannel-3"),
                splitIds(restoredContext.assigned(0)));
        Assertions.assertEquals(
                Arrays.asList("pchannel-2", "pchannel-1"), splitIds(restoredContext.assigned(1)));
        Assertions.assertEquals(0, restoredEnumerator.currentUnassignedSplitSize());
    }

    @Test
    void restoreRedistributesPendingSplitsAfterParallelismShrinks() throws Exception {
        MilvusCdcSourceState checkpointState =
                new MilvusCdcSourceState(pendingSplits(splits(6), 3));
        TestEnumeratorContext restoredContext = new TestEnumeratorContext(2, 0, 1);
        MilvusCdcSplitEnumerator restoredEnumerator = enumerator(restoredContext, checkpointState);

        restoredEnumerator.run();

        Assertions.assertEquals(
                Arrays.asList("pchannel-0", "pchannel-1", "pchannel-2"),
                splitIds(restoredContext.assigned(0)));
        Assertions.assertEquals(
                Arrays.asList("pchannel-3", "pchannel-4", "pchannel-5"),
                splitIds(restoredContext.assigned(1)));
        Assertions.assertEquals(0, restoredEnumerator.currentUnassignedSplitSize());
    }

    @Test
    void lateReaderReceivesOnlyItsOwnedPendingSplits() throws Exception {
        TestEnumeratorContext context = new TestEnumeratorContext(3, 0, 1);
        MilvusCdcSplitEnumerator enumerator = enumerator(context, splits(3));

        enumerator.run();

        Assertions.assertEquals(Arrays.asList("pchannel-0"), splitIds(context.assigned(0)));
        Assertions.assertEquals(Arrays.asList("pchannel-1"), splitIds(context.assigned(1)));
        Assertions.assertEquals(1, enumerator.currentUnassignedSplitSize());

        context.registerReader(2);
        enumerator.registerReader(2);

        Assertions.assertEquals(Arrays.asList("pchannel-2"), splitIds(context.assigned(2)));
        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void splitRequestBeforeRegisterDoesNotDropPendingSplits() throws Exception {
        TestEnumeratorContext context = new TestEnumeratorContext(2);
        MilvusCdcSplitEnumerator enumerator = enumerator(context, splits(2));

        enumerator.run();
        enumerator.handleSplitRequest(0);

        Assertions.assertTrue(context.assigned(0).isEmpty());
        Assertions.assertEquals(2, enumerator.currentUnassignedSplitSize());

        context.registerReader(0);
        enumerator.registerReader(0);

        Assertions.assertEquals(Arrays.asList("pchannel-0"), splitIds(context.assigned(0)));
        Assertions.assertEquals(1, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void returnedSplitsWaitForOriginalSubtask() throws Exception {
        TestEnumeratorContext context = new TestEnumeratorContext(2, 0, 1);
        MilvusCdcSplitEnumerator enumerator = enumerator(context, splits(4));
        enumerator.run();
        List<MilvusCdcSplit> readerZeroSplits = new ArrayList<>(context.assigned(0));
        context.clearAssignments();

        enumerator.addSplitsBack(readerZeroSplits, 0);
        enumerator.registerReader(1);

        Assertions.assertTrue(context.assigned(1).isEmpty());
        Assertions.assertEquals(2, enumerator.currentUnassignedSplitSize());

        enumerator.registerReader(0);

        Assertions.assertEquals(
                Arrays.asList("pchannel-0", "pchannel-2"), splitIds(context.assigned(0)));
        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void snapshotIncludesReturnedSplits() throws Exception {
        TestEnumeratorContext context = new TestEnumeratorContext(2, 0, 1);
        MilvusCdcSplitEnumerator enumerator = enumerator(context, splits(4));
        enumerator.run();

        enumerator.addSplitsBack(context.assigned(0), 0);
        MilvusCdcSourceState checkpointState = enumerator.snapshotState(1L);

        Assertions.assertEquals(
                Arrays.asList("pchannel-0", "pchannel-2"),
                splitIds(checkpointState.getPendingSplits().get(0)));
    }

    @Test
    void assignmentWaitsForCheckpointContextMonitor() throws Exception {
        TestEnumeratorContext context = new TestEnumeratorContext(1);
        MilvusCdcSplitEnumerator enumerator = enumerator(context, splits(1));
        enumerator.run();
        context.registerReader(0);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch assignmentAttempted = new CountDownLatch(1);

        try {
            Future<?> assignment;
            synchronized (context) {
                assignment =
                        executor.submit(
                                () -> {
                                    assignmentAttempted.countDown();
                                    enumerator.registerReader(0);
                                });
                Assertions.assertTrue(assignmentAttempted.await(5, TimeUnit.SECONDS));
                Assertions.assertFalse(
                        context.assignmentCompleted.await(200, TimeUnit.MILLISECONDS));
            }

            assignment.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    Collections.singletonList("pchannel-0"), splitIds(context.assigned(0)));
        } finally {
            executor.shutdownNow();
        }
    }

    private MilvusCdcSplitEnumerator enumerator(
            TestEnumeratorContext context, List<MilvusCdcSplit> splits) {
        return new MilvusCdcSplitEnumerator(context, config(splits), null);
    }

    private MilvusCdcSplitEnumerator enumerator(
            TestEnumeratorContext context, MilvusCdcSourceState checkpointState) {
        return new MilvusCdcSplitEnumerator(
                context, ReadonlyConfig.fromMap(Collections.emptyMap()), checkpointState);
    }

    private ReadonlyConfig config(List<MilvusCdcSplit> splits) {
        List<Map<String, Object>> channelPositions = new ArrayList<>();
        for (int i = 0; i < splits.size(); i++) {
            Map<String, Object> start = new HashMap<>();
            start.put("resume_message_id", "message-" + i);
            start.put("wal_name", "Pulsar");
            start.put("timetick", (long) i);

            Map<String, Object> channelPosition = new HashMap<>();
            channelPosition.put("pchannel", splits.get(i).getPchannel());
            channelPosition.put("start", start);
            channelPositions.add(channelPosition);
        }

        Map<String, Object> config = new HashMap<>();
        config.put("startup_mode", "cdc");
        config.put("channel_positions", channelPositions);
        return ReadonlyConfig.fromMap(config);
    }

    private List<MilvusCdcSplit> splits(int count) {
        List<MilvusCdcSplit> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String pchannel = "pchannel-" + i;
            splits.add(MilvusCdcSplit.builder().splitId(pchannel).pchannel(pchannel).build());
        }
        return splits;
    }

    private List<String> splitIds(List<MilvusCdcSplit> splits) {
        return splits.stream().map(MilvusCdcSplit::splitId).collect(Collectors.toList());
    }

    private Map<Integer, List<MilvusCdcSplit>> pendingSplits(
            List<MilvusCdcSplit> splits, int parallelism) {
        Map<Integer, List<MilvusCdcSplit>> pendingSplits = new HashMap<>();
        for (int i = 0; i < splits.size(); i++) {
            pendingSplits
                    .computeIfAbsent(i % parallelism, ignored -> new ArrayList<>())
                    .add(splits.get(i));
        }
        return pendingSplits;
    }

    private static class TestEnumeratorContext
            implements SourceSplitEnumerator.Context<MilvusCdcSplit> {
        private final int parallelism;
        private final Set<Integer> registeredReaders = new LinkedHashSet<>();
        private final Map<Integer, List<MilvusCdcSplit>> assignments = new HashMap<>();
        private final Map<Integer, List<SourceEvent>> events = new HashMap<>();
        private final CountDownLatch assignmentCompleted = new CountDownLatch(1);

        private TestEnumeratorContext(int parallelism, Integer... readers) {
            this.parallelism = parallelism;
            registeredReaders.addAll(Arrays.asList(readers));
        }

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return registeredReaders;
        }

        @Override
        public void assignSplit(int subtaskId, List<MilvusCdcSplit> splits) {
            assignments.computeIfAbsent(subtaskId, ignored -> new ArrayList<>()).addAll(splits);
            assignmentCompleted.countDown();
        }

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
            events.computeIfAbsent(subtaskId, ignored -> new ArrayList<>()).add(event);
        }

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }

        private List<MilvusCdcSplit> assigned(int subtaskId) {
            return assignments.getOrDefault(subtaskId, Collections.emptyList());
        }

        private void clearAssignments() {
            assignments.clear();
        }

        private List<SourceEvent> events(int subtaskId) {
            return events.getOrDefault(subtaskId, Collections.emptyList());
        }

        private void registerReader(int subtaskId) {
            registeredReaders.add(subtaskId);
        }
    }
}
