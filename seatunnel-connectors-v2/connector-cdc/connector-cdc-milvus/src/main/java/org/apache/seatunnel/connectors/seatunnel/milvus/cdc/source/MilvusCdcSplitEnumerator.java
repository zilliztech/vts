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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.offset.MilvusCdcOffset;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class MilvusCdcSplitEnumerator
        implements SourceSplitEnumerator<MilvusCdcSplit, MilvusCdcSourceState> {

    private final SourceSplitEnumerator.Context<MilvusCdcSplit> context;
    private final Map<Integer, List<MilvusCdcSplit>> pendingSplits;
    private final Object stateLock = new Object();
    private boolean started;

    public MilvusCdcSplitEnumerator(
            SourceSplitEnumerator.Context<MilvusCdcSplit> context,
            ReadonlyConfig config,
            MilvusCdcSourceState checkpointState) {
        this.context = context;
        this.pendingSplits = new HashMap<>();
        if (checkpointState == null) {
            addPendingSplits(MilvusCdcSourceConfigParser.parseChannelPositions(config));
        } else {
            addPendingSplits(flattenPendingSplits(checkpointState.getPendingSplits()));
        }
    }

    @Override
    public void open() {}

    @Override
    public void run() {
        synchronized (context) {
            synchronized (stateLock) {
                started = true;
                assignSplits(context.registeredReaders());
            }
        }
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void addSplitsBack(List<MilvusCdcSplit> splits, int subtaskId) {
        synchronized (context) {
            synchronized (stateLock) {
                addPendingSplits(splits, subtaskId);
            }
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        synchronized (stateLock) {
            return pendingSplits.values().stream().mapToInt(List::size).sum();
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        synchronized (context) {
            synchronized (stateLock) {
                if (started) {
                    assignSplits(Collections.singletonList(subtaskId));
                }
            }
        }
    }

    @Override
    public void registerReader(int subtaskId) {
        synchronized (context) {
            synchronized (stateLock) {
                if (started) {
                    assignSplits(Collections.singletonList(subtaskId));
                }
            }
        }
    }

    @Override
    public MilvusCdcSourceState snapshotState(long checkpointId) {
        synchronized (context) {
            synchronized (stateLock) {
                return new MilvusCdcSourceState(copyPendingSplits(pendingSplits));
            }
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    private void assignSplits(Collection<Integer> readers) {
        if (pendingSplits.isEmpty() || readers == null || readers.isEmpty()) {
            return;
        }
        for (int reader : readers) {
            if (!context.registeredReaders().contains(reader)) {
                log.warn(
                        "Reader {} is not registered. Pending Milvus CDC splits are not assigned.",
                        reader);
                continue;
            }
            List<MilvusCdcSplit> assignment = pendingSplits.remove(reader);
            if (assignment != null && !assignment.isEmpty()) {
                log.info("Assign Milvus CDC splits {} to reader {}.", assignment, reader);
                List<MilvusCdcSplit> assignmentCopies = copySplits(assignment);
                context.assignSplit(reader, assignmentCopies);
            }
        }
    }

    private void addPendingSplits(List<MilvusCdcSplit> splits) {
        int readerCount = Math.max(1, context.currentParallelism());
        for (int i = 0; i < splits.size(); i++) {
            addPendingSplit(splits.get(i), i % readerCount);
        }
    }

    private void addPendingSplits(List<MilvusCdcSplit> splits, int ownerReader) {
        for (MilvusCdcSplit split : splits) {
            addPendingSplit(split, ownerReader);
        }
    }

    private void addPendingSplit(MilvusCdcSplit split, int ownerReader) {
        MilvusCdcSplit splitCopy = copySplit(split);
        pendingSplits.computeIfAbsent(ownerReader, ignored -> new ArrayList<>()).add(splitCopy);
    }

    private static Map<Integer, List<MilvusCdcSplit>> copyPendingSplits(
            Map<Integer, List<MilvusCdcSplit>> pendingSplits) {
        Map<Integer, List<MilvusCdcSplit>> copies = new HashMap<>();
        if (pendingSplits == null) {
            return copies;
        }
        for (Map.Entry<Integer, List<MilvusCdcSplit>> entry : pendingSplits.entrySet()) {
            copies.put(entry.getKey(), copySplits(entry.getValue()));
        }
        return copies;
    }

    private static List<MilvusCdcSplit> flattenPendingSplits(
            Map<Integer, List<MilvusCdcSplit>> pendingSplits) {
        List<MilvusCdcSplit> splits = new ArrayList<>();
        if (pendingSplits == null) {
            return splits;
        }
        List<Integer> subtasks = new ArrayList<>(pendingSplits.keySet());
        Collections.sort(subtasks);
        for (Integer subtask : subtasks) {
            splits.addAll(copySplits(pendingSplits.get(subtask)));
        }
        return splits;
    }

    private static List<MilvusCdcSplit> copySplits(List<MilvusCdcSplit> splits) {
        List<MilvusCdcSplit> copies = new ArrayList<>();
        if (splits == null) {
            return copies;
        }
        for (MilvusCdcSplit split : splits) {
            copies.add(copySplit(split));
        }
        return copies;
    }

    static MilvusCdcSplit copySplit(MilvusCdcSplit split) {
        return MilvusCdcSplit.builder()
                .splitId(split.getSplitId())
                .pchannel(split.getPchannel())
                .startOffset(copyOffset(split.getStartOffset()))
                .currentOffset(copyOffset(split.getCurrentOffset()))
                .build();
    }

    static MilvusCdcOffset copyOffset(MilvusCdcOffset offset) {
        if (offset == null) {
            return null;
        }
        return MilvusCdcOffset.builder()
                .walName(offset.getWalName())
                .resumeMessageId(offset.getResumeMessageId())
                .consumedMessageId(offset.getConsumedMessageId())
                .timetick(offset.getTimetick())
                .build();
    }
}
