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

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.chromadb.client.ChromaDBClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Counts each collection once and assigns fixed ranges without waiting for readers to consume them.
 */
@Slf4j
public class ChromaDBSplitEnumerator
        implements SourceSplitEnumerator<ChromaDBSourceSplit, ChromaDBSourceState> {

    private final Context<ChromaDBSourceSplit> context;
    private final ReadonlyConfig config;
    private final Map<String, CatalogTable> collectionTables;
    private final Set<String> enumeratedCollectionIds;
    private final Map<String, ChromaDBSourceSplit> pendingSplits = new LinkedHashMap<>();
    private final Object stateLock = new Object();
    private ChromaDBClient client;

    public ChromaDBSplitEnumerator(
            Context<ChromaDBSourceSplit> context,
            ReadonlyConfig config,
            Map<String, CatalogTable> collectionTables,
            ChromaDBSourceState state) {
        this.context = context;
        this.config = config;
        this.collectionTables = collectionTables;
        this.enumeratedCollectionIds =
                state == null ? new HashSet<>() : new HashSet<>(state.getEnumeratedCollectionIds());
        if (state != null) {
            for (ChromaDBSourceSplit split : state.getPendingSplits()) {
                pendingSplits.put(split.splitId(), split.copy());
            }
        }
    }

    @Override
    public void open() {
        client = ChromaDBSource.createClient(config);
    }

    @Override
    public void run() throws Exception {
        int parallelism = context.currentParallelism();
        if (parallelism <= 0) {
            throw new IllegalArgumentException("Source parallelism must be positive");
        }
        for (Map.Entry<String, CatalogTable> entry : collectionTables.entrySet()) {
            String collectionId = entry.getKey();
            synchronized (stateLock) {
                if (enumeratedCollectionIds.contains(collectionId)) {
                    continue;
                }
            }
            long count = client.countRecords(collectionId);
            List<ChromaDBSourceSplit> splits = new ArrayList<>();
            for (int i = 0; i < parallelism; i++) {
                // Equivalent to floor(count * i / parallelism), without overflowing count * i.
                long start = (count / parallelism) * i + ((count % parallelism) * i) / parallelism;
                long end =
                        (count / parallelism) * (i + 1L)
                                + ((count % parallelism) * (i + 1L)) / parallelism;
                if (start < end) {
                    splits.add(
                            new ChromaDBSourceSplit(
                                    collectionId, entry.getValue().getTablePath(), start, end));
                }
            }
            synchronized (stateLock) {
                if (enumeratedCollectionIds.add(collectionId)) {
                    splits.forEach(split -> pendingSplits.put(split.splitId(), split));
                }
            }
            log.info(
                    "Counted collection '{}': {} records, {} ranges",
                    collectionId,
                    count,
                    splits.size());
        }
        synchronized (stateLock) {
            assignPendingSplits();
        }
    }

    /** Called under stateLock; late registration triggers assignment rather than blocking run(). */
    private void assignPendingSplits() {
        if (!enumeratedCollectionIds.containsAll(collectionTables.keySet())) {
            return;
        }
        List<Integer> readers = new ArrayList<>(context.registeredReaders());
        if (readers.isEmpty() || readers.size() < context.currentParallelism()) {
            return;
        }
        Collections.sort(readers);
        Map<Integer, List<ChromaDBSourceSplit>> assignments = new LinkedHashMap<>();
        int index = 0;
        for (ChromaDBSourceSplit split : pendingSplits.values()) {
            int reader = readers.get(index++ % readers.size());
            assignments.computeIfAbsent(reader, ignored -> new ArrayList<>()).add(split);
        }
        for (Map.Entry<Integer, List<ChromaDBSourceSplit>> assignment : assignments.entrySet()) {
            context.assignSplit(assignment.getKey(), assignment.getValue());
            // Assigned state belongs to the reader; the engine returns its checkpoint on recovery.
            assignment.getValue().forEach(split -> pendingSplits.remove(split.splitId()));
        }
        readers.forEach(context::signalNoMoreSplits);
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public void addSplitsBack(List<ChromaDBSourceSplit> splits, int subtaskId) {
        synchronized (stateLock) {
            splits.forEach(split -> pendingSplits.put(split.splitId(), split.copy()));
            assignPendingSplits();
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        synchronized (stateLock) {
            return pendingSplits.size();
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        synchronized (stateLock) {
            assignPendingSplits();
        }
    }

    @Override
    public void registerReader(int subtaskId) {
        synchronized (stateLock) {
            assignPendingSplits();
        }
    }

    @Override
    public ChromaDBSourceState snapshotState(long checkpointId) {
        synchronized (stateLock) {
            List<ChromaDBSourceSplit> splits = new ArrayList<>();
            pendingSplits.values().forEach(split -> splits.add(split.copy()));
            return new ChromaDBSourceState(new HashSet<>(enumeratedCollectionIds), splits);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Reader checkpoints own the progress of assigned ranges.
    }
}
