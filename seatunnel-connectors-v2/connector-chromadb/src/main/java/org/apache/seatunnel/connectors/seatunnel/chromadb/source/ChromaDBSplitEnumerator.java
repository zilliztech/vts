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
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class ChromaDBSplitEnumerator
        implements SourceSplitEnumerator<ChromaDBSourceSplit, ChromaDBSourceState> {

    private final Context<ChromaDBSourceSplit> context;
    private final Map<String, CatalogTable> collectionTables;
    private final ConcurrentLinkedQueue<ChromaDBSourceSplit> pendingSplits;
    private final Map<Integer, List<ChromaDBSourceSplit>> assignedSplits;
    private final Object stateLock = new Object();

    public ChromaDBSplitEnumerator(
            Context<ChromaDBSourceSplit> context,
            Map<String, CatalogTable> collectionTables,
            ChromaDBSourceState state) {
        this.context = context;
        this.collectionTables = collectionTables;

        if (state == null) {
            // Fresh start: create one split per collection
            this.pendingSplits = new ConcurrentLinkedQueue<>();
            for (Map.Entry<String, CatalogTable> entry : collectionTables.entrySet()) {
                pendingSplits.add(
                        new ChromaDBSourceSplit(entry.getKey(), entry.getValue().getTablePath()));
            }
            this.assignedSplits = new HashMap<>();
        } else {
            this.pendingSplits = new ConcurrentLinkedQueue<>(state.getPendingSplits());
            this.assignedSplits = new HashMap<>(state.getAssignedSplits());
        }
    }

    @Override
    public void open() {
        // No resources to open
    }

    @Override
    public void run() throws Exception {
        Set<Integer> readers = context.registeredReaders();
        log.info(
                "ChromaDB split enumerator started, {} splits for {} readers",
                pendingSplits.size(),
                readers.size());

        synchronized (stateLock) {
            assignSplits(readers);
        }

        log.info("All splits assigned. Signaling no more splits.");
        readers.forEach(context::signalNoMoreSplits);
    }

    private void assignSplits(Collection<Integer> readers) {
        if (readers.isEmpty()) {
            return;
        }
        // Round-robin assignment
        List<Integer> readerList = new ArrayList<>(readers);
        int idx = 0;
        while (!pendingSplits.isEmpty()) {
            ChromaDBSourceSplit split = pendingSplits.poll();
            if (split == null) {
                break;
            }
            int reader = readerList.get(idx % readerList.size());
            log.info("Assigning split '{}' to reader {}", split.getCollectionId(), reader);
            context.assignSplit(reader, Collections.singletonList(split));
            assignedSplits
                    .computeIfAbsent(reader, r -> new ArrayList<>())
                    .add(split);
            idx++;
        }
    }

    @Override
    public void close() throws IOException {
        // No resources to close
    }

    @Override
    public void addSplitsBack(List<ChromaDBSourceSplit> splits, int subtaskId) {
        synchronized (stateLock) {
            pendingSplits.addAll(splits);
            if (context.registeredReaders().contains(subtaskId)) {
                assignSplits(Collections.singletonList(subtaskId));
            }
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        // Not used
    }

    @Override
    public void registerReader(int subtaskId) {
        log.info("Reader {} registered", subtaskId);
        synchronized (stateLock) {
            if (!pendingSplits.isEmpty()) {
                assignSplits(Collections.singletonList(subtaskId));
            }
        }
    }

    @Override
    public ChromaDBSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            Map<Integer, List<ChromaDBSourceSplit>> deepCopy = new HashMap<>();
            for (Map.Entry<Integer, List<ChromaDBSourceSplit>> entry :
                    assignedSplits.entrySet()) {
                deepCopy.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }
            return new ChromaDBSourceState(new ArrayList<>(pendingSplits), deepCopy);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // No action needed
    }
}
