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
import org.apache.seatunnel.connectors.seatunnel.chromadb.config.ChromaDBSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.chromadb.exception.ChromaDBConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.chromadb.exception.ChromaDBConnectorException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Enumerator that performs Phase 1 (scan collection IDs via offset/limit) itself, partitions
 * each batch into N sub-splits (N = reader parallelism), and pushes them to readers with
 * backpressure coordination.
 *
 * <h2>Execution model</h2>
 *
 * <pre>
 *   for each collection:
 *       offset = 0
 *       while true:
 *           waitForAllReadersReady()        ← barrier: all readers drained their previous split
 *           ids = client.phase1(collection, offset, id_split_size)
 *           if ids empty: break to next collection
 *           parts = partition(ids, parallelism)
 *           for each part: assignSplit(reader, part)     (empty parts → reader marked ready)
 *           offset += ids.size()
 *           if ids.size() &lt; id_split_size: break  (last batch)
 *       mark collection completed
 *   signalNoMoreSplits(all readers)
 * </pre>
 *
 * <p>Backpressure: readers call {@code context.sendSplitRequest()} when their pending queue
 * empties. That RPC arrives in {@link #handleSplitRequest(int)}, which adds the reader to
 * {@code readyReaders} and notifies the run() loop.
 *
 * <h2>State semantics (v1)</h2>
 *
 * <p>Checkpoint granularity is per-collection. If a job fails mid-collection, the whole
 * collection is re-read on restart. This assumes an idempotent (upsert-by-ID) sink, which is
 * the case for Milvus.
 */
@Slf4j
public class ChromaDBSplitEnumerator
        implements SourceSplitEnumerator<ChromaDBSourceSplit, ChromaDBSourceState> {

    private final Context<ChromaDBSourceSplit> context;
    private final ReadonlyConfig config;
    private final Map<String, CatalogTable> collectionTables;

    /** Collections already fully processed (for checkpoint restore skipping). */
    private final Set<String> completedCollections;

    /** Splits that were assigned to readers but may need re-assignment (failure recovery). */
    private final Map<Integer, List<ChromaDBSourceSplit>> assignedSplits;

    /** Splits not yet assigned — from restored state, drained before the main loop starts. */
    private final ConcurrentLinkedQueue<ChromaDBSourceSplit> carriedOverSplits;

    /** Monotonic sequence number per collection, used to build unique splitIds. */
    private final Map<String, Long> collectionSplitSeq = new HashMap<>();

    /** Readers currently idle (empty pending queue). Guarded by {@link #readyLock}. */
    private final Set<Integer> readyReaders = new HashSet<>();

    private final Object readyLock = new Object();
    private final Object stateLock = new Object();

    private volatile boolean shuttingDown = false;

    private ChromaDBClient client;

    public ChromaDBSplitEnumerator(
            Context<ChromaDBSourceSplit> context,
            ReadonlyConfig config,
            Map<String, CatalogTable> collectionTables,
            ChromaDBSourceState state) {
        this.context = context;
        this.config = config;
        this.collectionTables = collectionTables;

        if (state == null) {
            this.completedCollections = new HashSet<>();
            this.assignedSplits = new HashMap<>();
            this.carriedOverSplits = new ConcurrentLinkedQueue<>();
        } else {
            this.completedCollections = new HashSet<>(state.getCompletedCollectionIds());
            this.assignedSplits = new HashMap<>(state.getAssignedSplits());
            this.carriedOverSplits = new ConcurrentLinkedQueue<>(state.getPendingSplits());
            // Re-queue any previously assigned splits; we don't know if the reader finished them.
            for (List<ChromaDBSourceSplit> splits : state.getAssignedSplits().values()) {
                carriedOverSplits.addAll(splits);
            }
            this.assignedSplits.clear();
        }
    }

    @Override
    public void open() {
        client = new ChromaDBClient(config);
    }

    @Override
    public void run() throws Exception {
        Set<Integer> readers = new LinkedHashSet<>(context.registeredReaders());
        if (readers.isEmpty()) {
            log.warn("No readers registered; nothing to do.");
            return;
        }

        // All readers start idle — they're awaiting their first split.
        synchronized (readyLock) {
            readyReaders.addAll(readers);
        }

        log.info(
                "ChromaDBSplitEnumerator starting. parallelism={}, collections={}, already_completed={}",
                readers.size(),
                collectionTables.keySet(),
                completedCollections);

        try {
            // Step A: drain any carried-over splits from a previous checkpoint.
            drainCarriedOverSplits(readers);

            // Step B: process remaining collections.
            int idSplitSize = config.get(ChromaDBSourceConfig.ID_SPLIT_SIZE);
            for (Map.Entry<String, CatalogTable> entry : collectionTables.entrySet()) {
                if (shuttingDown) {
                    return;
                }
                String collectionId = entry.getKey();
                if (completedCollections.contains(collectionId)) {
                    log.info(
                            "Skipping already-completed collection '{}' (from checkpoint)",
                            collectionId);
                    continue;
                }
                processCollection(collectionId, entry.getValue(), readers, idSplitSize);
                synchronized (stateLock) {
                    completedCollections.add(collectionId);
                }
            }
        } finally {
            log.info("All collections processed. Signaling no more splits.");
            readers.forEach(context::signalNoMoreSplits);
        }
    }

    private void drainCarriedOverSplits(Set<Integer> readers) throws InterruptedException {
        if (carriedOverSplits.isEmpty()) {
            return;
        }
        log.info("Draining {} carried-over splits from checkpoint", carriedOverSplits.size());
        List<Integer> readerList = new ArrayList<>(readers);
        int idx = 0;
        while (!carriedOverSplits.isEmpty() && !shuttingDown) {
            waitForAllReaders(readers);
            synchronized (readyLock) {
                readyReaders.clear();
            }
            // Assign up to one carried-over split per reader per round.
            for (int i = 0; i < readers.size() && !carriedOverSplits.isEmpty(); i++) {
                ChromaDBSourceSplit split = carriedOverSplits.poll();
                if (split == null) break;
                int readerId = readerList.get(idx % readerList.size());
                idx++;
                assignToReader(readerId, split);
            }
            markRemainingReadersReady(readers);
        }
    }

    private void processCollection(
            String collectionId,
            CatalogTable table,
            Set<Integer> readers,
            int idSplitSize)
            throws InterruptedException {
        log.info("Processing collection '{}'", collectionId);
        int offset = 0;
        long totalForCollection = 0;
        int batchNum = 0;

        while (!shuttingDown) {
            // Wait for all readers to be ready before fetching next batch.
            waitForAllReaders(readers);

            // Phase 1: scan next batch of IDs from the enumerator's single client.
            List<String> batchIds = fetchIdBatch(collectionId, offset, idSplitSize);
            if (batchIds == null || batchIds.isEmpty()) {
                break;
            }

            batchNum++;
            log.info(
                    "Collection '{}' batch #{}: fetched {} IDs [{}-{})",
                    collectionId,
                    batchNum,
                    batchIds.size(),
                    offset,
                    offset + batchIds.size());

            // Reset readyReaders for this round — we're about to assign new work.
            synchronized (readyLock) {
                readyReaders.clear();
            }

            // Partition and push to readers.
            partitionAndAssign(collectionId, table, readers, batchIds);
            totalForCollection += batchIds.size();
            offset += batchIds.size();

            // Last batch — break out after assignment.
            if (batchIds.size() < idSplitSize) {
                break;
            }
        }

        // Make sure enumerator doesn't move on to the next collection until readers finish
        // this collection's last batch.
        waitForAllReaders(readers);
        log.info("Collection '{}' complete, {} records total", collectionId, totalForCollection);
    }

    private List<String> fetchIdBatch(String collectionId, int offset, int limit) {
        try {
            ChromaDBClient.GetRecordsResult result =
                    client.getRecords(
                            collectionId,
                            new ChromaDBClient.GetRecordsRequest()
                                    .setLimit(limit)
                                    .setOffset(offset)
                                    .setInclude(Collections.emptyList()));
            if (result == null) {
                throw new ChromaDBConnectorException(
                        ChromaDBConnectorErrorCode.READ_DATA_FAIL,
                        "Received null response from ChromaDB during Phase 1 for collection '"
                                + collectionId
                                + "' at offset "
                                + offset);
            }
            return result.getIds();
        } catch (ChromaDBConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new ChromaDBConnectorException(
                    ChromaDBConnectorErrorCode.READ_DATA_FAIL,
                    "Phase 1 scan failed for collection '" + collectionId + "' at offset " + offset,
                    e);
        }
    }

    private void partitionAndAssign(
            String collectionId,
            CatalogTable table,
            Set<Integer> readers,
            List<String> batchIds) {
        int parallelism = readers.size();
        int total = batchIds.size();
        // Ceiling division so that the last reader may get fewer items, not more.
        int perReader = (total + parallelism - 1) / parallelism;

        List<Integer> readerList = new ArrayList<>(readers);
        for (int i = 0; i < parallelism; i++) {
            int start = i * perReader;
            int end = Math.min(start + perReader, total);
            int readerId = readerList.get(i);
            if (start >= end) {
                // This reader has no work this round; mark it ready immediately so the
                // next-round barrier doesn't wait on it.
                synchronized (readyLock) {
                    readyReaders.add(readerId);
                    readyLock.notifyAll();
                }
                continue;
            }
            List<String> shardIds = new ArrayList<>(batchIds.subList(start, end));
            long seq = nextSplitSeq(collectionId);
            ChromaDBSourceSplit split =
                    new ChromaDBSourceSplit(
                            collectionId, table.getTablePath(), seq, shardIds);
            assignToReader(readerId, split);
        }
    }

    private long nextSplitSeq(String collectionId) {
        synchronized (stateLock) {
            long seq = collectionSplitSeq.getOrDefault(collectionId, 0L);
            collectionSplitSeq.put(collectionId, seq + 1);
            return seq;
        }
    }

    private void assignToReader(int readerId, ChromaDBSourceSplit split) {
        synchronized (stateLock) {
            assignedSplits
                    .computeIfAbsent(readerId, r -> new ArrayList<>())
                    .add(split);
        }
        context.assignSplit(readerId, Collections.singletonList(split));
        log.debug(
                "Assigned split {} ({} ids) to reader {}",
                split.splitId(),
                split.getIds().size(),
                readerId);
    }

    private void markRemainingReadersReady(Set<Integer> readers) {
        synchronized (readyLock) {
            readyReaders.addAll(readers);
            readyLock.notifyAll();
        }
    }

    private void waitForAllReaders(Set<Integer> readers) throws InterruptedException {
        synchronized (readyLock) {
            while (!readyReaders.containsAll(readers) && !shuttingDown) {
                readyLock.wait();
            }
        }
    }

    @Override
    public void close() throws IOException {
        shuttingDown = true;
        synchronized (readyLock) {
            readyLock.notifyAll();
        }
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public void addSplitsBack(List<ChromaDBSourceSplit> splits, int subtaskId) {
        log.info("Re-queueing {} splits from failed reader {}", splits.size(), subtaskId);
        synchronized (stateLock) {
            carriedOverSplits.addAll(splits);
            List<ChromaDBSourceSplit> assigned = assignedSplits.get(subtaskId);
            if (assigned != null) {
                assigned.removeAll(splits);
            }
        }
        // Don't directly assign here — the main run() loop handles carried-over splits.
    }

    @Override
    public int currentUnassignedSplitSize() {
        return carriedOverSplits.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        synchronized (readyLock) {
            readyReaders.add(subtaskId);
            readyLock.notifyAll();
        }
        log.debug("Reader {} requested more splits (marked ready)", subtaskId);
    }

    @Override
    public void registerReader(int subtaskId) {
        log.info("Reader {} registered", subtaskId);
        // Initial-ready is set inside run(); nothing to do here. The framework guarantees
        // registerReader() is called before run().
    }

    @Override
    public ChromaDBSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            Map<Integer, List<ChromaDBSourceSplit>> deepCopy = new HashMap<>();
            for (Map.Entry<Integer, List<ChromaDBSourceSplit>> entry :
                    assignedSplits.entrySet()) {
                deepCopy.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }
            return new ChromaDBSourceState(
                    new HashSet<>(completedCollections),
                    new ArrayList<>(carriedOverSplits),
                    deepCopy);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // No action needed
    }

    // Package-visible for tests
    Collection<String> completedCollectionsView() {
        synchronized (stateLock) {
            return new HashSet<>(completedCollections);
        }
    }
}
