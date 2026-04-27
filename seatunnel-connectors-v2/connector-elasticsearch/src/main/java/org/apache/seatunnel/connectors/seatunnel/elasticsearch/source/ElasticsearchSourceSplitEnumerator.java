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
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.EsClusterConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.IndexDocsCount;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ShardInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class ElasticsearchSourceSplitEnumerator
        implements SourceSplitEnumerator<ElasticsearchSourceSplit, ElasticsearchSourceState> {

    /**
     * Bucket key used when a split has no known {@code nodeId} — e.g., unassigned
     * shard (defensive) or older ES that doesn't surface one. Collapses those
     * splits into a single bucket; the interleave degenerates to plain round-robin
     * by list order for that bucket, which is still a safe even distribution.
     *
     * <p>Note: {@code wan_only=true} does NOT fall through to this key — {@code nodeId}
     * is resolved from {@code _search_shards} regardless of {@code wan_only}, so the
     * opening-batch datanode spread still applies even when all TCP traffic routes
     * through the configured LB. Only {@code dataNodeAddress} is nulled in wan_only mode.
     */
    private static final String UNKNOWN_NODE_KEY = "__unknown__";

    /** Max attempts to {@code context.assignSplit} per reader before giving up. */
    private static final int DISPATCH_MAX_ATTEMPTS = 3;

    private final Context<ElasticsearchSourceSplit> context;

    private final ReadonlyConfig connConfig;

    // Volatile: open() and run()/close() may execute on different framework threads;
    // we don't rely on implicit lifecycle happens-before.
    private volatile EsRestClient esRestClient;

    private final Object stateLock = new Object();

    // Per-reader queue of splits waiting to be dispatched. Guarded by stateLock.
    private final Map<Integer, List<ElasticsearchSourceSplit>> pendingSplit;

    private final List<SourceConfig> sourceConfigs;

    private volatile boolean shouldEnumerate;

    public ElasticsearchSourceSplitEnumerator(
            Context<ElasticsearchSourceSplit> context,
            ReadonlyConfig connConfig,
            List<SourceConfig> sourceConfigs) {
        this(context, null, connConfig, sourceConfigs);
    }

    public ElasticsearchSourceSplitEnumerator(
            Context<ElasticsearchSourceSplit> context,
            ElasticsearchSourceState sourceState,
            ReadonlyConfig connConfig,
            List<SourceConfig> sourceConfigs) {
        this.context = context;
        this.connConfig = connConfig;
        this.pendingSplit = new HashMap<>();
        this.shouldEnumerate = sourceState == null;
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            Map<Integer, List<ElasticsearchSourceSplit>> restored = sourceState.getPendingSplit();
            if (restored != null && !restored.isEmpty()) {
                if (this.shouldEnumerate) {
                    // Inconsistent saved state: "enumeration not done yet" + "here are
                    // some already-assigned splits" should never coexist. Current code
                    // paths can't construct this state, but if a future refactor or
                    // state-layer change allows it, restoring these splits AND letting
                    // run() re-enumerate would double-read every shard. Drop the stale
                    // pending and let run() do a fresh enumeration.
                    log.warn(
                            "Ignoring {} saved pending-split entries because "
                                    + "shouldEnumerate=true indicates enumeration was "
                                    + "never completed; re-enumerating from scratch.",
                            restored.size());
                } else {
                    restorePendingSplit(restored);
                }
            }
        }
        this.sourceConfigs = sourceConfigs;
    }

    /**
     * Re-run the datanode-interleave assignment on all restored splits against the
     * current parallelism. Handles three savepoint-restore scenarios uniformly:
     * <ul>
     *   <li><b>Scale-down</b> (old > current): owner IDs past current parallelism
     *       would otherwise leave splits orphaned in queues nobody drains → job hang.</li>
     *   <li><b>Scale-up</b> (old < current): new reader slots would otherwise sit idle
     *       while the old owners carry all the load.</li>
     *   <li><b>Unchanged</b>: re-interleave is deterministic idempotent-ish on the
     *       leftover set — some reassignment may happen but load stays balanced.</li>
     * </ul>
     * Reapplying the initial assignment algorithm also preserves the datanode-affinity
     * and opening-batch-spread properties end-to-end, without hand-coded rehash logic.
     *
     * <p>Caller is the constructor; no lock needed (object not yet published).
     */
    private void restorePendingSplit(
            Map<Integer, List<ElasticsearchSourceSplit>> restored) {
        List<ElasticsearchSourceSplit> all = new ArrayList<>();
        for (List<ElasticsearchSourceSplit> queue : restored.values()) {
            all.addAll(queue);
        }
        if (all.isEmpty()) {
            return;
        }
        log.info(
                "Restoring {} pending splits across {} readers via datanode-interleave.",
                all.size(), context.currentParallelism());
        assignInterleavedByDataNode(all);
    }

    @Override
    public void open() {
        esRestClient = EsRestClient.createInstance(connConfig);
    }

    /**
     * One-shot enumeration for a bounded source. On the first invocation, fetches
     * the shard layout from ES, interleaves splits across readers, and signals
     * {@code NoMoreSplits}. On subsequent invocations (if the framework ever
     * re-dispatches run), {@code shouldEnumerate=false} short-circuits the
     * enumeration — only pending re-queued splits (from addSplitsBack) get
     * drained and delivered.
     *
     * <p>Not designed for streaming / periodic enumeration. If this connector
     * ever gains a continuous-discovery mode, the {@code shouldEnumerate=true
     * → false} transition should be revisited.
     */
    @Override
    public void run() {
        // Enumerate OUTSIDE the lock — it issues HTTP calls and we must not
        // block snapshotState / addSplitsBack / registerReader while doing so.
        List<ElasticsearchSourceSplit> newSplits = null;
        if (shouldEnumerate) {
            newSplits = getElasticsearchSplit();
        }

        Set<Integer> allReaders;
        Map<Integer, List<ElasticsearchSourceSplit>> failed;
        synchronized (stateLock) {
            // Double-check under lock — if a concurrent run() already claimed the
            // enumeration, drop our (now-redundant) newSplits rather than appending
            // duplicates to pendingSplit.
            if (newSplits != null && shouldEnumerate) {
                assignInterleavedByDataNode(newSplits);
                shouldEnumerate = false;
            }
            // Read registered readers under lock after enumeration completes —
            // readers that registered while getElasticsearchSplit() was making
            // HTTP calls are visible here and will receive their pending splits
            // in the drain below.
            allReaders = new HashSet<>(context.registeredReaders());
            Map<Integer, List<ElasticsearchSourceSplit>> drained = drainPending(allReaders);
            // Dispatch INSIDE the lock so a concurrent snapshotState cannot
            // observe the "drained but not yet delivered" window.
            failed = dispatch(drained);
        }

        if (!failed.isEmpty()) {
            // run() is followed immediately by signalNoMoreSplits. Rolling the
            // failed splits back into pendingSplit would be useless — the readers
            // would be told "no more splits" and never ask again. Throw so the
            // framework fails the job and restarts from the last checkpoint.
            int totalFailed = failed.values().stream().mapToInt(List::size).sum();
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.DISPATCH_SPLIT_FAILED,
                    String.format(
                            "run() failed to dispatch %d splits to %d readers",
                            totalFailed, failed.size()));
        }

        log.debug("No more splits to assign. Sending NoMoreSplitsEvent to reader {}.", allReaders);
        allReaders.forEach(context::signalNoMoreSplits);
    }

    /**
     * Split assignment — group by datanode, interleave across groups, then
     * round-robin the interleaved list onto readers. This guarantees that when all
     * N readers start their first split concurrently, they hit up to N distinct
     * datanodes (bounded only by how many unique datanodes exist in the grouping),
     * avoiding the hot-spot failure mode where hash-routing piles multiple shards
     * from the same datanode onto the same reader.
     *
     * <p>The datanode grouping is a local computation — we don't keep it around
     * as state because nothing after initial assignment consults it.
     *
     * <p>Callable from two contexts:
     * <ul>
     *   <li>Initial enumeration in {@link #run()} — caller MUST hold {@code stateLock};</li>
     *   <li>Savepoint restore in the constructor — object is not yet published, so no
     *       lock needed and none is taken.</li>
     * </ul>
     */
    private void assignInterleavedByDataNode(List<ElasticsearchSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        if (readerCount <= 0 || splits.isEmpty()) {
            return;
        }

        // Step 1: bucket by datanode. LinkedHashMap for deterministic group iteration.
        Map<String, List<ElasticsearchSourceSplit>> byDataNode = new LinkedHashMap<>();
        for (ElasticsearchSourceSplit split : splits) {
            byDataNode.computeIfAbsent(nodeKeyFor(split), k -> new ArrayList<>()).add(split);
        }

        // Step 2: interleave across buckets (round-robin one split per bucket per pass).
        List<ElasticsearchSourceSplit> interleaved = interleaveByBucket(byDataNode);

        // Step 3: round-robin the interleaved list across readers.
        for (int i = 0; i < interleaved.size(); i++) {
            ElasticsearchSourceSplit split = interleaved.get(i);
            int owner = i % readerCount;
            pendingSplit.computeIfAbsent(owner, r -> new ArrayList<>()).add(split);
            log.debug(
                    "Assigned split {} (datanode={}) to reader {}",
                    split.splitId(),
                    nodeKeyFor(split),
                    owner);
        }
    }

    /**
     * Pull one split from each bucket per pass, producing a flat list where
     * consecutive items come from different datanodes whenever possible.
     */
    static List<ElasticsearchSourceSplit> interleaveByBucket(
            Map<String, List<ElasticsearchSourceSplit>> byDataNode) {
        List<ArrayDeque<ElasticsearchSourceSplit>> queues = new ArrayList<>(byDataNode.size());
        for (List<ElasticsearchSourceSplit> list : byDataNode.values()) {
            queues.add(new ArrayDeque<>(list));
        }
        List<ElasticsearchSourceSplit> result = new ArrayList<>();
        boolean pulledSomething = true;
        while (pulledSomething) {
            pulledSomething = false;
            for (ArrayDeque<ElasticsearchSourceSplit> q : queues) {
                ElasticsearchSourceSplit s = q.poll();
                if (s != null) {
                    result.add(s);
                    pulledSomething = true;
                }
            }
        }
        return result;
    }

    private static String nodeKeyFor(ElasticsearchSourceSplit split) {
        // Bucket by nodeId (logical node identity), not dataNodeAddress — so wan_only
        // jobs (which null out dataNodeAddress) still benefit from opening-batch
        // spread: different shards on different physical nodes go to different
        // readers regardless of whether we connect directly or via LB.
        String nodeId = split.getNodeId();
        return nodeId != null ? nodeId : UNKNOWN_NODE_KEY;
    }

    /**
     * Re-queue splits returned by a reader back into that reader's queue. The
     * {@code subtaskId} parameter is the sole owner — framework contract is that
     * {@code addSplitsBack(splits, subtaskId)} always carries splits that belong
     * to {@code subtaskId}, so no reverse lookup is needed.
     *
     * <p>Caller MUST hold {@code stateLock}.
     */
    private void addPendingSplit(
            Collection<ElasticsearchSourceSplit> splits, int subtaskId) {
        for (ElasticsearchSourceSplit split : splits) {
            log.debug("Re-queueing split {} to reader {}", split, subtaskId);
            pendingSplit.computeIfAbsent(subtaskId, r -> new ArrayList<>()).add(split);
        }
    }

    /**
     * Snapshot the pending-split queues for the given readers under lock and clear
     * them from {@code pendingSplit}. The returned map is owned by the caller and
     * should be passed to {@link #dispatch} outside the lock.
     *
     * <p>Caller MUST hold {@code stateLock}.
     */
    private Map<Integer, List<ElasticsearchSourceSplit>> drainPending(Collection<Integer> readers) {
        Map<Integer, List<ElasticsearchSourceSplit>> out = new HashMap<>();
        for (int reader : readers) {
            List<ElasticsearchSourceSplit> splits = pendingSplit.remove(reader);
            if (splits != null && !splits.isEmpty()) {
                out.put(reader, splits);
            }
        }
        return out;
    }

    /**
     * Deliver drained splits to readers via {@code context.assignSplit}.
     *
     * <p><b>Called INSIDE {@code stateLock}</b> — this keeps drain + deliver atomic
     * w.r.t. {@link #snapshotState}, so a concurrent checkpoint can never observe
     * splits "in flight" (removed from pendingSplit but not yet in the reader's
     * queue). The cost is holding the lock across a Hazelcast RPC + up to
     * {@link #DISPATCH_MAX_ATTEMPTS} retries with backoff; for this bounded
     * connector the dispatch events are infrequent (once per enumeration, once
     * per reader failure/register) so the lock-hold is acceptable.
     *
     * <p>Transient failures are retried with exponential backoff. Per-reader
     * exhaustion is recorded in the returned map rather than thrown — the caller
     * decides whether to throw (fatal, like {@link #run}) or roll back to
     * {@code pendingSplit} (recoverable, like {@link #addSplitsBack} where the
     * returning reader is typically mid-restart and will {@code registerReader}
     * shortly).
     *
     * @return map of (readerId → undispatched splits) for entries that failed
     *         after all retries; empty if everything delivered cleanly.
     */
    private Map<Integer, List<ElasticsearchSourceSplit>> dispatch(
            Map<Integer, List<ElasticsearchSourceSplit>> drained) {
        if (drained.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<Integer, List<ElasticsearchSourceSplit>> failed = null;
        for (Map.Entry<Integer, List<ElasticsearchSourceSplit>> entry : drained.entrySet()) {
            int reader = entry.getKey();
            List<ElasticsearchSourceSplit> splits = entry.getValue();
            log.info("Assign splits {} to reader {}", splits, reader);
            try {
                assignSplitWithRetry(reader, splits);
            } catch (ElasticsearchConnectorException e) {
                log.error(
                        "Failed to assign splits to reader {} after {} attempts: {}",
                        reader, DISPATCH_MAX_ATTEMPTS, e.getMessage());
                if (failed == null) failed = new HashMap<>();
                failed.put(reader, splits);
            }
        }
        return failed == null ? Collections.emptyMap() : failed;
    }

    private void assignSplitWithRetry(
            int reader, List<ElasticsearchSourceSplit> splits) {
        Exception last = null;
        for (int attempt = 1; attempt <= DISPATCH_MAX_ATTEMPTS; attempt++) {
            try {
                context.assignSplit(reader, splits);
                return;
            } catch (Exception e) {
                last = e;
                if (attempt < DISPATCH_MAX_ATTEMPTS) {
                    long sleepMs = 500L * (1L << (attempt - 1));
                    log.warn(
                            "assignSplit to reader {} failed (attempt {}/{}), sleep {}ms then retry",
                            reader, attempt, DISPATCH_MAX_ATTEMPTS, sleepMs, e);
                    try {
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new ElasticsearchConnectorException(
                                ElasticsearchConnectorErrorCode.DISPATCH_SPLIT_FAILED,
                                "Interrupted while retrying assignSplit to reader " + reader,
                                ie);
                    }
                }
            }
        }
        throw new ElasticsearchConnectorException(
                ElasticsearchConnectorErrorCode.DISPATCH_SPLIT_FAILED,
                String.format(
                        "Failed to assign %d splits to reader %d after %d attempts",
                        splits.size(), reader, DISPATCH_MAX_ATTEMPTS),
                last);
    }

    private List<ElasticsearchSourceSplit> getElasticsearchSplit() {
        boolean wanOnly = connConfig.get(EsClusterConnectionConfig.WAN_ONLY);

        Map<String, String> nodeAddresses = Collections.emptyMap();
        if (!wanOnly) {
            try {
                nodeAddresses = esRestClient.getNodesHttpAddresses();
            } catch (Exception e) {
                log.warn("Failed to get node addresses, falling back to configured hosts", e);
            }
        }

        List<ElasticsearchSourceSplit> splits = new ArrayList<>();
        int total = sourceConfigs.size();
        for (int i = 0; i < total; i++) {
            SourceConfig sourceConfig = sourceConfigs.get(i);
            String userIndexPattern = sourceConfig.getIndex();
            log.info(
                    "Resolving indices for pattern [{}] ({}/{})",
                    userIndexPattern, i + 1, total);

            // Resolve the pattern to concrete indices. Two-stage handling:
            //   - null docsCount → fail loudly (index is closed, recovering, or in
            //     some other abnormal state; silently skipping would lose the user's
            //     data without them knowing).
            //   - zero docsCount → safe to drop (no point opening a scroll/PIT that
            //     will read zero docs).
            // Then sort by doc count ascending so small indices finish first and
            // their shards are assigned to readers before the big ones.
            List<IndexDocsCount> resolved = esRestClient.getIndexDocsCount(userIndexPattern);
            for (IndexDocsCount info : resolved) {
                if (info.getDocsCount() == null) {
                    throw new ElasticsearchConnectorException(
                            ElasticsearchConnectorErrorCode.SEARCH_SHARDS_FAILED,
                            String.format(
                                    "Index [%s] (matched by pattern [%s]) has null docs "
                                            + "count — it may be closed, initializing, or "
                                            + "recovering. Open the index or narrow the "
                                            + "pattern, then retry.",
                                    info.getIndex(), userIndexPattern));
                }
            }
            List<IndexDocsCount> concreteIndices =
                    resolved.stream()
                            .filter(x -> x.getDocsCount() > 0)
                            .sorted(Comparator.comparingLong(IndexDocsCount::getDocsCount))
                            .collect(Collectors.toList());
            long totalDocs =
                    concreteIndices.stream()
                            .mapToLong(IndexDocsCount::getDocsCount)
                            .sum();
            log.info(
                    "Resolved {} non-empty indices for pattern [{}], total {} docs",
                    concreteIndices.size(), userIndexPattern, totalDocs);

            // One _search_shards call per concrete index: gets shard topology
            // (which datanode hosts which primary). SourceConfig is shared across
            // all splits of this source — runtime metadata lives on the Split itself.
            for (IndexDocsCount indexInfo : concreteIndices) {
                String concreteIndex = indexInfo.getIndex();
                List<ShardInfo> shards = esRestClient.getSearchShards(concreteIndex);
                log.info(
                        "Index [{}] ({} docs) -> {} shards",
                        concreteIndex, indexInfo.getDocsCount(), shards.size());
                for (ShardInfo shard : shards) {
                    String nodeAddr = nodeAddresses.get(shard.getNodeId());
                    splits.add(
                            new ElasticsearchSourceSplit(
                                    sourceConfig,
                                    shard.getConcreteIndex(),
                                    shard.getShardId(),
                                    shard.getNodeId(),
                                    nodeAddr));
                }
            }
        }
        return splits;
    }

    @Override
    public void close() throws IOException {
        if (esRestClient != null) {
            esRestClient.close();
        }
    }

    @Override
    public void addSplitsBack(List<ElasticsearchSourceSplit> splits, int subtaskId) {
        if (splits.isEmpty()) {
            return;
        }
        synchronized (stateLock) {
            addPendingSplit(splits, subtaskId);
            // Attempt an immediate re-dispatch — the reader may already be back.
            // If it isn't (retry budget of ~1.5s is tight vs. the 5-30s Zeta
            // typically takes to restart a failed task), leave the splits in
            // pendingSplit so the next registerReader call picks them up.
            // We do NOT throw here: unlike run()'s dispatch failure, addSplitsBack
            // has no signalNoMoreSplits follow-up that would make pending splits
            // unreachable — so a rollback is the safe, non-hanging outcome.
            Map<Integer, List<ElasticsearchSourceSplit>> drained =
                    drainPending(Collections.singleton(subtaskId));
            Map<Integer, List<ElasticsearchSourceSplit>> failed = dispatch(drained);
            rollbackFailed(failed);
        }
    }

    /**
     * Put undispatched splits back into {@code pendingSplit} so a future
     * {@code registerReader} call can re-drain and redeliver them.
     *
     * <p>Caller MUST hold {@code stateLock}.
     */
    private void rollbackFailed(Map<Integer, List<ElasticsearchSourceSplit>> failed) {
        if (failed.isEmpty()) {
            return;
        }
        for (Map.Entry<Integer, List<ElasticsearchSourceSplit>> entry : failed.entrySet()) {
            log.warn(
                    "Rolling {} splits back into pendingSplit for reader {} — "
                            + "dispatch failed, awaiting next registerReader.",
                    entry.getValue().size(), entry.getKey());
            pendingSplit
                    .computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                    .addAll(entry.getValue());
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        synchronized (stateLock) {
            int total = 0;
            for (List<ElasticsearchSourceSplit> queue : pendingSplit.values()) {
                total += queue.size();
            }
            return total;
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new ElasticsearchConnectorException(
                CommonErrorCode.OPERATION_NOT_SUPPORTED,
                "Unsupported handleSplitRequest: " + subtaskId);
    }

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to ElasticsearchSourceSplitEnumerator.", subtaskId);
        boolean signalNoMore;
        synchronized (stateLock) {
            // Dispatch INSIDE the lock (see dispatch javadoc). Failures roll back
            // to pendingSplit and then fail the job. A registered reader may not
            // trigger another registerReader/handleSplitRequest cycle, so silently
            // keeping pending splits here can leave the reader waiting forever.
            Map<Integer, List<ElasticsearchSourceSplit>> drained =
                    pendingSplit.isEmpty()
                            ? Collections.emptyMap()
                            : drainPending(Collections.singletonList(subtaskId));
            Map<Integer, List<ElasticsearchSourceSplit>> failed = dispatch(drained);
            if (!failed.isEmpty()) {
                rollbackFailed(failed);
                int totalFailed = failed.values().stream().mapToInt(List::size).sum();
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.DISPATCH_SPLIT_FAILED,
                        String.format(
                                "registerReader(%d) failed to dispatch %d splits; "
                                        + "rolled back to pending state and failing the job",
                                subtaskId, totalFailed));
            }
            signalNoMore = !shouldEnumerate;
        }
        if (signalNoMore) {
            log.info("Sending NoMoreSplitsEvent to late-registered reader {}.", subtaskId);
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public ElasticsearchSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            // Deep-copy the inner lists so the snapshot is decoupled from live
            // enumerator state — any concurrent addPendingSplit / drainPending
            // after this returns won't mutate the captured view.
            Map<Integer, List<ElasticsearchSourceSplit>> pendingCopy =
                    new HashMap<>(pendingSplit.size());
            for (Map.Entry<Integer, List<ElasticsearchSourceSplit>> e : pendingSplit.entrySet()) {
                pendingCopy.put(e.getKey(), new ArrayList<>(e.getValue()));
            }
            return new ElasticsearchSourceState(shouldEnumerate, pendingCopy);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
