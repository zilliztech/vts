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
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.EsClusterConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SearchApiTypeEnum;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.PointInTimeResult;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ScrollResult;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.source.DefaultSeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.source.ElasticsearchRecord;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.source.SeaTunnelRowDeserializer;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public class ElasticsearchSourceReader
        implements SourceReader<SeaTunnelRow, ElasticsearchSourceSplit> {

    Context context;

    private final ReadonlyConfig connConfig;

    // Volatile: open() writes, pollNext() and close() read; framework guarantees
    // happens-before but being explicit is cheap and matches the enumerator.
    private volatile EsRestClient esRestClient;

    private final Map<String, EsRestClient> dataNodeClients = new ConcurrentHashMap<>();

    final ConcurrentLinkedDeque<ElasticsearchSourceSplit> splits = new ConcurrentLinkedDeque<>();

    volatile boolean noMoreSplit;

    private final long pollNextWaitTime = 1000L;

    // Resolved at construction from connConfig (no IO needed): whether the job
    // is pinned to configured hosts only (skips direct-to-data-node optimization)
    // and the scheme (http|https) to prepend when dialing a data-node
    // publish_address (which is scheme-less). Final so cross-thread reads in
    // pollNext don't rely on framework-provided happens-before with open().
    private final boolean wanOnly;
    private final String directNodeScheme;

    public ElasticsearchSourceReader(Context context, ReadonlyConfig connConfig) {
        this.context = context;
        this.connConfig = connConfig;
        this.wanOnly = connConfig.get(EsClusterConnectionConfig.WAN_ONLY);
        this.directNodeScheme = deriveScheme(connConfig.get(EsClusterConnectionConfig.HOSTS));
    }

    @Override
    public void open() {
        esRestClient = EsRestClient.createInstance(this.connConfig);
    }

    /**
     * ES {@code _nodes/http} returns {@code publish_address} as {@code host:port} with no scheme.
     * Inherit the scheme from the first configured host that carries one, defaulting to {@code http}.
     */
    private static String deriveScheme(List<String> hosts) {
        if (hosts != null) {
            for (String h : hosts) {
                int idx = h.indexOf("://");
                if (idx > 0) {
                    return h.substring(0, idx);
                }
            }
        }
        return "http";
    }

    @Override
    public void close() throws IOException {
        try {
            if (esRestClient != null) {
                esRestClient.close();
            }
        } finally {
            for (EsRestClient client : dataNodeClients.values()) {
                if (client != null && client != esRestClient) {
                    try {
                        client.close();
                    } catch (Exception e) {
                        log.warn("Failed to close data node client", e);
                    }
                }
            }
            dataNodeClients.clear();
        }
    }

    private EsRestClient getClientForSplit(ElasticsearchSourceSplit split) {
        if (wanOnly || split.getDataNodeAddress() == null) {
            return esRestClient;
        }
        return dataNodeClients.computeIfAbsent(split.getDataNodeAddress(), this::createDirectClient);
    }

    /**
     * Build an {@link EsRestClient} pointed at a specific data node, inheriting
     * auth / TLS settings from the job config. Performs a connectivity check to
     * make sure the node is reachable before caching the client; on failure,
     * falls back to the shared client.
     *
     * <p>Trade-off: the fallback is cached under the same addr key, so later
     * splits targeting the same down node skip the slow reconnect but also don't
     * reclaim direct routing if the node recovers mid-job. Accepted — keeps
     * pollNext latency bounded when a node is down, at the cost of missing a
     * best-effort optimization.
     */
    private EsRestClient createDirectClient(String addr) {
        EsRestClient directClient = null;
        try {
            Optional<String> username = connConfig.getOptional(EsClusterConnectionConfig.USERNAME);
            Optional<String> password = connConfig.getOptional(EsClusterConnectionConfig.PASSWORD);
            Optional<String> apiKey = connConfig.getOptional(EsClusterConnectionConfig.API_KEY);
            Optional<String> cloudId = connConfig.getOptional(EsClusterConnectionConfig.CLOUD_ID);
            boolean tlsVerifyCertificate =
                    connConfig.get(EsClusterConnectionConfig.TLS_VERIFY_CERTIFICATE);
            boolean tlsVerifyHostnames =
                    connConfig.get(EsClusterConnectionConfig.TLS_VERIFY_HOSTNAME);
            Optional<String> keystorePath = Optional.empty();
            Optional<String> keystorePassword = Optional.empty();
            Optional<String> truststorePath = Optional.empty();
            Optional<String> truststorePassword = Optional.empty();
            if (tlsVerifyCertificate) {
                keystorePath =
                        connConfig.getOptional(EsClusterConnectionConfig.TLS_KEY_STORE_PATH);
                keystorePassword =
                        connConfig.getOptional(EsClusterConnectionConfig.TLS_KEY_STORE_PASSWORD);
                truststorePath =
                        connConfig.getOptional(EsClusterConnectionConfig.TLS_TRUST_STORE_PATH);
                truststorePassword =
                        connConfig.getOptional(EsClusterConnectionConfig.TLS_TRUST_STORE_PASSWORD);
            }
            // publish_address is scheme-less — inherit from configured HOSTS
            // so HTTPS clusters don't silently fall back to the shared client.
            String directUrl = addr.contains("://") ? addr : directNodeScheme + "://" + addr;
            directClient =
                    EsRestClient.createInstance(
                            Collections.singletonList(directUrl),
                            cloudId,
                            username,
                            password,
                            apiKey,
                            tlsVerifyCertificate,
                            tlsVerifyHostnames,
                            keystorePath,
                            keystorePassword,
                            truststorePath,
                            truststorePassword);
            directClient.getClusterInfo();
            log.info("Connected to data node {}", addr);
            return directClient;
        } catch (Exception e) {
            if (directClient != null) {
                try {
                    directClient.close();
                } catch (Exception closeException) {
                    log.warn("Failed to close unreachable data node client {}", addr, closeException);
                }
            }
            log.warn("Cannot connect to data node {}, using default client", addr, e);
            return esRestClient;
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            ElasticsearchSourceSplit split = splits.poll();
            if (split != null) {
                SeaTunnelRowType seaTunnelRowType = split.getSeaTunnelRowType();
                Map<String, Map<String, Object>> columnOptionsMap = new HashMap<>();
                CatalogTable catalogTable = split.getSourceConfig().getCatalogTable();
                if (catalogTable != null) {
                    for (Column col : catalogTable.getTableSchema().getColumns()) {
                        if (col.getOptions() != null && !col.getOptions().isEmpty()) {
                            columnOptionsMap.put(col.getName(), col.getOptions());
                        }
                    }
                }
                SeaTunnelRowDeserializer deserializer =
                        new DefaultSeaTunnelRowDeserializer(
                                seaTunnelRowType, columnOptionsMap);
                if (split.getSourceConfig().getSearchApiType() == SearchApiTypeEnum.PIT) {
                    pitSearch(split, output, deserializer);
                } else {
                    scrollSearch(split, output, deserializer);
                }
            } else if (noMoreSplit) {
                log.info("Closed the bounded Elasticsearch source");
                context.signalNoMoreElement();
            } else {
                Thread.sleep(pollNextWaitTime);
            }
        }
    }

    private void scrollSearch(
            ElasticsearchSourceSplit split,
            Collector<SeaTunnelRow> output,
            SeaTunnelRowDeserializer deserializer) {
        SourceConfig cfg = split.getSourceConfig();
        log.info(
                "scrollSearch started: index={}, shardId={}, preference={}",
                split.getConcreteIndex(),
                split.getShardId(),
                split.getPreference());
        EsRestClient client = getClientForSplit(split);
        String scrollId = null;
        long totalProcessed = 0;
        // -1 = ES didn't report total on the first response. 0 or positive =
        // captured from the first batch, must equal totalProcessed at loop exit.
        long totalHits = -1;
        try {
            ScrollResult scrollResult =
                    client.searchByScroll(
                            split.getConcreteIndex(),
                            cfg.getSource(),
                            cfg.getQuery(),
                            cfg.getScrollTime(),
                            cfg.getScrollSize(),
                            split.getPreference());
            scrollId = scrollResult.getScrollId();
            totalHits = scrollResult.getTotalHits();
            // The scroll request forces track_total_hits=true, so a missing
            // hits.total on the first batch means the response has been tampered
            // with (proxy/WAF) or the backend is a non-compliant ES fork. Refuse
            // to run blind — losing a split is preferable to silent data loss.
            if (totalHits < 0) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                        String.format(
                                "Refusing scrollSearch without hits.total "
                                        + "(index=%s, shard=%d). "
                                        + "track_total_hits was requested but the response lacked it; "
                                        + "check for a proxy/WAF stripping fields or a non-compliant ES backend.",
                                split.getConcreteIndex(),
                                split.getShardId()));
            }
            totalProcessed += scrollResult.getDocs().size();
            outputFromDocs(scrollResult.getDocs(), cfg, output, deserializer);
            while (scrollResult.getDocs() != null && !scrollResult.getDocs().isEmpty()) {
                scrollResult =
                        client.searchWithScrollId(
                                scrollResult.getScrollId(), cfg.getScrollTime());
                scrollId = scrollResult.getScrollId();
                totalProcessed += scrollResult.getDocs().size();
                outputFromDocs(scrollResult.getDocs(), cfg, output, deserializer);
            }

            if (totalHits >= 0 && totalProcessed != totalHits) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                        String.format(
                                "scrollSearch data inconsistency: index=%s, "
                                        + "totalProcessed=%d != totalHits=%d",
                                split.getConcreteIndex(),
                                totalProcessed,
                                totalHits));
            }
            log.info(
                    "scrollSearch completed: index={}, shardId={}, totalProcessed={}, totalHits={}",
                    split.getConcreteIndex(),
                    split.getShardId(),
                    totalProcessed,
                    totalHits);
        } finally {
            if (scrollId != null) {
                client.clearScroll(scrollId);
            }
        }
    }

    private void pitSearch(
            ElasticsearchSourceSplit split,
            Collector<SeaTunnelRow> output,
            SeaTunnelRowDeserializer deserializer) {
        SourceConfig cfg = split.getSourceConfig();
        String splitId = split.splitId();
        log.info(
                "pitSearch started: index={}, shardId={}, preference={}",
                split.getConcreteIndex(),
                split.getShardId(),
                split.getPreference());
        EsRestClient client = getClientForSplit(split);
        String pitId = null;
        try {
            pitId =
                    client.createPointInTime(
                            split.getConcreteIndex(),
                            cfg.getPitKeepAlive(),
                            split.getPreference());
            log.info(
                    "Created PIT {} for split {} (index={})",
                    pitId, splitId, split.getConcreteIndex());

            Object[] searchAfter = null;
            long totalProcessed = 0;
            // -1 = ES didn't report total. 0 or positive = ES-reported count from
            // the first batch; totalProcessed must equal it at loop exit.
            long totalHits = -1;
            boolean firstBatch = true;

            while (true) {
                PointInTimeResult result =
                        client.doSearchWithPointInTime(
                                cfg.getSource(),
                                cfg.getQuery(),
                                cfg.getPitBatchSize(),
                                pitId,
                                searchAfter,
                                cfg.getPitKeepAlive(),
                                /* trackTotalHits */ firstBatch);
                pitId = result.getPitId();

                // Capture totalHits BEFORE the empty-check so we also cover the
                // pathological "first batch returns empty docs but total>0" case
                // (would otherwise silently skip the totalProcessed == totalHits
                // reconciliation). track_total_hits was requested on this first
                // call and will be false on subsequent calls (firstBatch flag).
                if (firstBatch) {
                    totalHits = result.getTotalHits();
                    firstBatch = false;
                    // The first PIT call forces track_total_hits=true, so a
                    // missing hits.total means the response has been tampered
                    // with (proxy/WAF) or the backend is a non-compliant ES
                    // fork. Refuse to run blind — losing a split is preferable
                    // to silent data loss.
                    if (totalHits < 0) {
                        throw new ElasticsearchConnectorException(
                                ElasticsearchConnectorErrorCode.SEARCH_WITH_PIT_FAILED,
                                String.format(
                                        "Refusing pitSearch without hits.total "
                                                + "(index=%s, shard=%d, pitId=%s). "
                                                + "track_total_hits was requested but the response lacked it; "
                                                + "check for a proxy/WAF stripping fields or a non-compliant ES backend.",
                                        split.getConcreteIndex(),
                                        split.getShardId(),
                                        pitId));
                    }
                }

                if (result.getDocs().isEmpty()) {
                    break;
                }

                // Guard against a non-empty batch that failed to yield a next
                // search_after cursor — continuing would either loop forever or
                // duplicate the batch we just read.
                Object[] nextSearchAfter = result.getSearchAfter();
                if (nextSearchAfter == null || nextSearchAfter.length == 0) {
                    throw new ElasticsearchConnectorException(
                            ElasticsearchConnectorErrorCode.SEARCH_WITH_PIT_FAILED,
                            String.format(
                                    "PIT batch returned %d docs but no sort values to "
                                            + "advance search_after cursor "
                                            + "(index=%s, shard=%d, pitId=%s, totalProcessed=%d)",
                                    result.getDocs().size(),
                                    split.getConcreteIndex(),
                                    split.getShardId(),
                                    pitId,
                                    totalProcessed));
                }
                searchAfter = nextSearchAfter;

                totalProcessed += result.getDocs().size();
                outputFromDocs(result.getDocs(), cfg, output, deserializer);
            }

            if (totalHits >= 0 && totalProcessed != totalHits) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SEARCH_WITH_PIT_FAILED,
                        String.format(
                                "pitSearch data inconsistency: index=%s, "
                                        + "totalProcessed=%d != totalHits=%d",
                                split.getConcreteIndex(),
                                totalProcessed,
                                totalHits));
            }
            log.info(
                    "pitSearch completed: index={}, shardId={}, totalProcessed={}, totalHits={}",
                    split.getConcreteIndex(),
                    split.getShardId(),
                    totalProcessed,
                    totalHits);
        } finally {
            if (pitId != null) {
                client.deletePointInTime(pitId);
            }
        }
    }

    private void outputFromDocs(
            List<Map<String, Object>> docs,
            SourceConfig sourceConfig,
            Collector<SeaTunnelRow> output,
            SeaTunnelRowDeserializer deserializer) {
        List<String> source = sourceConfig.getSource();
        String tableId = sourceConfig.getCatalogTable().getTablePath().toString();
        for (Map<String, Object> doc : docs) {
            SeaTunnelRow seaTunnelRow =
                    deserializer.deserialize(new ElasticsearchRecord(doc, source, tableId));
            output.collect(seaTunnelRow);
        }
    }

    @Override
    public List<ElasticsearchSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(splits);
    }

    @Override
    public void addSplits(List<ElasticsearchSourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
