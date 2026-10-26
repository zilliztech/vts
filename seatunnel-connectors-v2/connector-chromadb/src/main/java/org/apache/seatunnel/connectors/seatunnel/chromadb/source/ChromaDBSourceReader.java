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
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.chromadb.client.ChromaDBClient;
import org.apache.seatunnel.connectors.seatunnel.chromadb.config.ChromaDBSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.chromadb.exception.ChromaDBConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.chromadb.exception.ChromaDBConnectorException;
import org.apache.seatunnel.connectors.seatunnel.chromadb.utils.ChromaDBConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Source reader that executes Phase 2 only — fetching full records by a pre-assigned ID list.
 *
 * <p>Phase 1 (scanning IDs via offset/limit) is owned by {@link ChromaDBSplitEnumerator}, which
 * hands out shards of the ID list as splits. This puts Phase 1's single-threaded sequential
 * cost in one place and lets N readers parallelize the dominant Phase 2 work end-to-end with
 * the sink.
 *
 * <p>When the pending split queue is empty, the reader calls {@code context.sendSplitRequest()}
 * to signal the enumerator that it's ready for more — which is the backpressure mechanism that
 * prevents the enumerator from over-producing splits into reader queues.
 */
@Slf4j
public class ChromaDBSourceReader implements SourceReader<SeaTunnelRow, ChromaDBSourceSplit> {

    private static final long EMPTY_QUEUE_SLEEP_MS = 50L;

    private final ReadonlyConfig config;
    private final Context context;
    private final Map<String, CatalogTable> collectionTables;
    private final Deque<ChromaDBSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();
    private ChromaDBClient client;
    private volatile boolean noMoreSplit;
    private volatile boolean splitRequestOutstanding;

    public ChromaDBSourceReader(
            ReadonlyConfig config,
            Context context,
            Map<String, CatalogTable> collectionTables) {
        this.config = config;
        this.context = context;
        this.collectionTables = collectionTables;
    }

    @Override
    public void open() throws Exception {
        validateConfig();
        client = ChromaDBSource.createClient(config);
        log.info("ChromaDB reader {} opened", context.getIndexOfSubtask());
    }

    private void validateConfig() {
        checkPositive(ChromaDBSourceConfig.BATCH_SIZE);
        checkPositive(ChromaDBSourceConfig.ID_SPLIT_SIZE);
        checkPositive(ChromaDBSourceConfig.CONNECT_TIMEOUT);
        checkPositive(ChromaDBSourceConfig.READ_TIMEOUT);
    }

    private void checkPositive(Option<Integer> option) {
        int value = config.get(option);
        if (value <= 0) {
            throw new ChromaDBConnectorException(
                    ChromaDBConnectorErrorCode.READ_DATA_FAIL,
                    option.key() + " must be positive, got: " + value);
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            ChromaDBSourceSplit split = pendingSplits.poll();
            if (split != null) {
                splitRequestOutstanding = false;
                processSplit(output, split);
                return;
            }
        }

        if (noMoreSplit) {
            if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
                context.signalNoMoreElement();
            }
            return;
        }

        // Queue is empty and there may be more splits coming — ask for one.
        if (!splitRequestOutstanding) {
            context.sendSplitRequest();
            splitRequestOutstanding = true;
        }
        Thread.sleep(EMPTY_QUEUE_SLEEP_MS);
    }

    private void processSplit(Collector<SeaTunnelRow> output, ChromaDBSourceSplit split) {
        String collectionId = split.getCollectionId();
        CatalogTable table = collectionTables.get(collectionId);
        if (table == null) {
            throw new ChromaDBConnectorException(
                    ChromaDBConnectorErrorCode.READ_DATA_FAIL,
                    "Split references collection '"
                            + collectionId
                            + "' which is not found in catalog. "
                            + "This may indicate a configuration change after checkpoint.");
        }
        List<String> ids = split.getIds();
        if (ids == null || ids.isEmpty()) {
            log.debug("Split {} has no IDs; nothing to do", split.splitId());
            return;
        }

        TableSchema tableSchema = table.getTableSchema();
        String tableId = split.getTablePath().toString();
        int batchSize = config.get(ChromaDBSourceConfig.BATCH_SIZE);

        log.info(
                "Processing split {} ({} ids) for collection '{}'",
                split.splitId(),
                ids.size(),
                collectionId);

        for (int i = 0; i < ids.size(); i += batchSize) {
            int end = Math.min(i + batchSize, ids.size());
            List<String> chunk = ids.subList(i, end);
            fetchAndEmit(output, collectionId, tableSchema, tableId, chunk, i);
        }
    }

    private void fetchAndEmit(
            Collector<SeaTunnelRow> output,
            String collectionId,
            TableSchema tableSchema,
            String tableId,
            List<String> chunk,
            int chunkOffset) {
        ChromaDBClient.GetRecordsResult result =
                client.getRecords(
                        collectionId,
                        new ChromaDBClient.GetRecordsRequest()
                                .setIds(chunk)
                                .setInclude(
                                        Arrays.asList(
                                                ChromaDBSourceConfig.INCLUDE_EMBEDDINGS,
                                                ChromaDBSourceConfig.INCLUDE_DOCUMENTS,
                                                ChromaDBSourceConfig.INCLUDE_URIS,
                                                ChromaDBSourceConfig.INCLUDE_METADATAS)));
        if (result == null) {
            throw new ChromaDBConnectorException(
                    ChromaDBConnectorErrorCode.READ_DATA_FAIL,
                    "Received null response from ChromaDB when fetching data for collection '"
                            + collectionId
                            + "', chunk offset "
                            + chunkOffset);
        }
        emitRows(output, tableSchema, tableId, result);
    }

    @Override
    public List<ChromaDBSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<ChromaDBSourceSplit> splits) {
        log.info("Reader {} received {} splits", context.getIndexOfSubtask(), splits.size());
        pendingSplits.addAll(splits);
        splitRequestOutstanding = false;
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Reader {} received no-more-splits signal", context.getIndexOfSubtask());
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // No action needed
    }

    private void emitRows(
            Collector<SeaTunnelRow> output,
            TableSchema tableSchema,
            String tableId,
            ChromaDBClient.GetRecordsResult result) {
        List<String> ids = result.getIds();
        List<float[]> embeddings = result.getEmbeddings();
        List<String> documents = result.getDocuments();
        List<String> uris = result.getUris();
        List<Map<String, Object>> metadatas = result.getMetadatas();

        for (int i = 0; i < ids.size(); i++) {
            try {
                String id = ids.get(i);
                float[] embedding =
                        (embeddings != null && i < embeddings.size())
                                ? embeddings.get(i)
                                : null;
                String document =
                        (documents != null && i < documents.size())
                                ? documents.get(i)
                                : null;
                String uri =
                        (uris != null && i < uris.size())
                                ? uris.get(i)
                                : null;
                Map<String, Object> metadata =
                        (metadatas != null && i < metadatas.size())
                                ? metadatas.get(i)
                                : null;

                SeaTunnelRow row =
                        ChromaDBConverter.convert(tableSchema, id, embedding, document, uri, metadata);
                row.setTableId(tableId);
                output.collect(row);
            } catch (Exception e) {
                throw new ChromaDBConnectorException(
                        ChromaDBConnectorErrorCode.READ_DATA_FAIL,
                        "Failed to convert record at index " + i,
                        e);
            }
        }
    }
}
