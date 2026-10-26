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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Slf4j
public class ChromaDBSourceReader implements SourceReader<SeaTunnelRow, ChromaDBSourceSplit> {

    private final ReadonlyConfig config;
    private final Context context;
    private final Map<String, CatalogTable> collectionTables;
    private final Deque<ChromaDBSourceSplit> pendingSplits = new ArrayDeque<>();
    private final Object stateLock = new Object();
    private ChromaDBClient client;
    private boolean noMoreSplit;
    private boolean finished;

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
        log.info("ChromaDB client connected, reader index: {}", context.getIndexOfSubtask());
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
        ChromaDBSourceSplit split;
        synchronized (stateLock) {
            split = pendingSplits.peekFirst();
            if (split == null) {
                signalFinished();
                return;
            }
        }
        CatalogTable table = collectionTables.get(split.getCollectionId());
        if (table == null) {
            throw new ChromaDBConnectorException(
                    ChromaDBConnectorErrorCode.READ_DATA_FAIL,
                    "Split references unknown collection '" + split.getCollectionId() + "'");
        }

        // Network calls run outside the checkpoint lock. A checkpoint taken during a request
        // retains the old progress and safely repeats that request after recovery.
        if (split.getPendingIds().isEmpty()) {
            int limit =
                    (int)
                            Math.min(
                                    config.get(ChromaDBSourceConfig.ID_SPLIT_SIZE),
                                    split.getEndOffset() - split.getNextOffset());
            ChromaDBClient.GetRecordsResult idResult =
                    client.getRecords(
                            split.getCollectionId(),
                            new ChromaDBClient.GetRecordsRequest()
                                    .setLimit(limit)
                                    .setOffset(split.getNextOffset())
                                    .setInclude(Collections.emptyList()));
            List<String> ids = idResult == null ? null : idResult.getIds();
            if (ids == null || ids.size() != limit || ids.stream().anyMatch(Objects::isNull)) {
                throw new ChromaDBConnectorException(
                        ChromaDBConnectorErrorCode.READ_DATA_FAIL,
                        "Expected "
                                + limit
                                + " IDs for split "
                                + split.splitId()
                                + " at offset "
                                + split.getNextOffset()
                                + ", received "
                                + (ids == null ? "null" : ids.size())
                                + ". The source may have changed or capped the response. "
                                + "Use a stable source and configure id_split_size within its request limit.");
            }
            split = split.withPendingIds(ids);
            synchronized (stateLock) {
                pendingSplits.removeFirst();
                pendingSplits.addFirst(split);
            }
        }

        int remaining = split.getPendingIds().size() - split.getPendingIdIndex();
        int batchSize = Math.min(config.get(ChromaDBSourceConfig.BATCH_SIZE), remaining);
        List<String> chunk =
                split.getPendingIds()
                        .subList(split.getPendingIdIndex(), split.getPendingIdIndex() + batchSize);
        ChromaDBClient.GetRecordsResult result =
                client.getRecords(
                        split.getCollectionId(),
                        new ChromaDBClient.GetRecordsRequest()
                                .setIds(chunk)
                                .setInclude(
                                        Arrays.asList(
                                                ChromaDBSourceConfig.INCLUDE_EMBEDDINGS,
                                                ChromaDBSourceConfig.INCLUDE_DOCUMENTS,
                                                ChromaDBSourceConfig.INCLUDE_URIS,
                                                ChromaDBSourceConfig.INCLUDE_METADATAS)));
        Set<String> requestedIds = new HashSet<>(chunk);
        if (result == null
                || result.getIds() == null
                || result.getIds().size() != chunk.size()
                || requestedIds.size() != chunk.size()
                || !requestedIds.equals(new HashSet<>(result.getIds()))) {
            throw new ChromaDBConnectorException(
                    ChromaDBConnectorErrorCode.READ_DATA_FAIL,
                    "ChromaDB did not return the requested IDs for split "
                            + split.splitId()
                            + " at offset "
                            + split.getNextOffset()
                            + ". Records may have changed or the request exceeds the server limit.");
        }

        synchronized (output.getCheckpointLock()) {
            synchronized (stateLock) {
                emitRows(output, table.getTableSchema(), split.getTablePath().toString(), result);
                // Publish progress only after the whole batch has been emitted successfully.
                ChromaDBSourceSplit next = split.advance(chunk.size());
                pendingSplits.removeFirst();
                if (next.getNextOffset() < next.getEndOffset()) {
                    pendingSplits.addFirst(next);
                }
                signalFinished();
            }
        }
    }

    /** Called under stateLock, also used when an empty reader receives noMoreSplits. */
    private void signalFinished() {
        if (!finished
                && noMoreSplit
                && pendingSplits.isEmpty()
                && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            finished = true;
            context.signalNoMoreElement();
        }
    }

    @Override
    public List<ChromaDBSourceSplit> snapshotState(long checkpointId) {
        synchronized (stateLock) {
            List<ChromaDBSourceSplit> snapshot = new ArrayList<>();
            pendingSplits.forEach(split -> snapshot.add(split.copy()));
            return snapshot;
        }
    }

    @Override
    public void addSplits(List<ChromaDBSourceSplit> splits) {
        synchronized (stateLock) {
            if (finished) {
                throw new IllegalStateException(
                        "Cannot assign splits to a finished ChromaDB reader");
            }
            for (ChromaDBSourceSplit split : splits) {
                if (split.getNextOffset() < split.getEndOffset()) {
                    pendingSplits.addLast(split.copy());
                }
            }
        }
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (stateLock) {
            noMoreSplit = true;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Progress is part of the split state.
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
