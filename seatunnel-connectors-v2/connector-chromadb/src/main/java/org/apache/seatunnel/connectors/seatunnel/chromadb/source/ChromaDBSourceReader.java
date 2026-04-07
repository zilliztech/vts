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
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public class ChromaDBSourceReader implements SourceReader<SeaTunnelRow, ChromaDBSourceSplit> {

    private final ReadonlyConfig config;
    private final Context context;
    private final Map<String, CatalogTable> collectionTables;
    private final Deque<ChromaDBSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();
    private ChromaDBClient client;
    private volatile boolean noMoreSplit;

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
        synchronized (output.getCheckpointLock()) {
            ChromaDBSourceSplit split = pendingSplits.poll();
            if (split != null) {
                String collectionId = split.getCollectionId();
                CatalogTable table = collectionTables.get(collectionId);
                if (table == null) {
                    throw new ChromaDBConnectorException(
                            ChromaDBConnectorErrorCode.READ_DATA_FAIL,
                            "Split references collection '"
                                    + collectionId
                                    + "' which is not found in catalog. "
                                    + "This may indicate a configuration change after checkpoint.");
                } else {
                    TableSchema tableSchema = table.getTableSchema();
                    String tableId = split.getTablePath().toString();
                    String collectionName = table.getTablePath().getTableName();

                    log.info("Reading collection '{}' (id={})", collectionName, collectionId);
                    readCollection(output, collectionId, tableSchema, tableId);
                }
            }
        }

        if (noMoreSplit
                && pendingSplits.isEmpty()
                && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            context.signalNoMoreElement();
        }
    }

    @Override
    public List<ChromaDBSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<ChromaDBSourceSplit> splits) {
        log.info("Adding {} splits to reader", splits.size());
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("No more splits to read");
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // No action needed
    }

    // ── Reading logic (per collection) ──

    private void readCollection(
            Collector<SeaTunnelRow> output,
            String collectionId,
            TableSchema tableSchema,
            String tableId) {
        int idSplitSize = config.get(ChromaDBSourceConfig.ID_SPLIT_SIZE);
        int batchSize = config.get(ChromaDBSourceConfig.BATCH_SIZE);
        int offset = 0;
        long totalRead = 0;

        while (true) {
            ChromaDBClient.GetRecordsResult idResult =
                    client.getRecords(
                            collectionId,
                            new ChromaDBClient.GetRecordsRequest()
                                    .setLimit(idSplitSize)
                                    .setOffset(offset)
                                    .setInclude(Collections.emptyList()));
            if (idResult == null) {
                throw new ChromaDBConnectorException(
                        ChromaDBConnectorErrorCode.READ_DATA_FAIL,
                        "Received null response from ChromaDB for collection '"
                                + collectionId
                                + "' at offset "
                                + offset
                                + ". This may indicate a proxy or load balancer issue.");
            }
            List<String> idSplit = idResult.getIds();

            if (idSplit == null || idSplit.isEmpty()) {
                break;
            }

            log.info(
                    "Phase 1: fetched ID split [{}-{}], {} IDs",
                    offset,
                    offset + idSplit.size(),
                    idSplit.size());

            fetchDataInBatches(output, collectionId, tableSchema, tableId, idSplit, batchSize);

            totalRead += idSplit.size();

            if (idSplit.size() < idSplitSize) {
                break;
            }
            offset += idSplitSize;
        }

        log.info("Read complete for collection: {} total records", totalRead);
    }

    private void fetchDataInBatches(
            Collector<SeaTunnelRow> output,
            String collectionId,
            TableSchema tableSchema,
            String tableId,
            List<String> ids,
            int batchSize) {
        for (int i = 0; i < ids.size(); i += batchSize) {
            int end = Math.min(i + batchSize, ids.size());
            List<String> chunk = ids.subList(i, end);

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
                                + "', batch offset "
                                + i
                                + ". This may indicate a proxy or load balancer issue.");
            }

            emitRows(output, tableSchema, tableId, result);
        }
    }

    private void emitRows(
            Collector<SeaTunnelRow> output,
            TableSchema tableSchema,
            String tableId,
            ChromaDBClient.GetRecordsResult result) {
        List<String> ids = result.getIds();
        List<List<Float>> embeddings = result.getEmbeddings();
        List<String> documents = result.getDocuments();
        List<String> uris = result.getUris();
        List<Map<String, Object>> metadatas = result.getMetadatas();

        for (int i = 0; i < ids.size(); i++) {
            try {
                String id = ids.get(i);
                List<Float> embedding =
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
