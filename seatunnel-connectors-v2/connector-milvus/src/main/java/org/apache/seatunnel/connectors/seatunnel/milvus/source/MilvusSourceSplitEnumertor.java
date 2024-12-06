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

package org.apache.seatunnel.connectors.seatunnel.milvus.source;

import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.ShowPartitionsResponse;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.partition.ShowPartitionsParam;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.partition.request.ListPartitionsReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.config.MilvusSourceConfig;

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
public class MilvusSourceSplitEnumertor
        implements SourceSplitEnumerator<MilvusSourceSplit, MilvusSourceState> {

    private final Map<TablePath, CatalogTable> tables;
    private final Context<MilvusSourceSplit> context;
    private final ConcurrentLinkedQueue<TablePath> pendingTables;
    private final Map<Integer, List<MilvusSourceSplit>> pendingSplits;
    private final Object stateLock = new Object();
    private MilvusClientV2 client = null;
    private Integer parallelism;

    private final ReadonlyConfig config;

    public MilvusSourceSplitEnumertor(
            Context<MilvusSourceSplit> context,
            ReadonlyConfig config,
            Map<TablePath, CatalogTable> sourceTables,
            MilvusSourceState sourceState) {
        this.context = context;
        this.tables = sourceTables;
        this.config = config;
        if (sourceState == null) {
            this.pendingTables = new ConcurrentLinkedQueue<>(tables.keySet());
            this.pendingSplits = new HashMap<>();
        } else {
            this.pendingTables = new ConcurrentLinkedQueue<>(sourceState.getPendingTables());
            this.pendingSplits = new HashMap<>(sourceState.getPendingSplits());
        }
    }

    @Override
    public void open() {
        ConnectConfig connectConfig =
                ConnectConfig.builder()
                        .uri(config.get(MilvusSourceConfig.URL))
                        .token(config.get(MilvusSourceConfig.TOKEN))
                        .dbName(config.get(MilvusSourceConfig.DATABASE))
                        .build();
        this.client = new MilvusClientV2(connectConfig);
        parallelism = config.get(MilvusSourceConfig.PARALLELISM);
    }

    @Override
    public void run() throws Exception {
        log.info("Starting milvus split enumerator.");
        Set<Integer> readers = context.registeredReaders();
        while (!pendingTables.isEmpty()) {
            synchronized (stateLock) {
                TablePath tablePath = pendingTables.poll();
                log.info("begin to split table path: {}", tablePath);
                Collection<MilvusSourceSplit> splits = generateSplits(tables.get(tablePath));
                log.info("end to split table {} into {} splits.", tablePath, splits.size());

                addPendingSplit(splits);
            }

            synchronized (stateLock) {
                assignSplit(readers);
            }
        }

        log.info("No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    private Collection<MilvusSourceSplit> generateSplits(CatalogTable table) {
        log.info("Start splitting collection {} into chunks by partition...", table.getTablePath());
        String collection = table.getTablePath().getTableName();
        DescribeCollectionResp describeCollectionResp =
                client.describeCollection(
                        DescribeCollectionReq.builder()
                                .collectionName(collection)
                                .build());
        boolean hasPartitionKey =
                describeCollectionResp.getCollectionSchema().getFieldSchemaList().stream()
                        .anyMatch(CreateCollectionReq.FieldSchema::getIsPartitionKey);

        List<MilvusSourceSplit> milvusSourceSplits = new ArrayList<>();
        ListPartitionsReq listPartitionsReq =
                ListPartitionsReq.builder()
                        .collectionName(collection)
                        .build();
        List<String> partitionList = client.listPartitions(listPartitionsReq);
        if (!hasPartitionKey && partitionList.size() > 1) {
            // more than 1 partition
            for (String partitionName : partitionList) {
                MilvusSourceSplit milvusSourceSplit =
                        MilvusSourceSplit.builder()
                                .tablePath(table.getTablePath())
                                .splitId(String.format("%s-%s", table.getTablePath(), partitionName))
                                .partitionName(partitionName)
                                .build();
                List<MilvusSourceSplit> milvusSourceSplitsInParallel = splitByOffset(milvusSourceSplit, parallelism);
                milvusSourceSplits.addAll(milvusSourceSplitsInParallel);
            }
        } else {
            MilvusSourceSplit milvusSourceSplit =
                    MilvusSourceSplit.builder()
                            .tablePath(table.getTablePath())
                            .splitId(String.format("%s", table.getTablePath()))
                            .collectionName(table.getTablePath().getTableName())
                            .build();
            List<MilvusSourceSplit> milvusSourceSplitsInParallel = splitByOffset(milvusSourceSplit, parallelism);
            milvusSourceSplits.addAll(milvusSourceSplitsInParallel);
        }
        return milvusSourceSplits;
    }

    private List<MilvusSourceSplit> splitByOffset(MilvusSourceSplit split, Integer parallelism) {
        if(parallelism < 2){
            return Collections.singletonList(split);
        }
        QueryReq queryReq = QueryReq.builder()
                .collectionName(split.getCollectionName())
                .filter("")
                .outputFields(Collections.singletonList("count(*)"))
                .build();

        if (StringUtils.isNotEmpty(split.getPartitionName())) {
            queryReq.setPartitionNames(Collections.singletonList(split.getPartitionName()));
        }

        QueryResp queryResp = client.query(queryReq);
        long numOfEntities = (long) queryResp.getQueryResults().get(0).getEntity().get("count(*)");

        log.info("Total records num: " + numOfEntities);
        List<MilvusSourceSplit> splits = new ArrayList<>();

        // Calculate the size of each split
        long splitSize = Math.max(1, numOfEntities / parallelism);

        for (int i = 0; i < parallelism; i++) {
            long offset = i * splitSize;

            // For the final batch, don't set a limit
            if (i == parallelism - 1) {
                MilvusSourceSplit newSplit = MilvusSourceSplit.builder()
                        .tablePath(split.getTablePath())
                        .splitId(String.format("%s-offset-%d", split.getCollectionName(), offset))
                        .collectionName(split.getCollectionName())
                        .partitionName(split.getPartitionName())
                        .offset(offset)
                        .build();
                splits.add(newSplit);

                log.info("Generated final split: {}", newSplit);
            } else {
                MilvusSourceSplit newSplit = MilvusSourceSplit.builder()
                        .tablePath(split.getTablePath())
                        .splitId(String.format("%s-offset-%d-limit-%d", split.getCollectionName(), offset, splitSize))
                        .collectionName(split.getCollectionName())
                        .partitionName(split.getPartitionName())
                        .offset(offset)
                        .limit(splitSize)
                        .build();
                splits.add(newSplit);

                log.info("Generated split: {}", newSplit);
            }
        }

        return splits;

    }

    private void addPendingSplit(Collection<MilvusSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (MilvusSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            log.info("Assigning {} to {} reader.", split, ownerReader);

            pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    private void assignSplit(Collection<Integer> readers) {
        log.info("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<MilvusSourceSplit> assignmentForReader = pendingSplits.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.debug("Assign splits {} to reader {}", assignmentForReader, reader);
                context.assignSplit(reader, assignmentForReader);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void addSplitsBack(List<MilvusSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            synchronized (stateLock) {
                addPendingSplit(splits, subtaskId);
                if (context.registeredReaders().contains(subtaskId)) {
                    assignSplit(Collections.singletonList(subtaskId));
                } else {
                    log.warn(
                            "Reader {} is not registered. Pending splits {} are not assigned.",
                            subtaskId,
                            splits);
                }
            }
        }
        log.info("Add back splits {} to JdbcSourceSplitEnumerator.", splits.size());
    }

    private void addPendingSplit(Collection<MilvusSourceSplit> splits, int ownerReader) {
        pendingSplits.computeIfAbsent(ownerReader, r -> new ArrayList<>()).addAll(splits);
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingTables.isEmpty() && pendingSplits.isEmpty() ? 0 : 1;
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new MilvusConnectorException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                String.format("Unsupported handleSplitRequest: %d", subtaskId));
    }

    @Override
    public void registerReader(int subtaskId) {
        log.info("Register reader {} to MilvusSourceSplitEnumerator.", subtaskId);
        if (!pendingSplits.isEmpty()) {
            synchronized (stateLock) {
                assignSplit(Collections.singletonList(subtaskId));
            }
        }
    }

    @Override
    public MilvusSourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new MilvusSourceState(
                    new ArrayList(pendingTables), new HashMap<>(pendingSplits));
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
