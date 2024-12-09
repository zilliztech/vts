package org.apache.seatunnel.connectors.seatunnel.milvus.source;

import io.milvus.orm.iterator.QueryIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConverter;
import org.codehaus.plexus.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class MilvusBufferReader {
    private final Collector<SeaTunnelRow> output;
    private final MilvusSourceConverter milvusSourceConverter;
    private final MilvusClientV2 milvusClient;
    private final String collectionName;
    private final String partitionName;
    private final Long offset;
    private final Long limit;
    private final TableSchema tableSchema;
    private final CountDownLatch completionSignal = new CountDownLatch(1);

    public MilvusBufferReader(MilvusSourceSplit split, Collector<SeaTunnelRow> output,
                              MilvusClientV2 client, TableSchema tableSchema) {
        this.output = output;
        this.milvusClient = client;
        this.tableSchema = tableSchema;
        this.milvusSourceConverter = new MilvusSourceConverter(tableSchema);
        this.collectionName = split.getTablePath().getTableName();
        this.partitionName = split.getPartitionName();
        this.offset = split.getOffset();
        this.limit = split.getLimit();
    }

    public void pollData(Integer batchSize) {
        log.info("Starting to read data from Milvus, table schema: {}", tableSchema);

        Boolean loadState = milvusClient.getLoadState(
                GetLoadStateReq.builder()
                        .collectionName(collectionName)
                        .build());
        if (!loadState) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.COLLECTION_NOT_LOADED);
        }

        log.info("Collection '{}' is loaded. Starting query execution...", collectionName);
        // query iterate data in background
        try {
            queryIteratorData(collectionName, partitionName, batchSize, offset, limit);
        } catch (Exception e) {
            log.error("Error in queryIteratorData task: ", e);
            throw new MilvusConnectorException(MilvusConnectionErrorCode.READ_DATA_FAIL, e);
        }
    }

    private void queryIteratorData(String collectionName, String partitionName, long batchSize, Long offset, Long limit) throws InterruptedException {
        log.info("Querying data from Milvus: collection={}, batchSize={}, offset={}, limit={}", collectionName, batchSize, offset, limit);

        QueryIteratorReq queryIteratorReq = QueryIteratorReq.builder()
                .collectionName(collectionName)
                .outputFields(Lists.newArrayList("*"))
                .batchSize(batchSize)
                .build();

        if (StringUtils.isNotEmpty(partitionName)) {
            queryIteratorReq.setPartitionNames(Collections.singletonList(partitionName));
        }
        if (offset != null){
            queryIteratorReq.setOffset(offset);
        }
        if (limit != null){
            queryIteratorReq.setLimit(limit);
        }

        QueryIterator iterator = milvusClient.queryIterator(queryIteratorReq);

        int maxFailRetry = 3;

        while (maxFailRetry > 0) {
            try {
                List<QueryResultsWrapper.RowRecord> next = iterator.next();
                if (next == null || next.isEmpty()) {
                    log.info("No more records in iterator");
                    completionSignal.countDown();
                    break;
                } else {
                    for (QueryResultsWrapper.RowRecord record : next) {
                        SeaTunnelRow seaTunnelRow = milvusSourceConverter.convertToSeaTunnelRow(record, tableSchema, collectionName, partitionName);
                        output.collect(seaTunnelRow);
                    }
                }
            } catch (Exception e) {
                if (e.getMessage().contains("rate limit exceeded")) {
                    maxFailRetry--;
                    log.warn("Rate limit exceeded. Retrying in 30 seconds. Retries left: {}", maxFailRetry);
                    Thread.sleep(30000);
                } else {
                    log.error("Query failed. Batch size: {}. Aborting...", batchSize, e);
                    throw new RuntimeException("Query failed", e);
                }
            }
        }

        log.info("Query execution completed for collection '{}'", collectionName);
    }
}