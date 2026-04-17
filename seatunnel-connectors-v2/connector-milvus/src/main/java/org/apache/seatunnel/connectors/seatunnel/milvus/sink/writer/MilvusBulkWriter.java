package org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import io.milvus.bulkwriter.RemoteBulkWriter;
import io.milvus.bulkwriter.RemoteBulkWriterParam;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.connect.AzureConnectParam;
import io.milvus.bulkwriter.connect.S3ConnectParam;
import io.milvus.bulkwriter.connect.StorageConnectParam;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusCommonConfig.URL;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.external.dto.StageBucket;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.BULK_WRITER_CONFIG;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.FIELD_SCHEMA;

import org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog.MilvusFieldSchema;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusImport;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusSinkConverter;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer.dispatcher.BulkWriterDispatcher;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer.dispatcher.BulkWriterDispatchers;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class MilvusBulkWriter implements MilvusWriter {
    private final DescribeCollectionResp describeCollectionResp;
    private final List<RemoteBulkWriter> remoteBulkWriters;
    private final List<BlockingQueue<SeaTunnelRow>> queues;
    private final List<Thread> workers;
    private final BulkWriterDispatcher dispatcher;
    private final int writerCount;
    MilvusImport milvusImport;

    MilvusSinkConverter milvusSinkConverter;

    private final CatalogTable catalogTable;
    private final ReadonlyConfig config;
    private final StageBucket stageBucket;
    private final Map<String, String> milvusFieldMapper;

    private final AtomicLong writeCache = new AtomicLong();
    private final AtomicLong writeCount = new AtomicLong();
    private final AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
    private volatile boolean closing = false;

    public MilvusBulkWriter(CatalogTable catalogTable, ReadonlyConfig config, StageBucket stageBucket,
                            DescribeCollectionResp describeCollectionResp, String partitionName) {
        this.catalogTable = catalogTable;
        this.config = config;
        this.stageBucket = stageBucket;

        this.milvusSinkConverter = MilvusSinkConverter.fromConfig(config);
        this.describeCollectionResp = describeCollectionResp;

        Gson gson = new Gson();
        Type type = new TypeToken<List<MilvusFieldSchema>>() {}.getType();
        List<MilvusFieldSchema> fieldSchemaList = gson.fromJson(gson.toJson(config.get(FIELD_SCHEMA)), type);

        this.milvusFieldMapper = new HashMap<>();
        if (fieldSchemaList != null) {
            for (MilvusFieldSchema field : fieldSchemaList) {
                String sourceFieldName = field.getSourceFieldName();
                if (sourceFieldName != null) {
                    milvusFieldMapper.put(sourceFieldName, field.getFieldName());
                }
            }
        }

        String collectionName = catalogTable.getTablePath().getTableName();
        StorageConnectParam storageConnectParam;
        if(Objects.equals(stageBucket.getCloudId(), "az") || Objects.equals(stageBucket.getCloudId(), "azure")){
            String connectionStr = "DefaultEndpointsProtocol=https;AccountName=" + stageBucket.getAccessKey() +
                    ";AccountKey=" + stageBucket.getSecretKey() + ";EndpointSuffix=core.windows.net";
            storageConnectParam = AzureConnectParam.newBuilder()
                    .withConnStr(connectionStr)
                    .withContainerName(stageBucket.getBucketName())
                    .build();
        }else {
            storageConnectParam = S3ConnectParam.newBuilder()
                    .withEndpoint(stageBucket.getMinioUrl())
                    .withRegion(stageBucket.getRegionId())
                    .withAccessKey(stageBucket.getAccessKey())
                    .withSecretKey(stageBucket.getSecretKey())
                    .withBucketName(stageBucket.getBucketName())
                    .withCloudName(stageBucket.getCloudId())
                    .build();
        }

        Map<String, String> bulkCfg = config.get(BULK_WRITER_CONFIG);
        int parallelism = 1;
        String rawParallelism = bulkCfg.get("writer_parallelism");
        if (rawParallelism != null && !rawParallelism.trim().isEmpty()) {
            parallelism = Math.max(1, Integer.parseInt(rawParallelism.trim()));
        }
        this.writerCount = parallelism;
        this.dispatcher = BulkWriterDispatchers.create(
                bulkCfg.getOrDefault("writer_dispatch_strategy", "round_robin"));
        // Bounded queue size controls memory headroom per writer (each slot holds
        // one SeaTunnelRow reference). write() blocks on queue.put() when the
        // queue is full, providing natural backpressure upstream — only the
        // worker thread ever touches the RemoteBulkWriter, so non-thread-safe
        // SDK state is preserved.
        int queueCapacity = 1024;
        String rawQueue = bulkCfg.get("writer_queue_capacity");
        if (rawQueue != null && !rawQueue.trim().isEmpty()) {
            queueCapacity = Math.max(1, Integer.parseInt(rawQueue.trim()));
        }
        this.remoteBulkWriters = new ArrayList<>(parallelism);
        this.queues = new ArrayList<>(parallelism);
        this.workers = new ArrayList<>(parallelism);

        String basePath = stageBucket.getPrefix() + "/" + collectionName + "/" + partitionName;
        try {
            for (int i = 0; i < parallelism; i++) {
                // Suffix per-writer path so concurrent writers don't collide on chunk files.
                String remotePath = parallelism == 1 ? basePath : basePath + "/w" + i;
                RemoteBulkWriterParam param = RemoteBulkWriterParam.newBuilder()
                        .withCollectionSchema(describeCollectionResp.getCollectionSchema())
                        .withConnectParam(storageConnectParam)
                        .withChunkSize(stageBucket.getChunkSize() * 1024 * 1024)
                        .withRemotePath(remotePath)
                        .withFileType(BulkFileType.PARQUET)
                        .build();
                RemoteBulkWriter writer = new RemoteBulkWriter(param);
                remoteBulkWriters.add(writer);

                BlockingQueue<SeaTunnelRow> queue = new ArrayBlockingQueue<>(queueCapacity);
                queues.add(queue);

                final int writerIdx = i;
                Thread worker = new Thread(
                        () -> runWorker(writer, queue),
                        "milvus-bulk-writer-" + collectionName + "-" + partitionName + "-w" + writerIdx);
                worker.setDaemon(true);
                worker.start();
                workers.add(worker);
            }
            if (stageBucket.getAutoImport()) {
                milvusImport = new MilvusImport(config.get(URL), config.get(DATABASE), collectionName, partitionName, stageBucket);
            }
        } catch (IOException e) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.INIT_WRITER_ERROR, e);
        }

        log.info("MilvusBulkWriter initialized: collection={}, partition={}, writerParallelism={}, dispatcher={}, queueCapacity={}",
                collectionName, partitionName, parallelism, dispatcher.name(), queueCapacity);
    }

    private void runWorker(RemoteBulkWriter writer, BlockingQueue<SeaTunnelRow> queue) {
        try {
            while (true) {
                SeaTunnelRow row = queue.poll(200, TimeUnit.MILLISECONDS);
                if (row == null) {
                    if (closing && queue.isEmpty()) {
                        return;
                    }
                    continue;
                }
                JsonObject data = milvusSinkConverter.buildMilvusData(
                        catalogTable, describeCollectionResp, milvusFieldMapper, row);
                writer.appendRow(data);
                writeCache.incrementAndGet();
                writeCount.incrementAndGet();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            asyncFailure.compareAndSet(null, e);
        } catch (Throwable t) {
            asyncFailure.compareAndSet(null, t);
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException, InterruptedException {
        Throwable failure = asyncFailure.get();
        if (failure != null) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.WRITE_ERROR, failure);
        }
        int idx = dispatcher.route(element, writerCount);
        queues.get(idx).put(element);
    }

    @Override
    public void commit(Boolean async) {
    }

    @Override
    public boolean needCommit() {
        return false;
    }

    @Override
    public void close() throws Exception {
        // Signal workers to exit after draining their queues.
        closing = true;
        for (Thread t : workers) {
            t.join(TimeUnit.HOURS.toMillis(1));
            if (t.isAlive()) {
                throw new MilvusConnectorException(MilvusConnectionErrorCode.WRITE_ERROR,
                        "timeout waiting for bulk writer worker to drain: " + t.getName());
            }
        }
        Throwable failure = asyncFailure.get();
        if (failure != null) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.WRITE_ERROR, failure);
        }
        // Close each writer to flush remaining rows as final chunks.
        for (RemoteBulkWriter writer : remoteBulkWriters) {
            writer.close();
        }

        // Collect every produced folder across all writers. Each writer may have
        // multiple chunk files; one importFolder call per unique folder is enough
        // since Milvus import dedupes by objectUrl internally.
        Set<String> folders = new LinkedHashSet<>();
        boolean anyFiles = false;
        for (RemoteBulkWriter writer : remoteBulkWriters) {
            List<List<String>> batches = writer.getBatchFiles();
            if (batches == null || batches.isEmpty()) {
                continue;
            }
            for (List<String> batch : batches) {
                for (String obj : batch) {
                    int lastSlash = obj.lastIndexOf('/');
                    if (lastSlash < 0) {
                        continue;
                    }
                    folders.add(obj.substring(0, lastSlash + 1));
                    anyFiles = true;
                }
            }
        }
        if (!anyFiles) {
            log.warn("No data uploaded to remote");
        }

        if (stageBucket.getAutoImport() && milvusImport != null) {
            for (String folder : folders) {
                milvusImport.importFolder(folder);
            }
            log.info("[MILVUS_IMPORT_SUMMARY] collection={}, partition={}, importJobCount={}, importJobs={}",
                    catalogTable.getTablePath().getTableName(),
                    describeCollectionResp.getCollectionName(),
                    milvusImport.getImportJobCount(),
                    milvusImport.getImportJobsInfo());
        }
    }

    @Override
    public long getWriteCache() {
        return this.writeCache.get();
    }

    @Override
    public void waitJobFinish() {
        if(stageBucket.getAutoImport()) {
            log.info("Waiting for Milvus import jobs to complete...");
            milvusImport.waitImportFinish();
            log.info("[MILVUS_IMPORT_COMPLETE] All import jobs completed. Total jobs: {}", milvusImport.getImportJobCount());
        }
    }

    public Map<String, String> getImportJobIds() {
        if (stageBucket.getAutoImport() && milvusImport != null) {
            return milvusImport.getImportJobIds();
        }
        return new HashMap<>();
    }

    public int getImportJobCount() {
        if (stageBucket.getAutoImport() && milvusImport != null) {
            return milvusImport.getImportJobCount();
        }
        return 0;
    }
}
