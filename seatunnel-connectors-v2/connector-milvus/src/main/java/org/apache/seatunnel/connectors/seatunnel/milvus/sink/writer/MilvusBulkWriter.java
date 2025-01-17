package org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer;

import com.google.gson.JsonObject;
import io.milvus.bulkwriter.RemoteBulkWriter;
import io.milvus.bulkwriter.RemoteBulkWriterParam;
import io.milvus.bulkwriter.common.clientenum.BulkFileType;
import io.milvus.bulkwriter.connect.AzureConnectParam;
import io.milvus.bulkwriter.connect.S3ConnectParam;
import io.milvus.bulkwriter.connect.StorageConnectParam;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusCommonConfig.URL;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.common.StageBucket;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.DATABASE;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusConnectorUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusSinkConverter;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class MilvusBulkWriter implements MilvusWriter {
    RemoteBulkWriter remoteBulkWriter;
    MilvusImport milvusImport;

    MilvusSinkConverter milvusSinkConverter;
    private final List<String> jsonFieldNames;
    private final String dynamicFieldName;

    private final CatalogTable catalogTable;
    private final ReadonlyConfig config;
    private final StageBucket stageBucket;

    private final AtomicLong writeCache = new AtomicLong();
    private final AtomicLong writeCount = new AtomicLong();

    public MilvusBulkWriter(CatalogTable catalogTable, ReadonlyConfig config, StageBucket stageBucket,
                            DescribeCollectionResp describeCollectionResp, String partitionName) {
        this.catalogTable = catalogTable;
        this.config = config;
        this.stageBucket = stageBucket;

        CollectionSchemaParam collectionSchemaParam = MilvusSinkConverter.convertToMilvusSchema(describeCollectionResp);

        this.milvusSinkConverter = new MilvusSinkConverter();
        this.dynamicFieldName = MilvusConnectorUtils.getDynamicField(catalogTable);
        this.jsonFieldNames = MilvusConnectorUtils.getJsonField(catalogTable);
        String collectionName = catalogTable.getTablePath().getTableName();
        StorageConnectParam storageConnectParam;
        if(Objects.equals(stageBucket.getCloudId(), "az")){
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
                    .build();
        }

        RemoteBulkWriterParam remoteBulkWriterParam = RemoteBulkWriterParam.newBuilder()
                .withCollectionSchema(collectionSchemaParam)
                .withConnectParam(storageConnectParam)
                .withChunkSize(stageBucket.getChunkSize() * 1024 * 1024)
                .withRemotePath(stageBucket.getPrefix() + "/" + collectionName + "/" + partitionName)
                .withFileType(BulkFileType.PARQUET)
                .build();

        try {
            remoteBulkWriter = new RemoteBulkWriter(remoteBulkWriterParam);
            if(stageBucket.getAutoImport()) {
                milvusImport = new MilvusImport(config.get(URL), config.get(DATABASE), collectionName, partitionName, stageBucket);
            }
        } catch (IOException e) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.INIT_WRITER_ERROR, e);
        }

    }
    @Override
    public void write(SeaTunnelRow element) throws IOException, InterruptedException {
        JsonObject data = milvusSinkConverter.buildMilvusData(
                        catalogTable, config, jsonFieldNames, dynamicFieldName, element);

        remoteBulkWriter.appendRow(data);
        writeCache.set(remoteBulkWriter.getBufferRowCount());
        writeCount.incrementAndGet();

    }
    @Override
    public void commit(Boolean async) throws InterruptedException {
        if(writeCache.get() == 0){
            return;
        }
        remoteBulkWriter.commit(async);
        writeCache.set(0);
        if(stageBucket.getAutoImport()) {
            milvusImport.importDatas(remoteBulkWriter.getBatchFiles());
        }
    }
    @Override
    public boolean needCommit() {
        return remoteBulkWriter.getBufferRowCount() == 500000;
    }

    @Override
    public void close() throws Exception {
        remoteBulkWriter.close();
        if(remoteBulkWriter.getBatchFiles().isEmpty()){
            log.info("No data uploaded to remote");
            throw new MilvusConnectorException(MilvusConnectionErrorCode.CLOSE_CLIENT_ERROR);
        }
        if(stageBucket.getAutoImport()) {
            milvusImport.importDatas(remoteBulkWriter.getBatchFiles());
            milvusImport.waitImportFinish();
        }
    }

    @Override
    public long getWriteCache() {
        return this.writeCache.get();
    }
}
