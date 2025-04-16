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
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import static org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusCommonConfig.URL;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog.MilvusField;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.common.StageBucket;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.EXTRACT_DYNAMIC;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusConnectorUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusSinkConverter;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class MilvusBulkWriter implements MilvusWriter {
    private final DescribeCollectionResp describeCollectionResp;
    RemoteBulkWriter remoteBulkWriter;
    MilvusImport milvusImport;

    MilvusSinkConverter milvusSinkConverter;
    private final List<String> jsonFieldNames;
    private final String dynamicFieldName;

    private final CatalogTable catalogTable;
    private final ReadonlyConfig config;
    private final StageBucket stageBucket;
    private final List<MilvusField> milvusFields;

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
        this.describeCollectionResp = describeCollectionResp;

        Gson gson = new Gson();
        Type type = new TypeToken<List<MilvusField>>() {}.getType();
        this.milvusFields = gson.fromJson(gson.toJson(config.get(EXTRACT_DYNAMIC)), type);

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
                    .withCloudName(stageBucket.getCloudId())
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
                        catalogTable, describeCollectionResp.getAutoID(),
                describeCollectionResp.getEnableDynamicField(), jsonFieldNames, dynamicFieldName, milvusFields, element);
        remoteBulkWriter.appendRow(data);
        writeCache.incrementAndGet();
        writeCount.incrementAndGet();
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
        // trigger import after all data write done
        remoteBulkWriter.close();
        if(remoteBulkWriter.getBatchFiles().isEmpty()){
            log.warn("No data uploaded to remote");
            return;
        }
        if(stageBucket.getAutoImport()) {
            String object = remoteBulkWriter.getBatchFiles().get(0).get(0);
            String objectFolder = object.substring(0, object.lastIndexOf("/")+1);
            milvusImport.importFolder(objectFolder);
        }
    }

    @Override
    public long getWriteCache() {
        return this.writeCache.get();
    }

    @Override
    public void waitJobFinish() {
        if(stageBucket.getAutoImport()) {
            milvusImport.waitImportFinish();
        }
    }
}
