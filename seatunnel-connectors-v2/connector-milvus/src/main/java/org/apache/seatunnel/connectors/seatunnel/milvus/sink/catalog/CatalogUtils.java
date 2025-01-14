package org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.VectorIndex;
import org.apache.seatunnel.common.constants.CommonOptions;
import org.apache.seatunnel.connectors.seatunnel.milvus.catalog.MilvusOptions;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.ENABLE_DYNAMIC_FIELD;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusSinkConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CatalogUtils {
    private final MilvusClientV2 client;
    private final ReadonlyConfig config;

    CatalogUtils(MilvusClientV2 client, ReadonlyConfig config) {
        this.client = client;
        this.config = config;
    }

    void createIndex(TablePath tablePath, TableSchema tableSchema){
        ConstraintKey constraintKey = tableSchema.getConstraintKeys().stream().filter(constraintKey1 -> constraintKey1.getConstraintType().equals(ConstraintKey.ConstraintType.VECTOR_INDEX_KEY)).findFirst().orElse(null);
        List<IndexParam> indexParams = new ArrayList<>();
        if (constraintKey != null) {
            constraintKey.getColumnNames().forEach(constraintKeyColumn -> {
                VectorIndex vectorIndex = (VectorIndex) constraintKeyColumn;
                IndexParam indexParam = IndexParam.builder()
                        .fieldName(vectorIndex.getColumnName())
                        .metricType(IndexParam.MetricType.valueOf(vectorIndex.getMetricType().name()))
                        .indexType(IndexParam.IndexType.AUTOINDEX)
                        .indexName(vectorIndex.getIndexName())
                        .build();
                indexParams.add(indexParam);
            });
        }
        // create index
        CreateIndexReq createIndexReq = CreateIndexReq.builder()
                .collectionName(tablePath.getTableName())
                .indexParams(indexParams)
                .build();
        this.client.createIndex(createIndexReq);
    }

    void createTableInternal(TablePath tablePath, CatalogTable catalogTable) {
        try {
            Map<String, String> options = catalogTable.getOptions();

            // partition key logic
            String partitionKeyField = null;
            if(options.containsKey(MilvusOptions.PARTITION_KEY_FIELD)){
                partitionKeyField = options.get(MilvusOptions.PARTITION_KEY_FIELD);
            }
            // if partition key is set in config, use the one in config
            if (StringUtils.isNotEmpty(config.get(MilvusSinkConfig.PARTITION_KEY))) {
                partitionKeyField = config.get(MilvusSinkConfig.PARTITION_KEY);
            }

            TableSchema tableSchema = catalogTable.getTableSchema();
            List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
            Boolean enableAutoId = false;
            if(options.containsKey(MilvusOptions.ENABLE_AUTO_ID)){
                enableAutoId = Boolean.valueOf(options.get(MilvusOptions.ENABLE_AUTO_ID));
            }
            if(config.get(MilvusSinkConfig.ENABLE_AUTO_ID) != null){
                enableAutoId = config.get(MilvusSinkConfig.ENABLE_AUTO_ID);
            }
            for (Column column : tableSchema.getColumns()) {
                if (column.getOptions() != null
                        && column.getOptions().containsKey(CommonOptions.METADATA.getName())
                        && (Boolean) column.getOptions().get(CommonOptions.METADATA.getName())) {
                    // skip dynamic field
                    continue;
                }
                CreateCollectionReq.FieldSchema fieldSchema = MilvusSinkConverter.convertToFieldType(
                        column,
                        tableSchema.getPrimaryKey(),
                        partitionKeyField,
                        enableAutoId);
                fieldSchemaList.add(fieldSchema);
            }

            Boolean enableDynamicField = true;

            if(options.containsKey(MilvusOptions.ENABLE_DYNAMIC_FIELD)) {
                enableDynamicField = Boolean.valueOf(options.get(MilvusOptions.ENABLE_DYNAMIC_FIELD));
            }
            // if enable_dynamic_field is set in config, use the one in config
            if(config.get(ENABLE_DYNAMIC_FIELD) != null){
                enableDynamicField = config.get(ENABLE_DYNAMIC_FIELD);
            }

            // consistency level
            ConsistencyLevel consistencyLevel = ConsistencyLevel.BOUNDED;
            if(options.containsKey(MilvusOptions.CONSISTENCY_LEVEL)){
                consistencyLevel = ConsistencyLevel.valueOf(options.get(MilvusOptions.CONSISTENCY_LEVEL).toUpperCase());
            }
            if(config.get(MilvusSinkConfig.CONSISTENCY_LEVEL) != null){
                consistencyLevel = config.get(MilvusSinkConfig.CONSISTENCY_LEVEL);
            }

            String collectionDescription = "";
            if (config.get(MilvusSinkConfig.COLLECTION_DESCRIPTION) != null
                    && config.get(MilvusSinkConfig.COLLECTION_DESCRIPTION)
                    .containsKey(tablePath.getTableName())) {
                // use description from config first
                collectionDescription =
                        config.get(MilvusSinkConfig.COLLECTION_DESCRIPTION)
                                .get(tablePath.getTableName());
            } else if (null != catalogTable.getComment()) {
                collectionDescription = catalogTable.getComment();
            }
            CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                    .fieldSchemaList(fieldSchemaList)
                    .enableDynamicField(enableDynamicField)
                    .build();
            CreateCollectionReq createCollectionReq =
                    CreateCollectionReq.builder()
                            .collectionName(tablePath.getTableName())
                            .description(collectionDescription)
                            .collectionSchema(collectionSchema)
                            .enableDynamicField(enableDynamicField)
                            .consistencyLevel(consistencyLevel)
                            .build();
            if (StringUtils.isNotEmpty(options.get(MilvusOptions.SHARDS_NUM))) {
                createCollectionReq.setNumShards(Integer.parseInt(options.get(MilvusOptions.SHARDS_NUM)));
            }
            if(config.get(MilvusSinkConfig.SHARDS_NUM) != null){
                createCollectionReq.setNumShards(config.get(MilvusSinkConfig.SHARDS_NUM));
            }
            int retry = 5;
            while (retry > 0){
                try {
                    client.createCollection(createCollectionReq);
                    break;
                } catch (Exception e) {
                    log.error("create collection failed, retry: {}", retry);
                    retry--;
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException interruptedException) {
                        log.error("sleep failed", interruptedException);
                    }
                }
            }

        } catch (Exception e) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.CREATE_COLLECTION_ERROR, e);
        }
    }
}
