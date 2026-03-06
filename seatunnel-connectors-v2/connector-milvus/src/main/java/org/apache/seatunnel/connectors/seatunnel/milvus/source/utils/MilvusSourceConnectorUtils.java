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

package org.apache.seatunnel.connectors.seatunnel.milvus.source.utils;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.IndexParam.MetricType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.partition.request.ListPartitionsReq;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.util.Lists;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.VectorIndex;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.connectors.seatunnel.milvus.common.MilvusConstants;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusConnectorUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.config.MilvusSourceConfig;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class MilvusSourceConnectorUtils {
    private final ReadonlyConfig config;
    private final String dbName;

    public MilvusSourceConnectorUtils(ReadonlyConfig config) {
        this.config = config;
        this.dbName = config.get(MilvusSourceConfig.DATABASE);
    }

    public Map<TablePath, CatalogTable> getTables() {
        MilvusClientV2 client = new MilvusClientV2(MilvusConnectorUtils.getConnectConfig(config));

        List<String> collectionList = new ArrayList<>();
        if (!config.get(MilvusSourceConfig.COLLECTION).isEmpty()) {
            collectionList.addAll(config.get(MilvusSourceConfig.COLLECTION));
        } else {
            ListCollectionsResp response = client.listCollections();
            List<String> collectionNames = response.getCollectionNames();
            collectionList.addAll(collectionNames);
        }

        Map<TablePath, CatalogTable> map = new HashMap<>();
        for (String collection : collectionList) {
            CatalogTable catalogTable = getCatalogTable(client, dbName, collection);
            TablePath tablePath = TablePath.of(dbName, null, collection);
            map.put(tablePath, catalogTable);
        }
        client.close();
        return map;
    }

    private CatalogTable getCatalogTable(
            MilvusClientV2 client, String database, String collection) {
        DescribeCollectionResp describeCollectionResp = client.describeCollection(
                DescribeCollectionReq.builder()
                        .collectionName(collection)
                        .build());

        log.info("describe collection database: {}, collection: {}, response: {}", database, collection,
                describeCollectionResp);
        // collection column
        CreateCollectionReq.CollectionSchema schema = describeCollectionResp.getCollectionSchema();
        List<Column> columns = new ArrayList<>();
        boolean existPartitionKeyField = false;
        String partitionKeyField = null;

        // Convert regular fields
        for (CreateCollectionReq.FieldSchema fieldSchema : schema.getFieldSchemaList()) {
            PhysicalColumn physicalColumn = MilvusSourceConverter.convertColumn(fieldSchema);
            columns.add(physicalColumn);
            if (fieldSchema.getIsPartitionKey()) {
                existPartitionKeyField = true;
                partitionKeyField = fieldSchema.getName();
            }
        }

        // Convert struct fields (Array[Struct] fields are stored separately)
        List<CreateCollectionReq.StructFieldSchema> schemaStructFields = schema.getStructFields();
        if (schemaStructFields != null && !schemaStructFields.isEmpty()) {
            for (CreateCollectionReq.StructFieldSchema structFieldSchema : schemaStructFields) {
                PhysicalColumn structColumn = MilvusSourceConverter.convertStructFieldToColumn(structFieldSchema);
                columns.add(structColumn);
                log.info("Converted struct field to column: {}", structFieldSchema.getName());
            }
        }
        if (describeCollectionResp.getEnableDynamicField()) {
            Map<String, Object> options = new HashMap<>();

            options.put(CommonOptions.METADATA.getName(), true);
            PhysicalColumn dynamicColumn = PhysicalColumn.builder()
                    .name(CommonOptions.METADATA.getName())
                    .dataType(STRING_TYPE)
                    .options(options)
                    .build();
            columns.add(dynamicColumn);
        }
        List<CreateCollectionReq.Function> functionList = describeCollectionResp.getCollectionSchema()
                .getFunctionList();
        // primary key
        PrimaryKey primaryKey = buildPrimaryKey(schema.getFieldSchemaList());

        // build tableSchema
        TableSchema tableSchema = TableSchema.builder()
                .columns(columns)
                .primaryKey(primaryKey)
                .build();

        // build tableId
        String CATALOG_NAME = MilvusConstants.MILVUS;
        TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, database, null, collection);
        // build options info
        Map<String, String> options = new HashMap<>();
        options.put(MilvusConstants.ENABLE_DYNAMIC_FIELD,
                String.valueOf(describeCollectionResp.getEnableDynamicField()));
        options.put(MilvusConstants.ENABLE_AUTO_ID, String.valueOf(describeCollectionResp.getAutoID()));
        options.put(MilvusConstants.CONSISTENCY_LEVEL,
                String.valueOf(describeCollectionResp.getConsistencyLevel().getName()));
        // Serialize functionList as JSON for proper reconstruction in sink
        if (functionList != null && !functionList.isEmpty()) {
            try {
                com.google.gson.Gson gson = new com.google.gson.Gson();
                options.put(MilvusConstants.FUNCTION_LIST, gson.toJson(functionList));
            } catch (Exception e) {
                log.warn("Failed to serialize functionList, setting empty list. Error: {}", e.getMessage());
                options.put(MilvusConstants.FUNCTION_LIST, "[]");
            }
        } else {
            options.put(MilvusConstants.FUNCTION_LIST, "[]");
        }

        // Note: struct fields are serialized per-column in MilvusSourceConverter, not
        // at schema level

        // Serialize vector index info as JSON for proper reconstruction in sink
        List<Object> indexList = new ArrayList<>();
        try {
            List<String> indexes = client.listIndexes(ListIndexesReq.builder().collectionName(collection).build());
            for (String index : indexes) {
                DescribeIndexReq describeIndexReq = DescribeIndexReq.builder()
                        .collectionName(collection)
                        .indexName(index)
                        .build();
                try {
                    DescribeIndexResp describeIndexResp = client.describeIndex(describeIndexReq);
                    for (DescribeIndexResp.IndexDesc indexDesc : describeIndexResp.getIndexDescriptions()) {
                        Map<String, String> indexInfo = new HashMap<>();
                        indexInfo.put("fieldName", indexDesc.getFieldName());
                        indexInfo.put("indexName", indexDesc.getIndexName());
                        indexInfo.put("indexType", indexDesc.getIndexType().getName());
                        if (indexDesc.getMetricType() != MetricType.INVALID) {
                            indexInfo.put("metricType", indexDesc.getMetricType().name());
                        }
                        com.google.gson.Gson gson = new com.google.gson.Gson();
                        Map<String, Object> extraParams = new HashMap<>();
                        extraParams.put("params", gson.toJson(indexDesc.getExtraParams()));
                        indexInfo.put("extraParams", gson.toJson(extraParams));
                        indexList.add(indexInfo);
                    }
                } catch (Exception e) {
                    log.info("describe index error, maybe index not exist {}", e.getMessage());
                    continue;
                }
            }
            com.google.gson.Gson gson = new com.google.gson.Gson();
            options.put(MilvusConstants.INDEX_LIST, gson.toJson(indexList));
        } catch (Exception e) {
            log.warn("Failed to get index info, setting empty list. Error: {}", e.getMessage());
            options.put(MilvusConstants.INDEX_LIST, "[]");
        }
        options.put(MilvusConstants.SHARDS_NUM, String.valueOf(describeCollectionResp.getShardsNum()));
        if (describeCollectionResp.getProperties() != null
                && describeCollectionResp.getProperties().containsKey(MilvusConstants.TIMEZONE)) {
            options.put(MilvusConstants.TIMEZONE,
                    String.valueOf(describeCollectionResp.getProperties().get(MilvusConstants.TIMEZONE)));
        }
        if (existPartitionKeyField) {
            options.put(MilvusConstants.PARTITION_KEY_FIELD, partitionKeyField);
        } else {
            options.put(MilvusConstants.PARTITION_NAMES, getPartitionNames(client, collection));
        }

        return CatalogTable.of(tableId, tableSchema, options, new ArrayList<>(),
                describeCollectionResp.getDescription());
    }

    private String getPartitionNames(MilvusClientV2 client, String collection) {
        ListPartitionsReq listPartitionsReq = ListPartitionsReq.builder()
                .collectionName(collection)
                .build();
        List<String> partitionNames = client.listPartitions(listPartitionsReq);
        return String.join(",", partitionNames);
    }

    public static PrimaryKey buildPrimaryKey(List<CreateCollectionReq.FieldSchema> fields) {
        for (CreateCollectionReq.FieldSchema field : fields) {
            if (field.getIsPrimaryKey()) {
                return PrimaryKey.of(field.getName(), Lists.newArrayList(field.getName()), field.getAutoID());
            }
        }
        return null;
    }
}
