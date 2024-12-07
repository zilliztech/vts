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

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.partition.request.ListPartitionsReq;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.seatunnel.common.constants.CommonOptions;
import org.apache.seatunnel.connectors.seatunnel.milvus.catalog.MilvusOptions;
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
        MilvusClientV2 client = new MilvusClientV2(
                        ConnectConfig.builder()
                                .uri(config.get(MilvusSourceConfig.URL))
                                .token(config.get(MilvusSourceConfig.TOKEN))
                                .dbName(dbName)
                                .build());

        List<String> collectionList = new ArrayList<>();
        if (!config.get(MilvusSourceConfig.COLLECTION).isEmpty()) {
            collectionList.addAll(config.get(MilvusSourceConfig.COLLECTION));
        } else {
            ListCollectionsResp response =
                    client.listCollections();
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

        log.info("describe collection database: {}, collection: {}, response: {}", database, collection, describeCollectionResp);
        // collection column
        CreateCollectionReq.CollectionSchema schema = describeCollectionResp.getCollectionSchema();
        List<Column> columns = new ArrayList<>();
        boolean existPartitionKeyField = false;
        String partitionKeyField = null;

        for (CreateCollectionReq.FieldSchema fieldSchema : schema.getFieldSchemaList()) {
            PhysicalColumn physicalColumn = MilvusSourceConverter.convertColumn(fieldSchema);
            columns.add(physicalColumn);
            if (fieldSchema.getIsPartitionKey()) {
                existPartitionKeyField = true;
                partitionKeyField = fieldSchema.getName();
            }
        }
        if (describeCollectionResp.getEnableDynamicField()) {
            Map<String, Object> options = new HashMap<>();

            options.put(CommonOptions.METADATA.getName(), true);
            PhysicalColumn dynamicColumn =
                    PhysicalColumn.builder()
                            .name(CommonOptions.METADATA.getName())
                            .dataType(STRING_TYPE)
                            .options(options)
                            .build();
            columns.add(dynamicColumn);
        }

        // primary key
        PrimaryKey primaryKey = buildPrimaryKey(schema.getFieldSchemaList());

        //vector info
        ConstraintKey constraintKey = getConstraintKeyColumns(client, collection);

        // build tableSchema
        TableSchema tableSchema = TableSchema.builder()
                                    .columns(columns)
                                    .primaryKey(primaryKey)
                                    .constraintKey(constraintKey)
                                    .build();

        // build tableId
        String CATALOG_NAME = MilvusOptions.MILVUS;
        TableIdentifier tableId = TableIdentifier.of(CATALOG_NAME, database, null, collection);
        // build options info
        Map<String, String> options = new HashMap<>();
        options.put(MilvusOptions.ENABLE_DYNAMIC_FIELD, String.valueOf(describeCollectionResp.getEnableDynamicField()));
        if (existPartitionKeyField) {
            options.put(MilvusOptions.PARTITION_KEY_FIELD, partitionKeyField);
        }else {
            options.put(MilvusOptions.PARTITION_NAMES, getPartitionNames(client, collection));
        }

        return CatalogTable.of(tableId, tableSchema, options, new ArrayList<>(), describeCollectionResp.getDescription());
    }

    private String getPartitionNames(MilvusClientV2 client, String collection) {
        ListPartitionsReq listPartitionsReq = ListPartitionsReq.builder()
                .collectionName(collection)
                .build();
        List<String> partitionNames = client.listPartitions(listPartitionsReq);
        return String.join(",", partitionNames);
    }

    private static @NotNull ConstraintKey getConstraintKeyColumns(MilvusClientV2 client, String collection) {
        List<ConstraintKey.ConstraintKeyColumn> constraintKeyColumns = new ArrayList<>();

        List<String> indexes = client.listIndexes(ListIndexesReq.builder().collectionName(collection).build());
        for (String index : indexes) {
            DescribeIndexReq describeIndexReq = DescribeIndexReq.builder()
                    .collectionName(collection)
                    .indexName(index)
                    .build();
            DescribeIndexResp describeIndexResp = client.describeIndex(describeIndexReq);

            for (DescribeIndexResp.IndexDesc indexDesc : describeIndexResp.getIndexDescriptions()) {
                VectorIndex vectorIndex = new VectorIndex(indexDesc.getIndexName(), indexDesc.getFieldName(),
                        indexDesc.getIndexType().getName(), indexDesc.getMetricType().name());
                constraintKeyColumns.add(vectorIndex);
            }
        }
        return ConstraintKey.of(ConstraintKey.ConstraintType.VECTOR_INDEX_KEY,
                "vector_index_key", constraintKeyColumns);
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
