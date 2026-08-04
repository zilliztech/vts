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

package org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.schema;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.GeometryType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.MilvusCdcSourceConfigParser;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.MilvusCdcSourceTable;

import com.google.gson.Gson;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.partition.request.ListPartitionsReq;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.ANALYZER_PARAMS;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.AUTO_ID;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.CATALOG_NAME;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.CONSISTENCY_LEVEL;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.DEFAULT_VALUE;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.ELEMENT_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.EMPTY_JSON_ARRAY;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.ENABLE_ANALYZER;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.ENABLE_AUTO_ID;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.ENABLE_DYNAMIC_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.ENABLE_MATCH;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.FUNCTION_LIST;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.INDEX_LIST;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.IS_NULLABLE;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.IS_PARTITION_KEY;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.MAX_CAPACITY;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.MAX_LENGTH;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.MILVUS_INTERNAL_DYNAMIC_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.MULTI_ANALYZER_PARAMS;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.PARTITION_KEY_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.PARTITION_NAMES;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.PARTITION_NUM;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.SHARDS_NUM;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.STRUCT_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.TIMEZONE;

@Slf4j
public class MilvusCdcSchemaDiscovery {

    public MilvusCdcCollectionSchemaRegistry discover(
            ReadonlyConfig config, List<MilvusCdcSourceTable> sourceTables) {
        MilvusClientV2 client =
                new MilvusClientV2(MilvusCdcSourceConfigParser.parseConnectConfig(config));
        try {
            List<MilvusCdcCollectionSchema> schemas = new ArrayList<>(sourceTables.size());
            for (MilvusCdcSourceTable sourceTable : sourceTables) {
                DescribeCollectionResp describeCollectionResp =
                        client.describeCollection(
                                DescribeCollectionReq.builder()
                                        .databaseName(sourceTable.getDatabase())
                                        .collectionName(sourceTable.getCollection())
                                        .build());
                log.info(
                        "Milvus CDC described source collection {}.{}: {}",
                        sourceTable.getDatabase(),
                        sourceTable.getCollection(),
                        describeCollectionResp);
                CatalogTable catalogTable =
                        buildCatalogTable(
                                client,
                                sourceTable.getDatabase(),
                                sourceTable.getCollection(),
                                describeCollectionResp);
                schemas.add(buildSchema(sourceTable, catalogTable));
            }
            return new MilvusCdcCollectionSchemaRegistry(schemas);
        } finally {
            client.close();
        }
    }

    private MilvusCdcCollectionSchema buildSchema(
            MilvusCdcSourceTable sourceTable, CatalogTable catalogTable) {
        String primaryKeyField = null;
        int primaryKeyIndex = -1;
        PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        if (primaryKey != null && primaryKey.getColumnNames().size() == 1) {
            primaryKeyField = primaryKey.getColumnNames().get(0);
            String[] fieldNames = catalogTable.getSeaTunnelRowType().getFieldNames();
            for (int i = 0; i < fieldNames.length; i++) {
                if (fieldNames[i].equals(primaryKeyField)) {
                    primaryKeyIndex = i;
                    break;
                }
            }
        }
        if (primaryKeyIndex < 0) {
            throw new IllegalArgumentException(
                    "Milvus CDC requires source collection "
                            + sourceTable.getDatabase()
                            + "."
                            + sourceTable.getCollection()
                            + " to have a single primary key.");
        }
        boolean enableDynamicField =
                Boolean.parseBoolean(
                        catalogTable
                                .getOptions()
                                .getOrDefault(ENABLE_DYNAMIC_FIELD, Boolean.FALSE.toString()));
        if (enableDynamicField && !hasDynamicFieldColumn(catalogTable)) {
            throw new IllegalArgumentException(
                    "Milvus CDC source collection "
                            + sourceTable.getDatabase()
                            + "."
                            + sourceTable.getCollection()
                            + " enables dynamic field but catalog schema has no "
                            + MILVUS_INTERNAL_DYNAMIC_FIELD
                            + " metadata column.");
        }
        return MilvusCdcCollectionSchema.builder()
                .sourceDatabase(sourceTable.getDatabase())
                .sourceCollection(sourceTable.getCollection())
                .catalogTable(catalogTable)
                .rowType(catalogTable.getSeaTunnelRowType())
                .tableId(catalogTable.getTablePath().toString())
                .primaryKeyField(primaryKeyField)
                .primaryKeyIndex(primaryKeyIndex)
                .enableDynamicField(enableDynamicField)
                .build();
    }

    private boolean hasDynamicFieldColumn(CatalogTable catalogTable) {
        for (Column column : catalogTable.getTableSchema().getColumns()) {
            if (isDynamicFieldColumn(column)) {
                return true;
            }
        }
        return false;
    }

    private boolean isDynamicFieldColumn(Column column) {
        return column != null
                && MILVUS_INTERNAL_DYNAMIC_FIELD.equals(column.getName())
                && column.getOptions() != null
                && Boolean.TRUE.equals(column.getOptions().get(CommonOptions.METADATA.getName()));
    }

    private CatalogTable buildCatalogTable(
            MilvusClientV2 client,
            String database,
            String sourceCollection,
            DescribeCollectionResp describeCollectionResp) {
        CreateCollectionReq.CollectionSchema schema = describeCollectionResp.getCollectionSchema();
        List<Column> columns = new ArrayList<>();
        boolean hasPartitionKey = false;
        String partitionKeyField = null;

        for (CreateCollectionReq.FieldSchema fieldSchema : schema.getFieldSchemaList()) {
            // Milvus includes its internal dynamic field in DescribeCollection, but the SDK field
            // model does not retain the isDynamic flag. Normalize it into the metadata column
            // below instead of exposing it as a regular JSON column.
            if (MILVUS_INTERNAL_DYNAMIC_FIELD.equals(fieldSchema.getName())) {
                continue;
            }
            columns.add(convertColumn(fieldSchema));
            if (Boolean.TRUE.equals(fieldSchema.getIsPartitionKey())) {
                hasPartitionKey = true;
                partitionKeyField = fieldSchema.getName();
            }
        }

        List<CreateCollectionReq.StructFieldSchema> structFields = schema.getStructFields();
        if (structFields != null) {
            for (CreateCollectionReq.StructFieldSchema structField : structFields) {
                columns.add(convertStructField(structField));
            }
        }

        if (describeCollectionResp.getEnableDynamicField()) {
            Map<String, Object> options = new HashMap<>();
            options.put(CommonOptions.METADATA.getName(), true);
            columns.add(
                    PhysicalColumn.builder()
                            .name(MILVUS_INTERNAL_DYNAMIC_FIELD)
                            .dataType(BasicType.STRING_TYPE)
                            .options(options)
                            .build());
        }

        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(columns)
                        .primaryKey(buildPrimaryKey(schema.getFieldSchemaList()))
                        .build();
        TableIdentifier tableId =
                TableIdentifier.of(CATALOG_NAME, database, null, sourceCollection);
        Map<String, String> options =
                buildOptions(
                        client,
                        database,
                        sourceCollection,
                        describeCollectionResp,
                        hasPartitionKey,
                        partitionKeyField);
        return CatalogTable.of(
                tableId,
                tableSchema,
                options,
                new ArrayList<>(),
                describeCollectionResp.getDescription());
    }

    private Map<String, String> buildOptions(
            MilvusClientV2 client,
            String database,
            String sourceCollection,
            DescribeCollectionResp describeCollectionResp,
            boolean hasPartitionKey,
            String partitionKeyField) {
        Map<String, String> options = new HashMap<>();
        options.put(
                ENABLE_DYNAMIC_FIELD,
                String.valueOf(describeCollectionResp.getEnableDynamicField()));
        options.put(ENABLE_AUTO_ID, String.valueOf(describeCollectionResp.getAutoID()));
        if (describeCollectionResp.getConsistencyLevel() != null) {
            options.put(CONSISTENCY_LEVEL, describeCollectionResp.getConsistencyLevel().getName());
        }
        List<CreateCollectionReq.Function> functionList =
                describeCollectionResp.getCollectionSchema().getFunctionList();
        options.put(
                FUNCTION_LIST,
                functionList == null ? EMPTY_JSON_ARRAY : new Gson().toJson(functionList));
        options.put(INDEX_LIST, EMPTY_JSON_ARRAY);
        if (describeCollectionResp.getShardsNum() != null) {
            options.put(SHARDS_NUM, String.valueOf(describeCollectionResp.getShardsNum()));
        }
        if (describeCollectionResp.getProperties() != null
                && describeCollectionResp.getProperties().containsKey(TIMEZONE)) {
            options.put(
                    TIMEZONE, String.valueOf(describeCollectionResp.getProperties().get(TIMEZONE)));
        }
        if (hasPartitionKey) {
            options.put(PARTITION_KEY_FIELD, partitionKeyField);
            if (describeCollectionResp.getNumOfPartitions() != null) {
                options.put(
                        PARTITION_NUM, String.valueOf(describeCollectionResp.getNumOfPartitions()));
            }
        } else {
            options.put(PARTITION_NAMES, partitionNames(client, database, sourceCollection));
        }
        return options;
    }

    private String partitionNames(MilvusClientV2 client, String database, String collection) {
        List<String> partitions =
                client.listPartitions(
                        ListPartitionsReq.builder()
                                .databaseName(database)
                                .collectionName(collection)
                                .build());
        return String.join(",", partitions);
    }

    private PrimaryKey buildPrimaryKey(List<CreateCollectionReq.FieldSchema> fields) {
        for (CreateCollectionReq.FieldSchema field : fields) {
            if (Boolean.TRUE.equals(field.getIsPrimaryKey())) {
                return PrimaryKey.of(
                        field.getName(),
                        Collections.singletonList(field.getName()),
                        field.getAutoID());
            }
        }
        return null;
    }

    private PhysicalColumn convertColumn(CreateCollectionReq.FieldSchema fieldSchema) {
        PhysicalColumn.PhysicalColumnBuilder builder = PhysicalColumn.builder();
        builder.name(fieldSchema.getName());
        builder.sourceType(fieldSchema.getDataType().name());
        builder.comment(fieldSchema.getDescription());
        builder.nullable(Boolean.TRUE.equals(fieldSchema.getIsNullable()));
        Map<String, Object> options = new HashMap<>();
        options.put(IS_NULLABLE, fieldSchema.getIsNullable());
        options.put(DEFAULT_VALUE, fieldSchema.getDefaultValue());
        if (fieldSchema.getAutoID() != null) {
            options.put(AUTO_ID, fieldSchema.getAutoID());
        }
        if (Boolean.TRUE.equals(fieldSchema.getIsPartitionKey())) {
            options.put(IS_PARTITION_KEY, true);
        }

        switch (fieldSchema.getDataType()) {
            case Bool:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case Int8:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case Int16:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case Int32:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case Int64:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case Float:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case Double:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case VarChar:
                builder.dataType(BasicType.STRING_TYPE);
                options.put(MAX_LENGTH, fieldSchema.getMaxLength());
                if (fieldSchema.getEnableAnalyzer() != null) {
                    options.put(ENABLE_ANALYZER, fieldSchema.getEnableAnalyzer());
                }
                if (fieldSchema.getEnableMatch() != null) {
                    options.put(ENABLE_MATCH, fieldSchema.getEnableMatch());
                }
                if (fieldSchema.getAnalyzerParams() != null
                        && !fieldSchema.getAnalyzerParams().isEmpty()) {
                    options.put(
                            ANALYZER_PARAMS, new Gson().toJson(fieldSchema.getAnalyzerParams()));
                }
                if (fieldSchema.getMultiAnalyzerParams() != null
                        && !fieldSchema.getMultiAnalyzerParams().isEmpty()) {
                    options.put(
                            MULTI_ANALYZER_PARAMS,
                            new Gson().toJson(fieldSchema.getMultiAnalyzerParams()));
                }
                break;
            case String:
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case JSON:
                builder.dataType(BasicType.STRING_TYPE);
                options.put(CommonOptions.JSON.getName(), true);
                break;
            case Array:
                builder.dataType(convertArrayType(fieldSchema.getElementType()));
                options.put(ELEMENT_TYPE, fieldSchema.getElementType().getCode());
                options.put(MAX_CAPACITY, fieldSchema.getMaxCapacity());
                options.put(MAX_LENGTH, fieldSchema.getMaxLength());
                if (fieldSchema.getElementType() == DataType.Struct) {
                    options.put(CommonOptions.JSON.getName(), true);
                }
                break;
            case FloatVector:
                builder.dataType(VectorType.VECTOR_FLOAT_TYPE);
                builder.scale(fieldSchema.getDimension());
                break;
            case BinaryVector:
                builder.dataType(VectorType.VECTOR_BINARY_TYPE);
                builder.scale(fieldSchema.getDimension());
                break;
            case SparseFloatVector:
                builder.dataType(VectorType.VECTOR_SPARSE_FLOAT_TYPE);
                break;
            case Int8Vector:
                builder.dataType(VectorType.VECTOR_INT8_TYPE);
                builder.scale(fieldSchema.getDimension());
                break;
            case Float16Vector:
                builder.dataType(VectorType.VECTOR_FLOAT16_TYPE);
                builder.scale(fieldSchema.getDimension());
                break;
            case BFloat16Vector:
                builder.dataType(VectorType.VECTOR_BFLOAT16_TYPE);
                builder.scale(fieldSchema.getDimension());
                break;
            case Struct:
                builder.dataType(BasicType.STRING_TYPE);
                options.put(CommonOptions.JSON.getName(), true);
                break;
            case Geometry:
                builder.dataType(GeometryType.GEOMETRY_TYPE);
                break;
            case Timestamptz:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Milvus CDC source field type: " + fieldSchema.getDataType());
        }
        builder.options(options);
        return builder.build();
    }

    private ArrayType<?, ?> convertArrayType(DataType elementType) {
        if (elementType == DataType.Bool) {
            return ArrayType.BOOLEAN_ARRAY_TYPE;
        }
        if (elementType == DataType.Int8) {
            return ArrayType.BYTE_ARRAY_TYPE;
        }
        if (elementType == DataType.Int16) {
            return ArrayType.SHORT_ARRAY_TYPE;
        }
        if (elementType == DataType.Int32) {
            return ArrayType.INT_ARRAY_TYPE;
        }
        if (elementType == DataType.Int64) {
            return ArrayType.LONG_ARRAY_TYPE;
        }
        if (elementType == DataType.Float) {
            return ArrayType.FLOAT_ARRAY_TYPE;
        }
        if (elementType == DataType.Double) {
            return ArrayType.DOUBLE_ARRAY_TYPE;
        }
        return ArrayType.STRING_ARRAY_TYPE;
    }

    private PhysicalColumn convertStructField(
            CreateCollectionReq.StructFieldSchema structFieldSchema) {
        Map<String, Object> options = new HashMap<>();
        options.put(CommonOptions.JSON.getName(), true);
        options.put(ELEMENT_TYPE, DataType.Struct.getCode());
        options.put(MAX_CAPACITY, structFieldSchema.getMaxCapacity());
        if (structFieldSchema.getFields() != null && !structFieldSchema.getFields().isEmpty()) {
            options.put(STRUCT_FIELDS, new Gson().toJson(structFieldSchema.getFields()));
        }
        return PhysicalColumn.builder()
                .name(structFieldSchema.getName())
                .sourceType("Array[Struct]")
                .comment(structFieldSchema.getDescription())
                .nullable(Boolean.TRUE.equals(structFieldSchema.getNullable()))
                .dataType(ArrayType.STRING_ARRAY_TYPE)
                .options(options)
                .build();
    }
}
