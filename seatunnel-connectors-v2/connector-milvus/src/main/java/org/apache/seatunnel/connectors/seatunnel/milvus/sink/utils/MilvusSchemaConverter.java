package org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils;

import io.milvus.v2.service.collection.request.CreateCollectionReq;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.milvus.common.MilvusConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MilvusSchemaConverter {
    /**
     * Convert column from source to Milvus FieldSchema.
     * This creates the base field schema from source data, which can be later overridden by config.
     *
     * @param column Source column definition
     * @param primaryKey Primary key from source (optional)
     * @return Field schema based on source column definition
     */
    public static CreateCollectionReq.FieldSchema convertToFieldType(
            Column column, PrimaryKey primaryKey) {

        SeaTunnelDataType<?> seaTunnelDataType = column.getDataType();

        io.milvus.v2.common.DataType milvusDataType = convertSqlTypeToDataType(seaTunnelDataType.getSqlType());

        CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                .name(column.getName())
                .dataType(milvusDataType)
                .build();
        // Handle column options safely - these come from source metadata
        Map<String, Object> options = column.getOptions();
        if (options != null) {
            // Handle analyzer settings from source
            Boolean enableAnalyzer = (Boolean) options.get(MilvusConstants.ENABLE_ANALYZER);
            if (Boolean.TRUE.equals(enableAnalyzer)) {
                fieldSchema.setEnableAnalyzer(true);
                // Set analyzer params if available
                String analyzerParamsStr = (String) options.get(MilvusConstants.ANALYZER_PARAMS);
                if (analyzerParamsStr != null && !analyzerParamsStr.isEmpty()) {
                    try {
                        com.google.gson.Gson gson = new com.google.gson.Gson();
                        com.google.gson.reflect.TypeToken<java.util.Map<String, Object>> typeToken =
                            new com.google.gson.reflect.TypeToken<java.util.Map<String, Object>>(){};
                        java.util.Map<String, Object> analyzerParams = gson.fromJson(analyzerParamsStr, typeToken.getType());
                        fieldSchema.setAnalyzerParams(analyzerParams);
                    } catch (Exception e) {
                        // Log error but don't fail the field creation
                        System.err.println("Failed to parse analyzer params: " + e.getMessage());
                    }
                }

                // Set multi-analyzer params if available
                String multiAnalyzerParamsStr = (String) options.get(MilvusConstants.MULTI_ANALYZER_PARAMS);
                if (multiAnalyzerParamsStr != null && !multiAnalyzerParamsStr.isEmpty()) {
                    try {
                        com.google.gson.Gson gson = new com.google.gson.Gson();
                        com.google.gson.reflect.TypeToken<java.util.Map<String, Object>> typeToken =
                            new com.google.gson.reflect.TypeToken<java.util.Map<String, Object>>(){};
                        java.util.Map<String, Object> multiAnalyzerParams = gson.fromJson(multiAnalyzerParamsStr, typeToken.getType());
                        fieldSchema.setMultiAnalyzerParams(multiAnalyzerParams);
                    } catch (Exception e) {
                        // Log error but don't fail the field creation
                        System.err.println("Failed to parse multi-analyzer params: " + e.getMessage());
                    }
                }
            }

            // Handle match settings from source
            Boolean enableMatch = (Boolean) options.get(MilvusConstants.ENABLE_MATCH);
            if (Boolean.TRUE.equals(enableMatch)) {
                fieldSchema.setEnableMatch(true);
            }

            // Handle partition key from source metadata
            Boolean isPartitionKey = (Boolean) options.get(MilvusConstants.IS_PARTITION_KEY);
            if (Boolean.TRUE.equals(isPartitionKey)) {
                fieldSchema.setIsPartitionKey(true);
            }

            // Handle auto ID from source metadata
            Boolean autoId = (Boolean) options.get(MilvusConstants.AUTO_ID);
            if (autoId != null) {
                fieldSchema.setAutoID(autoId);
            }

            // Handle nullable from source metadata
            Boolean isNullable = (Boolean) options.get(MilvusConstants.IS_NULLABLE);
            if (isNullable != null) {
                fieldSchema.setIsNullable(isNullable);
            }

            // Handle default value from source metadata
            Object defaultValue = options.get(MilvusConstants.DEFAULT_VALUE);
            if (defaultValue != null) {
                fieldSchema.setDefaultValue(defaultValue);
            }
        }
        if (StringUtils.isNotEmpty(column.getComment())) {
            fieldSchema.setDescription(column.getComment());
        }
        switch (seaTunnelDataType.getSqlType()) {
            case ROW:
                fieldSchema.setMaxLength(65535);
                break;
            case DATE:
            case TIME:
            case TIMESTAMP:
                fieldSchema.setMaxLength(50);
                break;
            case STRING:
                if (column.getOptions() != null
                        && column.getOptions().get(CommonOptions.JSON.getName()) != null
                        && (Boolean) column.getOptions().get(CommonOptions.JSON.getName())) {
                    // check if is json
                    fieldSchema.setDataType(io.milvus.v2.common.DataType.JSON);
                } else if (column.getOptions()!= null && column.getOptions().get(MilvusConstants.MAX_LENGTH) != null) {
                    fieldSchema.setMaxLength((Integer) column.getOptions().get(MilvusConstants.MAX_LENGTH));
                } else {
                    fieldSchema.setMaxLength(65535);
                }
                break;
            case ARRAY:
                fieldSchema.setDataType(io.milvus.v2.common.DataType.Array);
                ArrayType arrayType = (ArrayType) column.getDataType();
                SeaTunnelDataType elementType = arrayType.getElementType();
                fieldSchema.setElementType(convertSqlTypeToDataType(elementType.getSqlType()));
                fieldSchema.setMaxCapacity(4096);
                if (Objects.requireNonNull(elementType.getSqlType()) == SqlType.STRING) {
                    fieldSchema.setMaxLength(65535);
                }
                if(column.getOptions()!= null){
                    if (column.getOptions().get(MilvusConstants.MAX_LENGTH) != null) {
                        fieldSchema.setMaxLength((Integer) column.getOptions().get(MilvusConstants.MAX_LENGTH));
                    }
                    if (column.getOptions().get(MilvusConstants.MAX_CAPACITY) != null) {
                        fieldSchema.setMaxCapacity((Integer) column.getOptions().get(MilvusConstants.MAX_CAPACITY));
                    }
                    if (column.getOptions().get(MilvusConstants.ELEMENT_TYPE) != null) {
                        fieldSchema.setElementType(io.milvus.v2.common.DataType.forNumber((Integer) column.getOptions().get(MilvusConstants.ELEMENT_TYPE)));
                    }
                }
                break;
            case BINARY_VECTOR:
            case FLOAT_VECTOR:
            case FLOAT16_VECTOR:
            case BFLOAT16_VECTOR:
                fieldSchema.setDimension(column.getScale());
                break;
        }

        // Set primary key from source schema if defined
        // Only set primarykey when primary key num is 1
        if (null != primaryKey && primaryKey.getColumnNames().size() == 1 && primaryKey.getColumnNames().contains(column.getName())) {
            fieldSchema.setIsPrimaryKey(true);
            List<SqlType> integerTypes = new ArrayList<>();
            integerTypes.add(SqlType.INT);
            integerTypes.add(SqlType.SMALLINT);
            integerTypes.add(SqlType.TINYINT);
            integerTypes.add(SqlType.BIGINT);
            if (integerTypes.contains(seaTunnelDataType.getSqlType())) {
                fieldSchema.setDataType(io.milvus.v2.common.DataType.Int64);
            } else {
                fieldSchema.setDataType(io.milvus.v2.common.DataType.VarChar);
                fieldSchema.setMaxLength(65535);
            }
        }

        return fieldSchema;
    }

    public static io.milvus.v2.common.DataType convertSqlTypeToDataType(SqlType sqlType) {
        switch (sqlType) {
            case BOOLEAN:
                return io.milvus.v2.common.DataType.Bool;
            case TINYINT:
                return io.milvus.v2.common.DataType.Int8;
            case SMALLINT:
                return io.milvus.v2.common.DataType.Int16;
            case INT:
                return io.milvus.v2.common.DataType.Int32;
            case BIGINT:
                return io.milvus.v2.common.DataType.Int64;
            case FLOAT:
                return io.milvus.v2.common.DataType.Float;
            case DOUBLE:
                return io.milvus.v2.common.DataType.Double;
            case STRING:
                return io.milvus.v2.common.DataType.VarChar;
            case ARRAY:
                return io.milvus.v2.common.DataType.Array;
            case MAP:
                return io.milvus.v2.common.DataType.JSON;
            case FLOAT_VECTOR:
                return io.milvus.v2.common.DataType.FloatVector;
            case BINARY_VECTOR:
                return io.milvus.v2.common.DataType.BinaryVector;
            case FLOAT16_VECTOR:
                return io.milvus.v2.common.DataType.Float16Vector;
            case BFLOAT16_VECTOR:
                return io.milvus.v2.common.DataType.BFloat16Vector;
            case SPARSE_FLOAT_VECTOR:
                return io.milvus.v2.common.DataType.SparseFloatVector;
            case DATE:
            case TIME:
            case TIMESTAMP:
                return io.milvus.v2.common.DataType.VarChar;
            case ROW:
                return io.milvus.v2.common.DataType.JSON;
        }
        throw new CatalogException(
                String.format("Not support convert to milvus type, sqlType is %s", sqlType));
    }

}
