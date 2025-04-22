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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import io.milvus.grpc.DataType;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.FieldType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import static org.apache.seatunnel.api.table.catalog.PrimaryKey.isPrimaryKeyField;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.constants.CommonOptions;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog.MilvusField;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class MilvusSinkConverter {
    private static final Gson gson = new Gson();

    public Object convertBySeaTunnelType(
            SeaTunnelDataType<?> fieldType, Boolean isJson, Object value) {
        SqlType sqlType = fieldType.getSqlType();
        switch (sqlType) {
            case INT:
                return Integer.parseInt(value.toString());
            case TINYINT:
                return Byte.parseByte(value.toString());
            case BIGINT:
                return Long.parseLong(value.toString());
            case SMALLINT:
                return Short.parseShort(value.toString());
            case STRING:
                if (isJson) {
                    return gson.fromJson(value.toString(), JsonObject.class);
                }
                return value.toString();
            case FLOAT_VECTOR:
                ByteBuffer floatVectorBuffer = (ByteBuffer) value;
                Float[] floats = BufferUtils.toFloatArray(floatVectorBuffer);
                return Arrays.stream(floats).collect(Collectors.toList());
            case BINARY_VECTOR:
            case BFLOAT16_VECTOR:
            case FLOAT16_VECTOR:
                ByteBuffer binaryVector = (ByteBuffer) value;
                return gson.toJsonTree(binaryVector.array());
            case SPARSE_FLOAT_VECTOR:
                return JsonParser.parseString(JsonUtils.toJsonString(value)).getAsJsonObject();
            case FLOAT:
                return Float.parseFloat(value.toString());
            case BOOLEAN:
                return Boolean.parseBoolean(value.toString());
            case DOUBLE:
                return Double.parseDouble(value.toString());
            case TIME:
            case DATE:
            case TIMESTAMP:
                return value.toString();
            case ARRAY:
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) fieldType;
                switch (arrayType.getElementType().getSqlType()) {
                    case BOOLEAN:
                        Boolean[] booleanArray = (Boolean[]) value;
                        return Arrays.asList(booleanArray);
                    case STRING:
                        String[] stringArray = (String[]) value;
                        return Arrays.asList(stringArray);
                    case SMALLINT:
                        Short[] shortArray = (Short[]) value;
                        return Arrays.asList(shortArray);
                    case TINYINT:
                        Byte[] byteArray = (Byte[]) value;
                        return Arrays.asList(byteArray);
                    case INT:
                        Integer[] intArray = (Integer[]) value;
                        return Arrays.asList(intArray);
                    case BIGINT:
                        Long[] longArray = (Long[]) value;
                        return Arrays.asList(longArray);
                    case FLOAT:
                        Float[] floatArray = (Float[]) value;
                        return Arrays.asList(floatArray);
                    case DOUBLE:
                        Double[] doubleArray = (Double[]) value;
                        return Arrays.asList(doubleArray);
                }
            case ROW:
                SeaTunnelRow row = (SeaTunnelRow) value;
                SeaTunnelRowType rowType = (SeaTunnelRowType) fieldType;
                JsonObject data = new JsonObject();
                for (int i = 0; i < rowType.getFieldNames().length; i++) {
                    SeaTunnelDataType<?> subFieldType = rowType.getFieldType(i);
                    Object subValue = row.getField(i);
                    Object subRow = convertBySeaTunnelType(subFieldType, false, subValue);
                    data.add(rowType.getFieldNames()[i], gson.toJsonTree(subRow));
                }
                return data;
            case MAP:
                return JsonUtils.toJsonString(value);
            default:
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.NOT_SUPPORT_TYPE, sqlType.name());
        }
    }

    public static CreateCollectionReq.FieldSchema convertToFieldType(
            Column column, PrimaryKey primaryKey, String partitionKeyField, Boolean autoId) {

        SeaTunnelDataType<?> seaTunnelDataType = column.getDataType();

        io.milvus.v2.common.DataType milvusDataType = convertSqlTypeToDataType(seaTunnelDataType.getSqlType());

        CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                .name(column.getName())
                .dataType(milvusDataType)
                .build();

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
                } else if (column.getOptions()!= null && column.getOptions().get(CommonOptions.MAX_LENGTH.getName()) != null) {
                    fieldSchema.setMaxLength((Integer) column.getOptions().get(CommonOptions.MAX_LENGTH.getName()));
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
                    if (column.getOptions().get(CommonOptions.MAX_LENGTH.getName()) != null) {
                        fieldSchema.setMaxLength((Integer) column.getOptions().get(CommonOptions.MAX_LENGTH.getName()));
                    }
                    if (column.getOptions().get(CommonOptions.MAX_CAPACITY.getName()) != null) {
                        fieldSchema.setMaxCapacity((Integer) column.getOptions().get(CommonOptions.MAX_CAPACITY.getName()));
                    }
                    if (column.getOptions().get(CommonOptions.ELEMENT_TYPE.getName()) != null) {
                        fieldSchema.setElementType(convertSqlTypeToDataType((SqlType) column.getOptions().get(CommonOptions.ELEMENT_TYPE.getName())));
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

        // check is primaryKey
        // only override primarykey when primary key num is 1
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
            fieldSchema.setAutoID(autoId);
        }

        // check is partitionKey
        if (column.getName().equals(partitionKeyField)) {
            fieldSchema.setIsPartitionKey(true);
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

    public JsonObject buildMilvusData(
            CatalogTable catalogTable,
            DescribeCollectionResp describeCollectionResp,
            List<String> jsonFields,
            String dynamicField,
            List<MilvusField> milvusFields,
            SeaTunnelRow element) {
        SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        JsonObject data = new JsonObject();
        Gson gson = new Gson();
        for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
            String fieldName = seaTunnelRowType.getFieldNames()[i];
            Boolean isJson = jsonFields.contains(fieldName);
            if (describeCollectionResp.getAutoID() && fieldName.equals(describeCollectionResp.getPrimaryFieldName())) {
                continue; // if create table open AutoId, then don't need insert data with
                // primaryKey field.
            }

            SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(i);
            Object value = element.getField(i);
//            if (null == value) {
//                throw new MilvusConnectorException(
//                        MilvusConnectionErrorCode.FIELD_IS_NULL, fieldName);
//            }
            // if the field is dynamic field, then parse the dynamic field
            if (dynamicField != null
                    && dynamicField.equals(fieldName)
                    && describeCollectionResp.getEnableDynamicField()) {
                JsonObject dynamicData = gson.fromJson(value.toString(), JsonObject.class);
                dynamicData
                        .entrySet()
                        .forEach(
                                entry -> {
                                    String entryKey = entry.getKey();
                                    List<MilvusField> matches = milvusFields.stream().filter(a -> a.getSourceFieldName().equals(entryKey)).collect(Collectors.toList());
                                    if(!matches.isEmpty()) {
                                        if (matches.get(0).getSourceFieldName().equals(entryKey)) {
                                            DataType dataType = DataType.forNumber(matches.get(0).getDataType());
                                            Object newValue = convertDefault(dataType.getNumber(), entry.getValue());
                                            data.add(matches.get(0).getTargetFieldName(), gson.toJsonTree(newValue));
                                        }

                                    }else {
                                        data.add(entry.getKey(), entry.getValue());
                                    }
                                });
                continue;
            }
            Object object = convertBySeaTunnelType(fieldType, isJson, value);
            data.add(fieldName, gson.toJsonTree(object));
        }
        return data;
    }

    public Object convertDefault(Integer dataType, Object defaultValue) {
        if (defaultValue == null || defaultValue.toString().isEmpty()) {
            return null;
        }
        io.milvus.v2.common.DataType dataTypeEnum = io.milvus.v2.common.DataType.forNumber(dataType);
        JsonPrimitive value = (JsonPrimitive) defaultValue;
        try {
            switch (dataTypeEnum){
                case Int8:
                case Int16:
                    return value.getAsShort();
                case Int32:
                    return value.getAsInt();
                case Int64:
                    return Long.valueOf(defaultValue.toString());
                case Bool:
                    return Boolean.valueOf(defaultValue.toString());
                case Float:
                    return Float.valueOf(defaultValue.toString());
                case Double:
                    return Double.valueOf(defaultValue.toString());
                case VarChar:
                case String:
                    return defaultValue.toString();
                case JSON:
                    return defaultValue.toString();
                default:
                    return defaultValue;
            }} catch (Exception e) {
            // if the default value is not valid, return null
            return null;
        }
    }

    public static CollectionSchemaParam convertToMilvusSchema(DescribeCollectionResp describeCollectionResp) {
        List<FieldType> fieldTypes = new ArrayList<>();
        for(CreateCollectionReq.FieldSchema fieldSchema : describeCollectionResp.getCollectionSchema().getFieldSchemaList()){
            FieldType.Builder fieldType = FieldType.newBuilder()
                    .withName(fieldSchema.getName())
                    .withDataType(DataType.forNumber(fieldSchema.getDataType().getCode()))
                    .withPrimaryKey(fieldSchema.getIsPrimaryKey())
                    .withAutoID(fieldSchema.getAutoID())
                    .withDescription(fieldSchema.getDescription());
            if(fieldSchema.getDimension() != null){
                fieldType.withDimension(fieldSchema.getDimension());
            }
            if(fieldSchema.getMaxLength() != null){
                fieldType.withMaxLength(fieldSchema.getMaxLength());
            }
            if(fieldSchema.getMaxCapacity() != null){
                fieldType.withMaxCapacity(fieldSchema.getMaxCapacity());
                fieldType.withElementType(DataType.forNumber(fieldSchema.getElementType().getCode()));
            }
            if(fieldSchema.getIsNullable() != null){
                fieldType.withNullable(fieldSchema.getIsNullable());
            }
            if(fieldSchema.getDefaultValue() != null){
                fieldType.withDefaultValue(fieldSchema.getDefaultValue());
            }
            fieldTypes.add(fieldType.build());
        }
        return CollectionSchemaParam.newBuilder()
                .withEnableDynamicField(describeCollectionResp.getEnableDynamicField())
                .withFieldTypes(fieldTypes)
                .build();
    }
}
