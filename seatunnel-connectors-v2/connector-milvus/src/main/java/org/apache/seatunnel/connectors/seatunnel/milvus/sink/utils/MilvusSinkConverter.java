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
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.grpc.DataType;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.FieldType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq.FieldSchema;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
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
        if(value == null) {
            return null;
        }
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
                    String jsonStr = value.toString();
                    if (!"null".equalsIgnoreCase(jsonStr.trim())) {
                        JsonElement el = JsonParser.parseString(jsonStr);
                        if (el.isJsonObject()) {
                            return el.getAsJsonObject();
                        } else {
                            return el; // or throw, depending on desired behavior
                        }
                    } else {
                        return JsonNull.INSTANCE;
                    }
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

    public JsonObject buildMilvusData(
            CatalogTable catalogTable,
            DescribeCollectionResp describeCollectionResp,
            List<MilvusField> milvusFields,
            SeaTunnelRow element) {
        SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        JsonObject data = new JsonObject();
        Gson gson = new Gson();
        for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
            String fieldName = seaTunnelRowType.getFieldNames()[i];
            if (describeCollectionResp.getAutoID() && fieldName.equals(describeCollectionResp.getPrimaryFieldName())) {
                continue; // if create table open AutoId, then don't need insert data with
                // primaryKey field.
            }

            FieldSchema fieldSchema = describeCollectionResp.getCollectionSchema().getField(fieldName);
            Object value = element.getField(i);
            // if the field is dynamic field, then parse the dynamic field
            if (Objects.equals(fieldName, CommonOptions.METADATA.getName())
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
                                            data.add(matches.get(0).getTargetFieldName(), entry.getValue());
                                        }

                                    }else {
                                        data.add(entry.getKey(), entry.getValue());
                                    }
                                });
                continue;
            }
            Object object = convertByMilvusType(fieldSchema, value);
            if(fieldSchema.getDataType() == io.milvus.v2.common.DataType.SparseFloatVector && object == null){
                continue;
            }
            data.add(fieldName, gson.toJsonTree(object));
        }
        return data;
    }

    public static CollectionSchemaParam convertToMilvusSchema(DescribeCollectionResp describeCollectionResp) {
        List<FieldType> fieldTypes = new ArrayList<>();
        List<CreateCollectionReq.Function> functionList = describeCollectionResp.getCollectionSchema().getFunctionList();
        List<String> bm25Sparse = new ArrayList<>();
        for (CreateCollectionReq.Function function : functionList) {
            if(function.getFunctionType() == FunctionType.BM25){
                bm25Sparse.addAll(function.getOutputFieldNames());
            }
        }
        for(CreateCollectionReq.FieldSchema fieldSchema : describeCollectionResp.getCollectionSchema().getFieldSchemaList()){
            if(fieldSchema.getDataType()== io.milvus.v2.common.DataType.SparseFloatVector && bm25Sparse.contains(fieldSchema.getName())){
                continue;
            }
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

    public Object convertByMilvusType(FieldSchema fieldSchema, Object value) {
        if (value == null) {
            return null;
        }
        switch (fieldSchema.getDataType()) {
            case Int8:
                return Byte.parseByte(value.toString());
            case Int16:
                return Short.parseShort(value.toString());
            case Int32:
                return Integer.parseInt(value.toString());
            case Int64:
                return Long.parseLong(value.toString());
            case Float:
                return Float.parseFloat(value.toString());
            case Double:
                return Double.parseDouble(value.toString());
            case Bool:
                return Boolean.parseBoolean(value.toString());
            case VarChar:
            case String:
                return value.toString();
            case JSON:
                String jsonStr = value.toString();
                if (!"null".equalsIgnoreCase(jsonStr.trim())) {
                    JsonElement el = JsonParser.parseString(jsonStr);
                    if (el.isJsonObject()) {
                        return el.getAsJsonObject();
                    } else {
                        return el; // or throw, depending on desired behavior
                    }
                } else {
                    return JsonNull.INSTANCE;
                }
            case Array:
                switch (fieldSchema.getElementType()) {
                    case Int8:
                        Byte[] byteArray = (Byte[]) value;
                        return Arrays.asList(byteArray);
                    case Int16:
                        Short[] shortArray = (Short[]) value;
                        return Arrays.asList(shortArray);
                    case Int32:
                        Integer[] intArray = (Integer[]) value;
                        return Arrays.asList(intArray);
                    case Int64:
                        Long[] longArray = (Long[]) value;
                        return Arrays.asList(longArray);
                    case Float:
                        Float[] floatArray = (Float[]) value;
                        return Arrays.asList(floatArray);
                    case Double:
                        Double[] doubleArray = (Double[]) value;
                        return Arrays.asList(doubleArray);
                    case Bool:
                        Boolean[] booleanArray = (Boolean[]) value;
                        return Arrays.asList(booleanArray);
                    case VarChar:
                        String[] stringArray = (String[]) value;
                        return Arrays.asList(stringArray);
                    default:
                        return value;
                }
            case BinaryVector:
            case BFloat16Vector:
            case Float16Vector:
                ByteBuffer binaryVector = (ByteBuffer) value;
                return gson.toJsonTree(binaryVector.array());
            case FloatVector:
                ByteBuffer floatVectorBuffer = (ByteBuffer) value;
                Float[] floats = BufferUtils.toFloatArray(floatVectorBuffer);
                return Arrays.stream(floats).collect(Collectors.toList());
            case SparseFloatVector:
                return JsonParser.parseString(JsonUtils.toJsonString(value)).getAsJsonObject();
            default:
                throw new MilvusConnectorException(MilvusConnectionErrorCode.NOT_SUPPORT_TYPE, fieldSchema.getDataType().name());
        }
    }
}
