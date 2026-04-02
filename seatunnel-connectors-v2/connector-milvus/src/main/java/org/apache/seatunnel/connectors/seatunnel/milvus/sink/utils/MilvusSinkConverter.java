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
import com.google.gson.reflect.TypeToken;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.grpc.DataType;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.FieldType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq.FieldSchema;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
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
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog.MilvusFieldSchema;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class MilvusSinkConverter {
    private static final Gson gson = new Gson();

    /**
     * Geometry conversion is fully delegated to a {@link GeometryConverter} instance.
     * This dispatcher class deliberately knows nothing about coordinate orders, WKT
     * formats, or any other geometry-specific concept — it just hands {@code value}
     * to {@code geometryConverter} for the {@code Geometry} cases. Adding a new
     * geometry-specific configuration option does not require any change to this
     * file; it lives entirely on {@code GeometryConverter}.
     */
    private final GeometryConverter geometryConverter;

    public MilvusSinkConverter() {
        // Matches the production default (GEOMETRY_CONVERT_MODE = "passthrough"):
        // Geometry strings flow byte-for-byte to Milvus. Tests that need the
        // parsing path should construct with new MilvusSinkConverter(GeometryConverter.PARSE).
        this(GeometryConverter.PASSTHROUGH);
    }

    public MilvusSinkConverter(GeometryConverter geometryConverter) {
        this.geometryConverter = geometryConverter;
    }

    /**
     * Production factory: builds a converter wired up from the sink config.
     * Writers should construct via this entry point so the converter and its
     * helpers (currently just {@link GeometryConverter}) absorb any config keys
     * they need without polluting the writer with sub-domain knowledge.
     */
    public static MilvusSinkConverter fromConfig(ReadonlyConfig config) {
        return new MilvusSinkConverter(GeometryConverter.fromConfig(config));
    }

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
                return value.toString();
            case TIMESTAMP:
                // Convert java.sql.Timestamp to Long (Unix timestamp in microseconds for Milvus)
                if (value instanceof java.sql.Timestamp) {
                    return ((java.sql.Timestamp) value).getTime() * 1000;
                } else {
                    return value.toString();
                }
            case TIMESTAMP_TZ:
                // Milvus SDK requires Timestamptz as ISO 8601 String format
                if (value instanceof java.sql.Timestamp) {
                    return ((java.sql.Timestamp) value).toInstant().toString();
                } else if (value instanceof LocalDateTime) {
                    return ((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant().toString();
                } else if (value instanceof OffsetDateTime) {
                    return ((OffsetDateTime) value).toInstant().toString();
                } else {
                    return value.toString();
                }
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
            case GEOMETRY:
                return geometryConverter.convert(value);
            default:
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.NOT_SUPPORT_TYPE, sqlType.name());
        }
    }

    public JsonObject buildMilvusData(
            CatalogTable catalogTable,
            DescribeCollectionResp describeCollectionResp,
            Map<String, String> milvusFieldsMap,
            SeaTunnelRow element) {
        SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        JsonObject data = new JsonObject();
        Gson gson = new Gson();

        // Build source-to-target field name mapping from field_schema config
        // This allows renaming: source_field_name (old name) -> field_name (new name)
        for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
            String sourceFieldName = seaTunnelRowType.getFieldNames()[i];
            String fieldName = milvusFieldsMap.getOrDefault(sourceFieldName, sourceFieldName);
            // Skip auto-ID primary key fields
            if (describeCollectionResp.getAutoID() && fieldName.equals(describeCollectionResp.getPrimaryFieldName())) {
                continue;
            }

            Object value = element.getField(i);

            // Handle dynamic field extraction
            if (Objects.equals(fieldName, CommonOptions.METADATA.getName())
                    && describeCollectionResp.getEnableDynamicField()) {
                if (value == null) {
                    continue;
                }
                JsonObject dynamicData = gson.fromJson(value.toString(), JsonObject.class);
                dynamicData
                        .entrySet()
                        .forEach(
                                entry -> {
                                    String entryKey = entry.getKey();
                                    String matchedField = milvusFieldsMap.getOrDefault(entryKey, entryKey);
                                    if (data.has(matchedField)) {
                                        throw new MilvusConnectorException(
                                                MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                                                "Metadata key '"
                                                        + entryKey
                                                        + "' conflicts with existing field '"
                                                        + matchedField
                                                        + "'. Use field_schema to rename the conflicting field.");
                                    }
                                    data.add(matchedField, entry.getValue());
                                });
                continue;
            }

            // Get field schema from target collection (using target field name)
            // Check both regular fields and struct fields
            FieldSchema fieldSchema = getFieldSchema(describeCollectionResp, fieldName);

            // Convert value using Milvus type
            if(fieldSchema == null){
                continue;
            }
            Object object = convertByMilvusType(fieldSchema, value);
            if(fieldSchema.getDataType() == io.milvus.v2.common.DataType.SparseFloatVector && object == null){
                continue;
            }

            // Add to data using target field name, check for conflicts with dynamic fields
            if (data.has(fieldName)) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                        "Field '"
                                + fieldName
                                + "' conflicts with a previously written dynamic field. "
                                + "Use field_schema to rename the conflicting field.");
            }
            data.add(fieldName, gson.toJsonTree(object));
        }
        return data;
    }

    /**
     * Get field schema from either regular fields or struct fields
     */
    private FieldSchema getFieldSchema(DescribeCollectionResp describeCollectionResp, String fieldName) {
        // First check regular fields
        FieldSchema fieldSchema = describeCollectionResp.getCollectionSchema().getField(fieldName);
        if (fieldSchema != null) {
            return fieldSchema;
        }

        // Check struct fields (Array[Struct] fields are stored separately)
        List<CreateCollectionReq.StructFieldSchema> structFields = describeCollectionResp.getCollectionSchema().getStructFields();
        if (structFields != null) {
            for (CreateCollectionReq.StructFieldSchema structFieldSchema : structFields) {
                if (structFieldSchema.getName().equals(fieldName)) {
                    // Create a synthetic FieldSchema for Array[Struct]
                    FieldSchema syntheticSchema = FieldSchema.builder()
                            .name(structFieldSchema.getName())
                            .description(structFieldSchema.getDescription())
                            .dataType(io.milvus.v2.common.DataType.Array)
                            .elementType(io.milvus.v2.common.DataType.Struct)
                            .maxCapacity(structFieldSchema.getMaxCapacity())
                            .build();
                    return syntheticSchema;
                }
            }
        }

        return null;
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
                        // ES keyword fields are always deserialized as String by the ES source connector,
                        // even when the original value is an array (e.g. ["a","b"] becomes "[\"a\",\"b\"]").
                        // We need to detect JSON array strings and parse them, or wrap single values.
                        if (value instanceof String) {
                            String strVal = ((String) value).trim();
                            if (strVal.startsWith("[")) {
                                List<String> parsed = gson.fromJson(strVal, new TypeToken<List<String>>(){}.getType());
                                return parsed != null ? parsed : Collections.singletonList(strVal);
                            }
                            return Collections.singletonList(strVal);
                        }
                        String[] stringArray = (String[]) value;
                        return Arrays.asList(stringArray);
                    case Struct:
                        // Handle Array[Struct] - convert JSON strings to JsonObjects
                        String[] structArray = (String[]) value;
                        List<JsonObject> jsonObjects = new ArrayList<>();
                        for (String structJsonStr : structArray) {
                            if (structJsonStr != null && !"null".equalsIgnoreCase(structJsonStr.trim())) {
                                JsonElement el = JsonParser.parseString(structJsonStr);
                                if (el.isJsonObject()) {
                                    jsonObjects.add(el.getAsJsonObject());
                                }
                            }
                        }
                        return jsonObjects;
                    default:
                        return value;
                }
            case BinaryVector:
            case BFloat16Vector:
            case Float16Vector:
                ByteBuffer binaryVector = (ByteBuffer) value;
                return gson.toJsonTree(binaryVector.array());
            case FloatVector:
                if (value instanceof ByteBuffer) {
                    ByteBuffer floatVectorBuffer = (ByteBuffer) value;
                    Float[] floats = BufferUtils.toFloatArray(floatVectorBuffer);
                    return Arrays.stream(floats).collect(Collectors.toList());
                } else if (value instanceof Double[]) {
                    Double[] doubleArr = (Double[]) value;
                    return Arrays.stream(doubleArr).map(Double::floatValue).collect(Collectors.toList());
                } else if (value instanceof Float[]) {
                    return Arrays.asList((Float[]) value);
                } else {
                    // fallback: try to parse as list
                    return value;
                }
            case SparseFloatVector:
                return JsonParser.parseString(JsonUtils.toJsonString(value)).getAsJsonObject();
            case Struct:
                // Handle Struct - convert JSON string to JsonObject
                if (value instanceof String) {
                    String structJson = value.toString();
                    if (!"null".equalsIgnoreCase(structJson.trim())) {
                        JsonElement el = JsonParser.parseString(structJson);
                        if (el.isJsonObject()) {
                            return el.getAsJsonObject();
                        } else {
                            return el;
                        }
                    } else {
                        return JsonNull.INSTANCE;
                    }
                }
                return value;
            case Geometry:
                return geometryConverter.convert(value);
            case Timestamptz:
                // Milvus SDK requires Timestamptz as ISO 8601 String format (e.g., "2024-01-19T11:30:45Z")
                // Reference: Milvus ParamUtils.java line 430+ requires String type for Timestamptz
                if (value instanceof java.sql.Timestamp) {
                    // Convert java.sql.Timestamp to ISO 8601 string (UTC)
                    return ((java.sql.Timestamp) value).toInstant().toString();
                } else if (value instanceof LocalDateTime) {
                    // Convert LocalDateTime (systemDefault) to ISO 8601 string
                    return ((LocalDateTime) value).atZone(ZoneId.systemDefault()).toInstant().toString();
                } else if (value instanceof OffsetDateTime) {
                    // Convert OffsetDateTime to ISO 8601 string (preserves timezone)
                    return ((OffsetDateTime) value).toInstant().toString();
                } else if (value instanceof String) {
                    // Already a string - validate and/or convert to ISO 8601 format
                    String strValue = value.toString();
                    // Try to parse and normalize to ISO 8601
                    try {
                        java.sql.Timestamp ts = java.sql.Timestamp.valueOf(strValue);
                        return ts.toInstant().toString();
                    } catch (IllegalArgumentException e) {
                        // If it's already in ISO 8601 format, return as-is
                        return strValue;
                    }
                }
                // Fallback: convert to string
                return value.toString();
            default:
                throw new MilvusConnectorException(MilvusConnectionErrorCode.NOT_SUPPORT_TYPE, fieldSchema.getDataType().name());
        }
    }
}
