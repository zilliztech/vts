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

package org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.parser;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.config.MilvusCdcMessageType;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.schema.MilvusCdcCollectionSchema;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.MILVUS_INTERNAL_DYNAMIC_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.STRUCT_CHILD_CLOSE;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.STRUCT_CHILD_OPEN;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.STRUCT_CHILD_OPEN_CHAR;
import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.STRUCT_FIELDS;

class MilvusCdcRowConverter {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Gson gson = new Gson();

    SeaTunnelRow convert(MilvusCdcDecodedRecord record, MilvusCdcCollectionSchema schema) {
        SeaTunnelRowType rowType = schema.getRowType();
        String[] fieldNames = rowType.getFieldNames();
        Object[] fields = new Object[rowType.getTotalFields()];

        if (record.getMessageType() == MilvusCdcMessageType.DELETE) {
            Column primaryKeyColumn =
                    columnAt(
                            schema,
                            schema.getPrimaryKeyIndex(),
                            fieldNames[schema.getPrimaryKeyIndex()]);
            fields[schema.getPrimaryKeyIndex()] =
                    convert(
                            rowType.getFieldType(schema.getPrimaryKeyIndex()),
                            record.getPrimaryKey(),
                            primaryKeyColumn);
        } else {
            Map<String, Object> data = record.getData();
            if (data == null) {
                throw new IllegalArgumentException(
                        "Milvus CDC insert record data must not be null.");
            }
            boolean hasInternalDynamicField = data.containsKey(MILVUS_INTERNAL_DYNAMIC_FIELD);
            boolean hasDynamicFields = hasInternalDynamicField || record.isDynamicFieldsPresent();
            if (hasDynamicFields && !schema.isEnableDynamicField()) {
                throw new IllegalArgumentException(
                        "Milvus CDC insert record contains dynamic fields but schema does not "
                                + "enable dynamic field.");
            }
            boolean convertedDynamicField = false;
            Set<String> schemaFieldNames = schemaFieldNames(schema);
            validateStaticFields(record, schema);
            for (int i = 0; i < fieldNames.length; i++) {
                Column column = columnAt(schema, i, fieldNames[i]);
                if (isDynamicField(fieldNames[i])) {
                    convertedDynamicField = true;
                    fields[i] = toJson(dynamicFields(data, schemaFieldNames));
                    continue;
                }
                boolean hasDirectValue = data.containsKey(fieldNames[i]);
                Object value = hasDirectValue ? data.get(fieldNames[i]) : null;
                if (!hasDirectValue
                        && isJsonArrayColumn(rowType.getFieldType(i), column)
                        && hasFlattenedStructChildren(data, fieldNames[i])) {
                    value = flattenedStructArray(data, fieldNames[i], column);
                    hasDirectValue = true;
                }
                if (!hasDirectValue) {
                    throw new IllegalArgumentException(
                            "Milvus CDC insert record missing field " + fieldNames[i] + ".");
                }
                fields[i] = convert(rowType.getFieldType(i), value, column);
            }
            if (hasDynamicFields && !convertedDynamicField) {
                throw new IllegalArgumentException(
                        "Milvus CDC insert record contains dynamic fields but row type has no "
                                + "metadata column "
                                + MILVUS_INTERNAL_DYNAMIC_FIELD
                                + ".");
            }
        }

        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setRowKind(rowKind(record.getMessageType()));
        row.setTableId(schema.getTableId());
        if (record.getPartition() != null) {
            row.setPartitionName(record.getPartition());
        }
        return row;
    }

    private void validateStaticFields(
            MilvusCdcDecodedRecord record, MilvusCdcCollectionSchema schema) {
        // TODO: Handle Milvus DDL messages by updating the schema registry and emitting the
        // corresponding SchemaChangeEvent before subsequent DML records. Keep this fail-fast
        // guard until then.
        Set<String> schemaFieldNames = schemaFieldNames(schema);
        for (String fieldName : record.getStaticFieldNames()) {
            if (schemaFieldNames.contains(fieldName)
                    || isFlattenedSchemaField(fieldName, schemaFieldNames)) {
                continue;
            }
            throw new IllegalArgumentException(
                    "Milvus CDC source collection schema changed after job startup: unexpected "
                            + "static field "
                            + fieldName
                            + " in "
                            + schema.getTableId()
                            + ". Runtime schema changes are not supported yet.");
        }
    }

    private RowKind rowKind(MilvusCdcMessageType messageType) {
        return messageType == MilvusCdcMessageType.DELETE ? RowKind.DELETE : RowKind.INSERT;
    }

    private Object convert(SeaTunnelDataType<?> dataType, Object value, Column column) {
        if (value == null) {
            return null;
        }
        SqlType sqlType = dataType.getSqlType();
        switch (sqlType) {
            case BOOLEAN:
                return requireType(value, Boolean.class, sqlType);
            case TINYINT:
                return toByte(value);
            case SMALLINT:
                return toShort(value);
            case INT:
                return toInteger(value);
            case BIGINT:
                return toLong(value);
            case FLOAT:
                return toFloat(value);
            case DOUBLE:
                return toDouble(value);
            case STRING:
                return isJsonColumn(column) ? toJson(value) : requireString(value);
            case GEOMETRY:
                return toGeometryValue(value);
            case TIMESTAMP:
            case TIMESTAMP_TZ:
            case DATE:
            case TIME:
                return toTimeValue(value);
            case ARRAY:
                return convertArray((ArrayType<?, ?>) dataType, value, column);
            case FLOAT_VECTOR:
                return toFloatVectorByteBuffer(value);
            case BINARY_VECTOR:
            case INT8_VECTOR:
            case FLOAT16_VECTOR:
            case BFLOAT16_VECTOR:
                return toByteBuffer(value);
            case SPARSE_FLOAT_VECTOR:
                return value;
            case MAP:
            case ROW:
                return toJson(value);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Milvus CDC SeaTunnel SQL type: " + sqlType);
        }
    }

    private Object convertArray(ArrayType<?, ?> arrayType, Object value, Column column) {
        if (!(value instanceof List)) {
            throw new IllegalArgumentException(
                    "Milvus CDC array value must be a List, but got " + value.getClass().getName());
        }
        List<?> values = (List<?>) value;
        if (isJsonColumn(column)) {
            Map<String, CreateCollectionReq.FieldSchema> structFields = structFields(column);
            String[] strings = new String[values.size()];
            for (int i = 0; i < values.size(); i++) {
                Object element = normalizeStructElement(values.get(i), structFields);
                strings[i] = element instanceof String ? (String) element : toJson(element);
            }
            return strings;
        }
        switch (arrayType.getElementType().getSqlType()) {
            case BOOLEAN:
                Boolean[] booleans = new Boolean[values.size()];
                for (int i = 0; i < values.size(); i++) {
                    booleans[i] =
                            requireType(arrayElement(values, i), Boolean.class, SqlType.BOOLEAN);
                }
                return booleans;
            case TINYINT:
                Byte[] bytes = new Byte[values.size()];
                for (int i = 0; i < values.size(); i++) {
                    bytes[i] = toByte(arrayElement(values, i));
                }
                return bytes;
            case SMALLINT:
                Short[] shorts = new Short[values.size()];
                for (int i = 0; i < values.size(); i++) {
                    shorts[i] = toShort(arrayElement(values, i));
                }
                return shorts;
            case INT:
                Integer[] integers = new Integer[values.size()];
                for (int i = 0; i < values.size(); i++) {
                    integers[i] = toInteger(arrayElement(values, i));
                }
                return integers;
            case BIGINT:
                Long[] longs = new Long[values.size()];
                for (int i = 0; i < values.size(); i++) {
                    longs[i] = toLong(arrayElement(values, i));
                }
                return longs;
            case FLOAT:
                Float[] floats = new Float[values.size()];
                for (int i = 0; i < values.size(); i++) {
                    floats[i] = toFloat(arrayElement(values, i));
                }
                return floats;
            case DOUBLE:
                Double[] doubles = new Double[values.size()];
                for (int i = 0; i < values.size(); i++) {
                    doubles[i] = toDouble(arrayElement(values, i));
                }
                return doubles;
            case STRING:
                String[] strings = new String[values.size()];
                for (int i = 0; i < values.size(); i++) {
                    strings[i] = requireString(arrayElement(values, i));
                }
                return strings;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Milvus CDC array element SQL type: "
                                + arrayType.getElementType().getSqlType());
        }
    }

    private Object arrayElement(List<?> values, int index) {
        Object value = values.get(index);
        if (value == null) {
            throw new IllegalArgumentException(
                    "Milvus CDC array element at index " + index + " must not be null.");
        }
        return value;
    }

    private <T> T requireType(Object value, Class<T> expectedType, SqlType sqlType) {
        if (expectedType.isInstance(value)) {
            return expectedType.cast(value);
        }
        throw new IllegalArgumentException(
                "Unexpected Milvus CDC value type for "
                        + sqlType
                        + ": expected "
                        + expectedType.getSimpleName()
                        + ", got "
                        + value.getClass().getName());
    }

    private String requireString(Object value) {
        if (value instanceof String) {
            return (String) value;
        }
        throw new IllegalArgumentException(
                "Unexpected Milvus CDC string value type: " + value.getClass().getName());
    }

    private Number requireNumber(Object value, SqlType sqlType) {
        if (value instanceof Number) {
            return (Number) value;
        }
        throw new IllegalArgumentException(
                "Unexpected Milvus CDC value type for "
                        + sqlType
                        + ": expected Number, got "
                        + value.getClass().getName());
    }

    private long requireIntegral(Object value, SqlType sqlType) {
        if (value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long) {
            return ((Number) value).longValue();
        }
        throw new IllegalArgumentException(
                "Unexpected Milvus CDC integral value type for "
                        + sqlType
                        + ": got "
                        + value.getClass().getName());
    }

    private Byte toByte(Object value) {
        long number = requireIntegral(value, SqlType.TINYINT);
        if (number < Byte.MIN_VALUE || number > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Milvus CDC tinyint value out of range: " + number);
        }
        return (byte) number;
    }

    private Short toShort(Object value) {
        long number = requireIntegral(value, SqlType.SMALLINT);
        if (number < Short.MIN_VALUE || number > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Milvus CDC smallint value out of range: " + number);
        }
        return (short) number;
    }

    private Integer toInteger(Object value) {
        long number = requireIntegral(value, SqlType.INT);
        if (number < Integer.MIN_VALUE || number > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Milvus CDC int value out of range: " + number);
        }
        return (int) number;
    }

    private Long toLong(Object value) {
        return requireIntegral(value, SqlType.BIGINT);
    }

    private Float toFloat(Object value) {
        return requireNumber(value, SqlType.FLOAT).floatValue();
    }

    private Double toDouble(Object value) {
        return requireNumber(value, SqlType.DOUBLE).doubleValue();
    }

    private ByteBuffer toFloatVectorByteBuffer(Object value) {
        if (value instanceof ByteBuffer) {
            return (ByteBuffer) value;
        }
        if (value instanceof Float[]) {
            return BufferUtils.toByteBuffer((Float[]) value);
        }
        if (value instanceof float[]) {
            float[] array = (float[]) value;
            Float[] boxed = new Float[array.length];
            for (int i = 0; i < array.length; i++) {
                boxed[i] = array[i];
            }
            return BufferUtils.toByteBuffer(boxed);
        }
        if (value instanceof Object[]) {
            return toFloatVectorByteBuffer(Arrays.asList((Object[]) value));
        }
        if (value instanceof List) {
            List<?> values = (List<?>) value;
            Float[] floats = new Float[values.size()];
            for (int i = 0; i < values.size(); i++) {
                floats[i] = toFloat(arrayElement(values, i));
            }
            return BufferUtils.toByteBuffer(floats);
        }
        throw new IllegalArgumentException("Unexpected Milvus CDC FloatVector value: " + value);
    }

    private ByteBuffer toByteBuffer(Object value) {
        if (value instanceof ByteBuffer) {
            return (ByteBuffer) value;
        }
        if (value instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) value);
        }
        if (value instanceof CharSequence) {
            return ByteBuffer.wrap(Base64.getDecoder().decode(value.toString()));
        }
        throw new IllegalArgumentException(
                "Unexpected Milvus CDC binary vector value: " + value.getClass().getName());
    }

    private Object toGeometryValue(Object value) {
        if (value instanceof ByteBuffer) {
            return value;
        }
        if (value instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) value);
        }
        if (value instanceof CharSequence) {
            return value.toString();
        }
        throw new IllegalArgumentException(
                "Unexpected Milvus CDC geometry value: " + value.getClass().getName());
    }

    private Object toTimeValue(Object value) {
        if (value instanceof Number) {
            return unixMicrosToInstantString(((Number) value).longValue());
        }
        if (value instanceof CharSequence) {
            return value.toString();
        }
        throw new IllegalArgumentException(
                "Unexpected Milvus CDC time value: " + value.getClass().getName());
    }

    private Map<String, CreateCollectionReq.FieldSchema> structFields(Column column) {
        if (column == null || column.getOptions() == null) {
            throw new IllegalArgumentException("Milvus CDC struct column options must be set.");
        }
        Object rawStructFields = column.getOptions().get(STRUCT_FIELDS);
        if (rawStructFields == null) {
            throw new IllegalArgumentException(
                    "Milvus CDC struct column "
                            + column.getName()
                            + " is missing struct_fields option.");
        }

        JsonElement root = JsonParser.parseString(rawStructFields.toString());
        if (!root.isJsonArray()) {
            throw new IllegalArgumentException(
                    "Milvus CDC struct_fields option for column "
                            + column.getName()
                            + " must be a JSON array.");
        }
        Map<String, CreateCollectionReq.FieldSchema> fields = new LinkedHashMap<>();
        for (JsonElement element : root.getAsJsonArray()) {
            if (!element.isJsonObject()) {
                throw new IllegalArgumentException(
                        "Milvus CDC struct_fields option for column "
                                + column.getName()
                                + " contains a non-object element.");
            }
            CreateCollectionReq.FieldSchema fieldSchema = structField(element.getAsJsonObject());
            String childName = fieldSchema.getName();
            if (flattenedStructChildName(column.getName(), childName) != null) {
                throw new IllegalArgumentException(
                        "Milvus CDC struct child field schema for column "
                                + column.getName()
                                + " must use raw child field names, but got "
                                + childName);
            }
            if (fields.put(childName, fieldSchema) != null) {
                throw new IllegalArgumentException(
                        "Duplicate Milvus CDC struct child field in schema for column "
                                + column.getName()
                                + ": "
                                + childName);
            }
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException(
                    "Milvus CDC struct_fields option for column "
                            + column.getName()
                            + " must not be empty.");
        }
        return fields;
    }

    private CreateCollectionReq.FieldSchema structField(JsonObject object) {
        String fieldName = stringMember(object, "name");
        DataType dataType = dataTypeMember(object, "dataType");
        if (fieldName == null || fieldName.trim().isEmpty() || dataType == null) {
            throw new IllegalArgumentException(
                    "Milvus CDC struct field schema must contain non-empty name and dataType.");
        }
        CreateCollectionReq.FieldSchema.FieldSchemaBuilder builder =
                CreateCollectionReq.FieldSchema.builder()
                        .name(fieldName)
                        .dataType(dataType)
                        .elementType(dataTypeMember(object, "elementType"));
        Integer dimension = intMember(object, "dimension");
        if (dimension != null) {
            builder.dimension(dimension);
        }
        return builder.build();
    }

    private String stringMember(JsonObject object, String name) {
        JsonElement value = object.get(name);
        return value == null || value.isJsonNull() ? null : value.getAsString();
    }

    private Integer intMember(JsonObject object, String name) {
        JsonElement value = object.get(name);
        return value == null || value.isJsonNull() ? null : value.getAsInt();
    }

    private DataType dataTypeMember(JsonObject object, String name) {
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (value.isJsonPrimitive() && value.getAsJsonPrimitive().isNumber()) {
            return DataType.forNumber(value.getAsInt());
        }
        return DataType.valueOf(value.getAsString());
    }

    @SuppressWarnings("unchecked")
    private Object normalizeStructElement(
            Object element, Map<String, CreateCollectionReq.FieldSchema> structFields) {
        if (element == null) {
            throw new IllegalArgumentException("Milvus CDC struct element must not be null.");
        }
        if (element instanceof String) {
            String json = ((String) element).trim();
            if (!json.startsWith("{")) {
                throw new IllegalArgumentException(
                        "Milvus CDC struct element must be a JSON object string.");
            }
            return normalizeStructElement(gson.fromJson(json, LinkedHashMap.class), structFields);
        }
        if (!(element instanceof Map)) {
            throw new IllegalArgumentException(
                    "Milvus CDC struct element must be a map, but got "
                            + element.getClass().getName());
        }
        Map<String, Object> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : ((Map<String, Object>) element).entrySet()) {
            CreateCollectionReq.FieldSchema fieldSchema = structFields.get(entry.getKey());
            if (fieldSchema == null) {
                throw new IllegalArgumentException(
                        "Unexpected Milvus CDC struct child field: " + entry.getKey());
            }
            normalized.put(
                    entry.getKey(), normalizeStructFieldValue(fieldSchema, entry.getValue()));
        }
        for (String fieldName : structFields.keySet()) {
            if (!normalized.containsKey(fieldName)) {
                throw new IllegalArgumentException(
                        "Milvus CDC struct element missing child field: " + fieldName);
            }
        }
        return normalized;
    }

    private Object normalizeStructFieldValue(
            CreateCollectionReq.FieldSchema fieldSchema, Object value) {
        if (value == null) {
            throw new IllegalArgumentException(
                    "Milvus CDC struct child field "
                            + fieldSchema.getName()
                            + " must not be null.");
        }
        switch (fieldSchema.getDataType()) {
            case FloatVector:
                return toFloatList(value);
            case BinaryVector:
            case Int8Vector:
            case Float16Vector:
            case BFloat16Vector:
                return toByteList(value);
            default:
                return value;
        }
    }

    private List<Float> toFloatList(Object value) {
        if (value instanceof ByteBuffer) {
            return Arrays.asList(BufferUtils.toFloatArray(((ByteBuffer) value).duplicate()));
        }
        if (value instanceof Float[]) {
            return Arrays.asList((Float[]) value);
        }
        if (value instanceof float[]) {
            float[] floats = (float[]) value;
            List<Float> values = new ArrayList<>(floats.length);
            for (float v : floats) {
                values.add(v);
            }
            return values;
        }
        if (value instanceof Object[]) {
            return toFloatList(Arrays.asList((Object[]) value));
        }
        if (value instanceof List) {
            return ((List<?>) value).stream().map(this::toFloat).collect(Collectors.toList());
        }
        String text = value.toString().trim();
        if (text.startsWith("[")) {
            return gson.fromJson(
                    text, new com.google.gson.reflect.TypeToken<List<Float>>() {}.getType());
        }
        return Arrays.asList(
                BufferUtils.toFloatArray(ByteBuffer.wrap(Base64.getDecoder().decode(text))));
    }

    private List<Byte> toByteList(Object value) {
        if (value instanceof ByteBuffer) {
            return byteBufferToList((ByteBuffer) value);
        }
        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            List<Byte> values = new ArrayList<>(bytes.length);
            for (byte b : bytes) {
                values.add(b);
            }
            return values;
        }
        if (value instanceof List) {
            return ((List<?>) value).stream().map(this::toByte).collect(Collectors.toList());
        }
        String text = value.toString().trim();
        if (text.startsWith("[")) {
            return gson.fromJson(
                    text, new com.google.gson.reflect.TypeToken<List<Byte>>() {}.getType());
        }
        return toByteList(Base64.getDecoder().decode(text));
    }

    private List<Byte> byteBufferToList(ByteBuffer value) {
        ByteBuffer buffer = value.duplicate();
        List<Byte> bytes = new ArrayList<>(buffer.remaining());
        while (buffer.hasRemaining()) {
            bytes.add(buffer.get());
        }
        return bytes;
    }

    private String unixMicrosToInstantString(long value) {
        long seconds = Math.floorDiv(value, 1_000_000L);
        long micros = Math.floorMod(value, 1_000_000L);
        return Instant.ofEpochSecond(seconds, micros * 1_000L).toString();
    }

    private Column columnAt(MilvusCdcCollectionSchema schema, int index, String fieldName) {
        List<Column> columns = schema.getCatalogTable().getTableSchema().getColumns();
        if (index >= 0
                && index < columns.size()
                && columns.get(index).getName().equals(fieldName)) {
            return columns.get(index);
        }
        for (Column column : columns) {
            if (column.getName().equals(fieldName)) {
                return column;
            }
        }
        throw new IllegalArgumentException(
                "Column "
                        + fieldName
                        + " not found in Milvus CDC collection schema "
                        + schema.getTableId());
    }

    private boolean isJsonColumn(Column column) {
        return column != null
                && column.getOptions() != null
                && Boolean.TRUE.equals(column.getOptions().get(CommonOptions.JSON.getName()));
    }

    private boolean isJsonArrayColumn(SeaTunnelDataType<?> dataType, Column column) {
        return dataType.getSqlType() == SqlType.ARRAY && isJsonColumn(column);
    }

    private boolean isDynamicField(String fieldName) {
        return MILVUS_INTERNAL_DYNAMIC_FIELD.equals(fieldName);
    }

    private List<Map<String, Object>> flattenedStructArray(
            Map<String, Object> data, String fieldName, Column column) {
        if (data == null) {
            throw new IllegalArgumentException(
                    "Milvus CDC struct array field " + fieldName + " has no record data.");
        }
        Map<String, CreateCollectionReq.FieldSchema> structFields = structFields(column);
        Map<String, Object> childValues = new LinkedHashMap<>();
        Integer structElementCount = null;
        boolean hasPresentChildValue = false;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String childName = flattenedStructChildName(fieldName, entry.getKey());
            if (childName == null) {
                continue;
            }
            if (!structFields.containsKey(childName)) {
                throw new IllegalArgumentException(
                        "Unexpected Milvus CDC struct child field for "
                                + fieldName
                                + ": "
                                + childName);
            }
            Object value = entry.getValue();
            childValues.put(childName, value);
            if (value == null) {
                continue;
            }
            if (!(value instanceof List)) {
                throw new IllegalArgumentException(
                        "Invalid Milvus CDC struct array payload for field "
                                + fieldName
                                + ": child field "
                                + childName
                                + " must be a List, but got "
                                + value.getClass().getName());
            }
            hasPresentChildValue = true;
            int childElementCount = ((List<?>) value).size();
            if (structElementCount == null) {
                structElementCount = childElementCount;
            } else if (structElementCount != childElementCount) {
                throw new IllegalArgumentException(
                        "Invalid Milvus CDC struct array payload for field "
                                + fieldName
                                + ": child field "
                                + childName
                                + " has "
                                + childElementCount
                                + " elements, expected "
                                + structElementCount);
            }
        }
        if (childValues.isEmpty()) {
            throw new IllegalArgumentException(
                    "Milvus CDC struct array field "
                            + fieldName
                            + " has no flattened child fields.");
        }
        List<String> missingChildren = new ArrayList<>();
        for (String expectedChild : structFields.keySet()) {
            if (!childValues.containsKey(expectedChild)) {
                missingChildren.add(expectedChild);
            }
        }
        if (!missingChildren.isEmpty()) {
            throw new IllegalArgumentException(
                    "Invalid Milvus CDC struct array payload for field "
                            + fieldName
                            + ": missing flattened child fields "
                            + missingChildren);
        }
        if (!hasPresentChildValue) {
            return null;
        }
        for (Map.Entry<String, Object> entry : childValues.entrySet()) {
            if (entry.getValue() == null) {
                throw new IllegalArgumentException(
                        "Invalid Milvus CDC struct array payload for field "
                                + fieldName
                                + ": child field "
                                + entry.getKey()
                                + " is null while other child fields are present");
            }
        }
        List<Map<String, Object>> rows = new ArrayList<>(structElementCount);
        for (int i = 0; i < structElementCount; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : childValues.entrySet()) {
                row.put(entry.getKey(), flattenedStructChildValue(entry.getValue(), i));
            }
            rows.add(row);
        }
        return rows;
    }

    private Object flattenedStructChildValue(Object value, int structElementIndex) {
        if (!(value instanceof List)) {
            throw new IllegalArgumentException(
                    "Invalid Milvus CDC struct child value: expected List but got "
                            + value.getClass().getName());
        }
        List<?> values = (List<?>) value;
        if (structElementIndex >= values.size()) {
            throw new IllegalArgumentException(
                    "Invalid Milvus CDC struct child value index "
                            + structElementIndex
                            + ", size="
                            + values.size());
        }
        return values.get(structElementIndex);
    }

    private boolean hasFlattenedStructChildren(Map<String, Object> data, String fieldName) {
        for (String candidate : data.keySet()) {
            if (flattenedStructChildName(fieldName, candidate) != null) {
                return true;
            }
        }
        return false;
    }

    private String flattenedStructChildName(String fieldName, String candidate) {
        String prefix = fieldName + STRUCT_CHILD_OPEN;
        if (!candidate.startsWith(prefix) || !candidate.endsWith(STRUCT_CHILD_CLOSE)) {
            return null;
        }
        return candidate.substring(
                prefix.length(), candidate.length() - STRUCT_CHILD_CLOSE.length());
    }

    private boolean isFlattenedSchemaField(String candidate, Set<String> schemaFieldNames) {
        int bracket = candidate.indexOf(STRUCT_CHILD_OPEN_CHAR);
        if (bracket <= 0 || !candidate.endsWith(STRUCT_CHILD_CLOSE)) {
            return false;
        }
        return schemaFieldNames.contains(candidate.substring(0, bracket));
    }

    private Set<String> schemaFieldNames(MilvusCdcCollectionSchema schema) {
        Set<String> names = new HashSet<>();
        String[] fieldNames = schema.getRowType().getFieldNames();
        for (String fieldName : fieldNames) {
            if (!isDynamicField(fieldName)) {
                names.add(fieldName);
            }
        }
        return names;
    }

    private Map<String, Object> dynamicFields(
            Map<String, Object> data, Set<String> schemaFieldNames) {
        Map<String, Object> dynamicFields = new LinkedHashMap<>();
        if (data == null) {
            return dynamicFields;
        }
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (!schemaFieldNames.contains(entry.getKey())
                    && !isFlattenedSchemaField(entry.getKey(), schemaFieldNames)) {
                if (MILVUS_INTERNAL_DYNAMIC_FIELD.equals(entry.getKey())) {
                    mergeInternalDynamicField(dynamicFields, entry.getValue());
                } else {
                    dynamicFields.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return dynamicFields;
    }

    @SuppressWarnings("unchecked")
    private void mergeInternalDynamicField(Map<String, Object> dynamicFields, Object value) {
        if (value == null) {
            return;
        }
        if (value instanceof Map) {
            dynamicFields.putAll((Map<String, Object>) value);
            return;
        }
        String json = value.toString();
        if (json.isEmpty()) {
            return;
        }
        try {
            Map<String, Object> internalDynamicFields =
                    objectMapper.readValue(json, LinkedHashMap.class);
            dynamicFields.putAll(internalDynamicFields);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "Failed to parse Milvus internal dynamic field "
                            + MILVUS_INTERNAL_DYNAMIC_FIELD
                            + ".",
                    e);
        }
    }

    private String toJson(Object value) {
        if (value instanceof String) {
            return (String) value;
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize Milvus CDC field as JSON.", e);
        }
    }
}
