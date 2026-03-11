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

import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq.FieldSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

public class MilvusSinkConverterTest {

    private final MilvusSinkConverter converter = new MilvusSinkConverter();

    // --- C1: convertBySeaTunnelType TIMESTAMP_TZ returns ISO 8601 String ---

    @Test
    public void testConvertBySeaTunnelType_TimestampTz_SqlTimestamp() {
        java.sql.Timestamp ts = java.sql.Timestamp.from(
                OffsetDateTime.of(2024, 1, 19, 11, 30, 45, 0, ZoneOffset.UTC).toInstant());
        Object result = converter.convertBySeaTunnelType(
                LocalTimeType.OFFSET_DATE_TIME_TYPE, false, ts);
        Assertions.assertInstanceOf(String.class, result);
        Assertions.assertEquals("2024-01-19T11:30:45Z", result);
    }

    @Test
    public void testConvertBySeaTunnelType_TimestampTz_OffsetDateTime() {
        OffsetDateTime odt = OffsetDateTime.of(2024, 6, 15, 8, 0, 0, 0, ZoneOffset.ofHours(8));
        Object result = converter.convertBySeaTunnelType(
                LocalTimeType.OFFSET_DATE_TIME_TYPE, false, odt);
        Assertions.assertInstanceOf(String.class, result);
        Assertions.assertEquals("2024-06-15T00:00:00Z", result);
    }

    @Test
    public void testConvertBySeaTunnelType_TimestampTz_Null() {
        Object result = converter.convertBySeaTunnelType(
                LocalTimeType.OFFSET_DATE_TIME_TYPE, false, null);
        Assertions.assertNull(result);
    }

    @Test
    public void testConvertBySeaTunnelType_TimestampTz_String() {
        Object result = converter.convertBySeaTunnelType(
                LocalTimeType.OFFSET_DATE_TIME_TYPE, false, "2024-01-19T11:30:45Z");
        Assertions.assertEquals("2024-01-19T11:30:45Z", result);
    }

    // --- convertByMilvusType Timestamptz ---

    @Test
    public void testConvertByMilvusType_Timestamptz_SqlTimestamp() {
        FieldSchema schema = FieldSchema.builder()
                .name("ts").dataType(DataType.Timestamptz).build();
        java.sql.Timestamp ts = java.sql.Timestamp.from(
                OffsetDateTime.of(2024, 1, 19, 11, 30, 45, 0, ZoneOffset.UTC).toInstant());
        Object result = converter.convertByMilvusType(schema, ts);
        Assertions.assertInstanceOf(String.class, result);
        Assertions.assertEquals("2024-01-19T11:30:45Z", result);
    }

    @Test
    public void testConvertByMilvusType_Timestamptz_OffsetDateTime() {
        FieldSchema schema = FieldSchema.builder()
                .name("ts").dataType(DataType.Timestamptz).build();
        OffsetDateTime odt = OffsetDateTime.of(2024, 6, 15, 8, 0, 0, 0, ZoneOffset.ofHours(8));
        Object result = converter.convertByMilvusType(schema, odt);
        Assertions.assertInstanceOf(String.class, result);
        Assertions.assertEquals("2024-06-15T00:00:00Z", result);
    }

    @Test
    public void testConvertByMilvusType_Timestamptz_StringTimestamp() {
        FieldSchema schema = FieldSchema.builder()
                .name("ts").dataType(DataType.Timestamptz).build();
        Object result = converter.convertByMilvusType(schema, "2024-01-19 11:30:45");
        Assertions.assertInstanceOf(String.class, result);
        Assertions.assertTrue(result.toString().contains("2024-01-19"));
    }

    @Test
    public void testConvertByMilvusType_Timestamptz_StringIso8601() {
        FieldSchema schema = FieldSchema.builder()
                .name("ts").dataType(DataType.Timestamptz).build();
        Object result = converter.convertByMilvusType(schema, "2024-01-19T11:30:45Z");
        Assertions.assertEquals("2024-01-19T11:30:45Z", result);
    }

    @Test
    public void testConvertByMilvusType_Timestamptz_Null() {
        FieldSchema schema = FieldSchema.builder()
                .name("ts").dataType(DataType.Timestamptz).build();
        Object result = converter.convertByMilvusType(schema, null);
        Assertions.assertNull(result);
    }

    // --- S4: IllegalArgumentException instead of broad Exception ---

    @Test
    public void testConvertByMilvusType_Timestamptz_InvalidStringFallback() {
        FieldSchema schema = FieldSchema.builder()
                .name("ts").dataType(DataType.Timestamptz).build();
        // Not a valid Timestamp.valueOf format, should fall through to return as-is
        Object result = converter.convertByMilvusType(schema, "not-a-date");
        Assertions.assertEquals("not-a-date", result);
    }

    // --- Consistency: both paths return same type for same input ---

    @Test
    public void testTimestampTz_BothPaths_ReturnString() {
        java.sql.Timestamp ts = java.sql.Timestamp.from(
                OffsetDateTime.of(2024, 1, 19, 11, 30, 45, 0, ZoneOffset.UTC).toInstant());

        Object seaTunnelResult = converter.convertBySeaTunnelType(
                LocalTimeType.OFFSET_DATE_TIME_TYPE, false, ts);
        FieldSchema schema = FieldSchema.builder()
                .name("ts").dataType(DataType.Timestamptz).build();
        Object milvusResult = converter.convertByMilvusType(schema, ts);

        Assertions.assertInstanceOf(String.class, seaTunnelResult);
        Assertions.assertInstanceOf(String.class, milvusResult);
        Assertions.assertEquals(seaTunnelResult, milvusResult);
    }

    // --- ROW containing TIMESTAMP_TZ sub-field ---

    @Test
    public void testConvertBySeaTunnelType_Row_WithTimestampTzSubField() {
        // Build a ROW type: { name: STRING, event_time: TIMESTAMP_TZ }
        SeaTunnelRowType rowType = new SeaTunnelRowType(
                new String[]{"name", "event_time"},
                new SeaTunnelDataType<?>[]{BasicType.STRING_TYPE, LocalTimeType.OFFSET_DATE_TIME_TYPE}
        );

        java.sql.Timestamp ts = java.sql.Timestamp.from(
                OffsetDateTime.of(2024, 1, 19, 11, 30, 45, 0, ZoneOffset.UTC).toInstant());
        SeaTunnelRow row = new SeaTunnelRow(new Object[]{"test_event", ts});

        Object result = converter.convertBySeaTunnelType(rowType, false, row);

        // Should return a JsonObject
        Assertions.assertInstanceOf(JsonObject.class, result);
        JsonObject json = (JsonObject) result;

        // name should be a string
        Assertions.assertEquals("test_event", json.get("name").getAsString());

        // event_time should be an ISO 8601 string, NOT a number
        Assertions.assertTrue(json.get("event_time").getAsJsonPrimitive().isString(),
                "TIMESTAMP_TZ sub-field in ROW should be serialized as String, not Number");
        Assertions.assertEquals("2024-01-19T11:30:45Z", json.get("event_time").getAsString());
    }

    @Test
    public void testConvertBySeaTunnelType_Row_WithTimestampTzSubField_MultipleTimezones() {
        SeaTunnelRowType rowType = new SeaTunnelRowType(
                new String[]{"name", "event_time", "created_at"},
                new SeaTunnelDataType<?>[]{
                        BasicType.STRING_TYPE,
                        LocalTimeType.OFFSET_DATE_TIME_TYPE,
                        LocalTimeType.OFFSET_DATE_TIME_TYPE
                }
        );

        // event_time: Shanghai +08:00 -> should convert to UTC
        java.sql.Timestamp shanghaiTs = java.sql.Timestamp.from(
                OffsetDateTime.of(2024, 6, 15, 8, 0, 0, 0, ZoneOffset.ofHours(8)).toInstant());
        // created_at: New York -05:00 -> should convert to UTC
        java.sql.Timestamp nyTs = java.sql.Timestamp.from(
                OffsetDateTime.of(2024, 3, 10, 14, 30, 0, 0, ZoneOffset.ofHours(-5)).toInstant());

        SeaTunnelRow row = new SeaTunnelRow(new Object[]{"multi_tz", shanghaiTs, nyTs});

        Object result = converter.convertBySeaTunnelType(rowType, false, row);
        JsonObject json = (JsonObject) result;

        // Shanghai 08:00+08 = 00:00 UTC
        Assertions.assertTrue(json.get("event_time").getAsJsonPrimitive().isString());
        Assertions.assertEquals("2024-06-15T00:00:00Z", json.get("event_time").getAsString());

        // New York 14:30-05 = 19:30 UTC
        Assertions.assertTrue(json.get("created_at").getAsJsonPrimitive().isString());
        Assertions.assertEquals("2024-03-10T19:30:00Z", json.get("created_at").getAsString());
    }

    @Test
    public void testConvertBySeaTunnelType_Row_WithNullTimestampTzSubField() {
        SeaTunnelRowType rowType = new SeaTunnelRowType(
                new String[]{"name", "event_time"},
                new SeaTunnelDataType<?>[]{BasicType.STRING_TYPE, LocalTimeType.OFFSET_DATE_TIME_TYPE}
        );

        SeaTunnelRow row = new SeaTunnelRow(new Object[]{"null_time", null});

        Object result = converter.convertBySeaTunnelType(rowType, false, row);
        JsonObject json = (JsonObject) result;

        Assertions.assertEquals("null_time", json.get("name").getAsString());
        // null sub-field should be serialized as JsonNull
        Assertions.assertTrue(json.get("event_time").isJsonNull());
    }

    // --- convertByMilvusType with JsonElement inputs (dynamic field fix) ---

    @Test
    public void testConvertByMilvusType_VarChar_FromJsonPrimitiveNumber() {
        FieldSchema schema = FieldSchema.builder()
                .name("store_id").dataType(DataType.VarChar).maxLength(200).build();
        // Simulates: Qdrant keyword field with numeric value stored as JsonPrimitive(12345)
        JsonPrimitive input = new JsonPrimitive(12345);
        Object result = converter.convertByMilvusType(schema, input);
        Assertions.assertInstanceOf(String.class, result);
        Assertions.assertEquals("12345", result);
    }

    @Test
    public void testConvertByMilvusType_VarChar_FromJsonPrimitiveString() {
        FieldSchema schema = FieldSchema.builder()
                .name("name").dataType(DataType.VarChar).maxLength(200).build();
        JsonPrimitive input = new JsonPrimitive("hello");
        Object result = converter.convertByMilvusType(schema, input);
        Assertions.assertInstanceOf(String.class, result);
        Assertions.assertEquals("hello", result);
    }

    @Test
    public void testConvertByMilvusType_Int64_FromJsonPrimitiveNumber() {
        FieldSchema schema = FieldSchema.builder()
                .name("count").dataType(DataType.Int64).build();
        JsonPrimitive input = new JsonPrimitive(99999);
        Object result = converter.convertByMilvusType(schema, input);
        Assertions.assertInstanceOf(Long.class, result);
        Assertions.assertEquals(99999L, result);
    }

    @Test
    public void testConvertByMilvusType_Double_FromJsonPrimitiveNumber() {
        FieldSchema schema = FieldSchema.builder()
                .name("score").dataType(DataType.Double).build();
        JsonPrimitive input = new JsonPrimitive(9.5);
        Object result = converter.convertByMilvusType(schema, input);
        Assertions.assertInstanceOf(Double.class, result);
        Assertions.assertEquals(9.5, (Double) result, 0.001);
    }

    @Test
    public void testConvertByMilvusType_Bool_FromJsonPrimitiveBoolean() {
        FieldSchema schema = FieldSchema.builder()
                .name("active").dataType(DataType.Bool).build();
        JsonPrimitive input = new JsonPrimitive(true);
        Object result = converter.convertByMilvusType(schema, input);
        Assertions.assertInstanceOf(Boolean.class, result);
        Assertions.assertEquals(true, result);
    }

    @Test
    public void testConvertByMilvusType_JSON_FromJsonObject() {
        FieldSchema schema = FieldSchema.builder()
                .name("meta").dataType(DataType.JSON).build();
        JsonObject input = new JsonObject();
        input.addProperty("key", "value");
        Object result = converter.convertByMilvusType(schema, input);
        Assertions.assertInstanceOf(JsonObject.class, result);
        Assertions.assertEquals("value", ((JsonObject) result).get("key").getAsString());
    }

    @Test
    public void testConvertByMilvusType_VarChar_FromJsonNull() {
        FieldSchema schema = FieldSchema.builder()
                .name("field").dataType(DataType.VarChar).maxLength(200).build();
        Object result = converter.convertByMilvusType(schema, JsonNull.INSTANCE);
        Assertions.assertNull(result);
    }

    // --- FloatVector from Double[] input ---

    @Test
    @SuppressWarnings("unchecked")
    public void testConvertByMilvusType_FloatVector_FromDoubleArray() {
        FieldSchema schema = FieldSchema.builder()
                .name("embedding").dataType(DataType.FloatVector).dimension(4).build();
        Double[] input = new Double[]{1.0, 2.5, 3.7, 4.2};
        Object result = converter.convertByMilvusType(schema, input);
        Assertions.assertInstanceOf(List.class, result);
        List<Float> floatList = (List<Float>) result;
        Assertions.assertEquals(4, floatList.size());
        Assertions.assertEquals(1.0f, floatList.get(0));
        Assertions.assertEquals(2.5f, floatList.get(1));
        Assertions.assertEquals(3.7f, floatList.get(2));
        Assertions.assertEquals(4.2f, floatList.get(3));
    }
}
