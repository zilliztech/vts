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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq.FieldSchema;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.GeometryType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    // --- Per-field timezone handling for Timestamptz ---

    @Test
    public void testConvertByMilvusType_Timestamptz_LocalDateTime_PerFieldShanghai() {
        Map<String, ZoneId> overrides = new HashMap<>();
        overrides.put("ts", ZoneId.of("Asia/Shanghai"));
        MilvusSinkConverter shanghaiConverter = new MilvusSinkConverter(
                GeometryConverter.PASSTHROUGH, overrides);
        FieldSchema schema = FieldSchema.builder()
                .name("ts").dataType(DataType.Timestamptz).build();
        LocalDateTime ldt = LocalDateTime.of(2024, 1, 19, 10, 0, 0);
        Object result = shanghaiConverter.convertByMilvusType(schema, ldt);
        // 10:00 Shanghai = 02:00 UTC
        Assertions.assertEquals("2024-01-19T02:00:00Z", result);
    }

    @Test
    public void testConvertByMilvusType_Timestamptz_LocalDateTime_PerFieldUtc() {
        Map<String, ZoneId> overrides = new HashMap<>();
        overrides.put("ts", ZoneId.of("UTC"));
        MilvusSinkConverter utcConverter = new MilvusSinkConverter(
                GeometryConverter.PASSTHROUGH, overrides);
        FieldSchema schema = FieldSchema.builder()
                .name("ts").dataType(DataType.Timestamptz).build();
        LocalDateTime ldt = LocalDateTime.of(2024, 1, 19, 10, 0, 0);
        Object result = utcConverter.convertByMilvusType(schema, ldt);
        Assertions.assertEquals("2024-01-19T10:00:00Z", result);
    }

    @Test
    public void testConvertByMilvusType_Timestamptz_LocalDateTime_PerFieldUtcOffset() {
        // UTC offset format "+08:00" should work the same as IANA "Asia/Shanghai"
        Map<String, ZoneId> overrides = new HashMap<>();
        overrides.put("ts", ZoneId.of("+08:00"));
        MilvusSinkConverter offsetConverter = new MilvusSinkConverter(
                GeometryConverter.PASSTHROUGH, overrides);
        FieldSchema schema = FieldSchema.builder()
                .name("ts").dataType(DataType.Timestamptz).build();
        LocalDateTime ldt = LocalDateTime.of(2024, 1, 19, 10, 0, 0);
        Object result = offsetConverter.convertByMilvusType(schema, ldt);
        // 10:00 +08:00 = 02:00 UTC
        Assertions.assertEquals("2024-01-19T02:00:00Z", result);
    }

    @Test
    public void testConvertByMilvusType_Timestamptz_LocalDateTime_NoOverrideFallsBackToSystemDefault() {
        // No per-field override → systemDefault
        FieldSchema schema = FieldSchema.builder()
                .name("ts").dataType(DataType.Timestamptz).build();
        LocalDateTime ldt = LocalDateTime.of(2024, 1, 19, 10, 0, 0);
        Object result = converter.convertByMilvusType(schema, ldt);
        String expected = ldt.atZone(ZoneId.systemDefault()).toInstant().toString();
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testConvertByMilvusType_Timestamptz_MultipleFieldsDifferentZones() {
        Map<String, ZoneId> overrides = new HashMap<>();
        overrides.put("created_at", ZoneId.of("Asia/Shanghai"));
        overrides.put("updated_at", ZoneId.of("US/Eastern"));
        MilvusSinkConverter multiConverter = new MilvusSinkConverter(
                GeometryConverter.PASSTHROUGH, overrides);

        LocalDateTime ldt = LocalDateTime.of(2024, 6, 15, 12, 0, 0);

        FieldSchema createdSchema = FieldSchema.builder()
                .name("created_at").dataType(DataType.Timestamptz).build();
        Object createdResult = multiConverter.convertByMilvusType(createdSchema, ldt);
        // 12:00 Shanghai (+8) = 04:00 UTC
        Assertions.assertEquals("2024-06-15T04:00:00Z", createdResult);

        FieldSchema updatedSchema = FieldSchema.builder()
                .name("updated_at").dataType(DataType.Timestamptz).build();
        Object updatedResult = multiConverter.convertByMilvusType(updatedSchema, ldt);
        // 12:00 US/Eastern (EDT = -4 in June) = 16:00 UTC
        Assertions.assertEquals("2024-06-15T16:00:00Z", updatedResult);
    }

    @Test
    public void testFromConfig_FieldSchemaTimezone_WiredToConverter() {
        // Simulate field_schema config with timezone
        Map<String, Object> fieldEntry = new HashMap<>();
        fieldEntry.put("field_name", "event_time");
        fieldEntry.put("data_type", 26);  // Timestamptz
        fieldEntry.put("timezone", "Asia/Shanghai");

        List<Object> fieldSchemaList = new java.util.ArrayList<>();
        fieldSchemaList.add(fieldEntry);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("field_schema", fieldSchemaList);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        MilvusSinkConverter shanghaiConverter = MilvusSinkConverter.fromConfig(config);

        FieldSchema schema = FieldSchema.builder()
                .name("event_time").dataType(DataType.Timestamptz).build();
        LocalDateTime ldt = LocalDateTime.of(2024, 1, 19, 10, 0, 0);
        Object result = shanghaiConverter.convertByMilvusType(schema, ldt);
        // 10:00 Shanghai = 02:00 UTC
        Assertions.assertEquals("2024-01-19T02:00:00Z", result);
    }

    @Test
    public void testConvertByMilvusType_Timestamptz_String_WithOverrideUsesConfiguredTz() {
        // String input + per-field timezone → parse as naive wall-clock, apply configured tz
        Map<String, ZoneId> overrides = new HashMap<>();
        overrides.put("ts", ZoneId.of("Asia/Shanghai"));
        MilvusSinkConverter shanghaiConverter = new MilvusSinkConverter(
                GeometryConverter.PASSTHROUGH, overrides);
        FieldSchema schema = FieldSchema.builder()
                .name("ts").dataType(DataType.Timestamptz).build();
        Object result = shanghaiConverter.convertByMilvusType(schema, "2024-01-19 10:00:00");
        // 10:00 Shanghai = 02:00 UTC
        Assertions.assertEquals("2024-01-19T02:00:00Z", result);
    }

    @Test
    public void testConvertByMilvusType_Timestamptz_String_WithoutOverrideUsesOriginalBehavior() {
        // String input + no per-field timezone → original Timestamp.valueOf behavior
        FieldSchema schema = FieldSchema.builder()
                .name("ts").dataType(DataType.Timestamptz).build();
        Object result = converter.convertByMilvusType(schema, "2024-01-19 10:00:00");
        Assertions.assertInstanceOf(String.class, result);
        Assertions.assertTrue(result.toString().contains("2024-01-19"));
    }

    @Test
    public void testConvertByMilvusType_Timestamptz_String_Iso8601PassedThrough() {
        // ISO 8601 with offset — should pass through even with per-field timezone
        Map<String, ZoneId> overrides = new HashMap<>();
        overrides.put("ts", ZoneId.of("Asia/Shanghai"));
        MilvusSinkConverter shanghaiConverter = new MilvusSinkConverter(
                GeometryConverter.PASSTHROUGH, overrides);
        FieldSchema schema = FieldSchema.builder()
                .name("ts").dataType(DataType.Timestamptz).build();
        Object result = shanghaiConverter.convertByMilvusType(schema, "2024-01-19T10:00:00Z");
        // Already has offset Z — LocalDateTime.parse fails, falls through as-is
        Assertions.assertEquals("2024-01-19T10:00:00Z", result);
    }

    @Test
    public void testConvertByMilvusType_Timestamptz_OffsetDateTime_IgnoresPerFieldTimezone() {
        // OffsetDateTime already carries offset — per-field timezone must NOT affect it
        Map<String, ZoneId> overrides = new HashMap<>();
        overrides.put("ts", ZoneId.of("Asia/Shanghai"));
        MilvusSinkConverter shanghaiConverter = new MilvusSinkConverter(
                GeometryConverter.PASSTHROUGH, overrides);
        FieldSchema schema = FieldSchema.builder()
                .name("ts").dataType(DataType.Timestamptz).build();
        OffsetDateTime odt = OffsetDateTime.of(2024, 1, 19, 10, 0, 0, 0, ZoneOffset.ofHours(5));
        Object result = shanghaiConverter.convertByMilvusType(schema, odt);
        // 10:00+05 = 05:00 UTC, regardless of per-field timezone setting
        Assertions.assertEquals("2024-01-19T05:00:00Z", result);
    }

    @Test
    public void testConvertBySeaTunnelType_TimestampTz_LocalDateTime_UsesSystemDefault() {
        // convertBySeaTunnelType has no field name, always uses systemDefault
        LocalDateTime ldt = LocalDateTime.of(2024, 1, 19, 10, 0, 0);
        Object result = converter.convertBySeaTunnelType(
                LocalTimeType.OFFSET_DATE_TIME_TYPE, false, ldt);
        Assertions.assertInstanceOf(String.class, result);
        String expected = ldt.atZone(ZoneId.systemDefault()).toInstant().toString();
        Assertions.assertEquals(expected, result);
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
    // --- JSON target fields keep JsonElement semantics ---

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
    public void testConvertByMilvusType_JSON_FromJsonPrimitiveString() {
        FieldSchema schema = FieldSchema.builder()
                .name("metadata").dataType(DataType.JSON).build();
        JsonPrimitive input = new JsonPrimitive("hello");

        Object result = converter.convertByMilvusType(schema, input);

        Assertions.assertInstanceOf(JsonPrimitive.class, result);
        Assertions.assertEquals("hello", ((JsonPrimitive) result).getAsString());
    }

    @Test
    public void testBuildMilvusData_MetadataFieldConvertedByTargetSchema() {
        SeaTunnelRowType rowType = new SeaTunnelRowType(
                new String[]{CommonOptions.METADATA.getName()},
                new SeaTunnelDataType<?>[]{BasicType.STRING_TYPE});
        CatalogTable catalogTable = buildCatalogTable(rowType);

        JsonObject metadata = new JsonObject();
        metadata.addProperty("store_id", 100001);
        metadata.addProperty("msid", "MSID-A001");
        SeaTunnelRow row = new SeaTunnelRow(new Object[]{metadata.toString()});

        DescribeCollectionResp describeCollectionResp = buildDescribeCollectionResp(
                FieldSchema.builder()
                        .name("store_id")
                        .dataType(DataType.VarChar)
                        .maxLength(200)
                        .build(),
                FieldSchema.builder()
                        .name("milvus_msid")
                        .dataType(DataType.VarChar)
                        .maxLength(200)
                        .build());
        Map<String, String> fieldMap = new HashMap<>();
        fieldMap.put("store_id", "store_id");
        fieldMap.put("msid", "milvus_msid");

        JsonObject data = converter.buildMilvusData(
                catalogTable, describeCollectionResp, fieldMap, row);

        Assertions.assertTrue(data.get("store_id").getAsJsonPrimitive().isString());
        Assertions.assertEquals("100001", data.get("store_id").getAsString());
        Assertions.assertFalse(data.has("msid"));
        Assertions.assertTrue(data.get("milvus_msid").getAsJsonPrimitive().isString());
        Assertions.assertEquals("MSID-A001", data.get("milvus_msid").getAsString());
    }

    @Test
    public void testBuildMilvusData_MetadataArrayPassedThrough() {
        SeaTunnelRowType rowType = new SeaTunnelRowType(
                new String[]{CommonOptions.METADATA.getName()},
                new SeaTunnelDataType<?>[]{BasicType.STRING_TYPE});
        CatalogTable catalogTable = buildCatalogTable(rowType);

        JsonArray tags = new JsonArray();
        tags.add(100001);
        tags.add("MSID-A001");
        JsonObject metadata = new JsonObject();
        metadata.add("tags", tags);
        SeaTunnelRow row = new SeaTunnelRow(new Object[]{metadata.toString()});

        DescribeCollectionResp describeCollectionResp = buildDescribeCollectionResp(
                FieldSchema.builder()
                        .name("tags")
                        .dataType(DataType.Array)
                        .elementType(DataType.VarChar)
                        .maxCapacity(10)
                        .maxLength(200)
                        .build());
        Map<String, String> fieldMap = new HashMap<>();
        fieldMap.put("tags", "tags");

        JsonObject data = converter.buildMilvusData(
                catalogTable, describeCollectionResp, fieldMap, row);

        JsonArray result = data.getAsJsonArray("tags");
        Assertions.assertTrue(result.get(0).getAsJsonPrimitive().isNumber());
        Assertions.assertEquals(100001, result.get(0).getAsInt());
        Assertions.assertTrue(result.get(1).getAsJsonPrimitive().isString());
        Assertions.assertEquals("MSID-A001", result.get(1).getAsString());
    }

    // --- Geometry: in practice every VTS source connector emits Geometry as
    //     String. The sink tests below therefore only cover the String path
    //     (PASSTHROUGH and PARSE) and the contract that PASSTHROUGH returns
    //     whatever it gets, as-is. Byte-level WKB tests live alongside
    //     GeometryConverter.convertWkbBytes in the geometry-specific test class.

    @Test
    public void testConvertByMilvusType_Geometry_StringWkt() {
        FieldSchema schema = FieldSchema.builder()
                .name("loc").dataType(DataType.Geometry).build();
        Object result = converter.convertByMilvusType(schema, "POINT (1 2)");
        Assertions.assertEquals("POINT (1 2)", result);
    }

    @Test
    public void testConvertByMilvusType_Geometry_String3DWktPassthrough() {
        FieldSchema schema = FieldSchema.builder()
                .name("loc").dataType(DataType.Geometry).build();
        Object result = converter.convertByMilvusType(schema, "POINT Z (1 2 3)");
        Assertions.assertEquals("POINT Z (1 2 3)", result);
    }

    @Test
    public void testConvertByMilvusType_Geometry_NullStaysNull() {
        FieldSchema schema = FieldSchema.builder()
                .name("loc").dataType(DataType.Geometry).build();
        Assertions.assertNull(converter.convertByMilvusType(schema, null));
    }

    @Test
    public void testConvertByMilvusType_Geometry_PassthroughReturnsAnyValueAsIs() {
        // The core PASSTHROUGH contract: whatever arrives, goes out unchanged.
        // Matches the original connector behavior (literal `return value;`)
        // before any geometry conversion code was added.
        FieldSchema schema = FieldSchema.builder()
                .name("loc").dataType(DataType.Geometry).build();
        HashMap<String, Object> map = new HashMap<>();
        map.put("k", "v");
        Assertions.assertSame(map, converter.convertByMilvusType(schema, map));
        Assertions.assertEquals(42, converter.convertByMilvusType(schema, 42));
        Assertions.assertEquals("   not trimmed   ",
                converter.convertByMilvusType(schema, "   not trimmed   "));
    }

    @Test
    public void testConvertByMilvusType_Geometry_ParseMode_UnsupportedTypeThrows() {
        // Under PARSE mode, non-String values are rejected loudly.
        MilvusSinkConverter parseConverter = parseConverterWith(GeometryConverter.CoordinateOrder.LAT_LON);
        FieldSchema schema = FieldSchema.builder()
                .name("loc").dataType(DataType.Geometry).build();
        Assertions.assertThrows(MilvusConnectorException.class,
                () -> parseConverter.convertByMilvusType(schema, new HashMap<String, Object>()));
        Assertions.assertThrows(MilvusConnectorException.class,
                () -> parseConverter.convertByMilvusType(schema, 42));
    }

    @Test
    public void testConvertBySeaTunnelType_Geometry_StringWkt() {
        Object result = converter.convertBySeaTunnelType(
                GeometryType.GEOMETRY_TYPE, false, "POINT (1 2)");
        Assertions.assertEquals("POINT (1 2)", result);
    }

    @Test
    public void testConvertBySeaTunnelType_Geometry_PassthroughReturnsAnyValueAsIs() {
        HashMap<String, Object> map = new HashMap<>();
        Assertions.assertSame(map, converter.convertBySeaTunnelType(
                GeometryType.GEOMETRY_TYPE, false, map));
    }

    @Test
    public void testConvertBySeaTunnelType_Geometry_ParseMode_UnsupportedTypeThrows() {
        MilvusSinkConverter parseConverter = parseConverterWith(GeometryConverter.CoordinateOrder.LAT_LON);
        Assertions.assertThrows(MilvusConnectorException.class,
                () -> parseConverter.convertBySeaTunnelType(
                        GeometryType.GEOMETRY_TYPE, false, new HashMap<String, Object>()));
    }

    // --- Geometry coordinate-string ordering: configurable per converter instance ---
    //
    // All "lat,lon" / "lon,lat" tests below require PARSE mode — bare numeric
    // strings are only interpreted in PARSE mode. The default no-arg
    // MilvusSinkConverter (PASSTHROUGH mode) would pass them to Milvus
    // unchanged, where Milvus would reject them as malformed WKT.

    private static MilvusSinkConverter parseConverterWith(GeometryConverter.CoordinateOrder order) {
        return new MilvusSinkConverter(new GeometryConverter(GeometryConverter.ConvertMode.PARSE, order));
    }

    @Test
    public void testGeometry_DefaultParseOrder_LatLon() {
        // PARSE mode + default LAT_LON order (Elasticsearch convention)
        MilvusSinkConverter parseConverter = parseConverterWith(GeometryConverter.CoordinateOrder.LAT_LON);
        FieldSchema schema = FieldSchema.builder()
                .name("loc").dataType(DataType.Geometry).build();
        // "31.23,121.47" → lat=31.23, lon=121.47 → POINT (lon lat)
        Object result = parseConverter.convertByMilvusType(schema, "31.23,121.47");
        Assertions.assertEquals("POINT (121.47 31.23)", result);
    }

    @Test
    public void testGeometry_LonLatOrder_OptIn() {
        // Opt-in lon,lat ordering (e.g. for PostgreSQL text point sources)
        MilvusSinkConverter lonLatConverter = parseConverterWith(GeometryConverter.CoordinateOrder.LON_LAT);
        FieldSchema schema = FieldSchema.builder()
                .name("loc").dataType(DataType.Geometry).build();
        // "121.47,31.23" → lon=121.47, lat=31.23 → POINT (lon lat)
        Object result = lonLatConverter.convertByMilvusType(schema, "121.47,31.23");
        Assertions.assertEquals("POINT (121.47 31.23)", result);
    }

    @Test
    public void testGeometry_OrderAffectsBareStringOnly() {
        // Same input under different orders → opposite coordinates,
        // proving the option is plumbed all the way through
        MilvusSinkConverter latLon = parseConverterWith(GeometryConverter.CoordinateOrder.LAT_LON);
        MilvusSinkConverter lonLat = parseConverterWith(GeometryConverter.CoordinateOrder.LON_LAT);
        FieldSchema schema = FieldSchema.builder()
                .name("loc").dataType(DataType.Geometry).build();
        Assertions.assertEquals("POINT (20 10)", latLon.convertByMilvusType(schema, "10,20"));
        Assertions.assertEquals("POINT (10 20)", lonLat.convertByMilvusType(schema, "10,20"));
    }

    @Test
    public void testGeometry_OrderDoesNotAffectWkt() {
        // WKT is unambiguous and pass-through — order option must NOT touch it
        MilvusSinkConverter lonLat = parseConverterWith(GeometryConverter.CoordinateOrder.LON_LAT);
        FieldSchema schema = FieldSchema.builder()
                .name("loc").dataType(DataType.Geometry).build();
        Assertions.assertEquals("POINT (10 20)", lonLat.convertByMilvusType(schema, "POINT (10 20)"));
    }

    @Test
    public void testGeometry_OrderDoesNotAffectGeoJson() {
        // GeoJSON has its own canonical order ([lon,lat]) — option must not touch it
        MilvusSinkConverter latLon = parseConverterWith(GeometryConverter.CoordinateOrder.LAT_LON);
        FieldSchema schema = FieldSchema.builder()
                .name("loc").dataType(DataType.Geometry).build();
        // GeoJSON [10,20] is always (lon=10, lat=20)
        Object result = latLon.convertByMilvusType(schema,
                "{\"type\":\"Point\",\"coordinates\":[10,20]}");
        Assertions.assertEquals("POINT (10 20)", result);
    }

    // --- ConvertMode: production default is PASSTHROUGH ---

    @Test
    public void testGeometry_DefaultMode_IsPassthrough() {
        // The no-arg MilvusSinkConverter() must use PASSTHROUGH mode so that
        // existing Milvus → Milvus configs continue to work without any change.
        FieldSchema schema = FieldSchema.builder()
                .name("loc").dataType(DataType.Geometry).build();
        // GeoJSON-shaped string in PASSTHROUGH mode is NOT converted — it
        // flows to the destination unchanged. Proves the no-arg converter
        // is not running the parse logic.
        String geoJson = "{\"type\":\"Point\",\"coordinates\":[1,2]}";
        Object result = converter.convertByMilvusType(schema, geoJson);
        Assertions.assertEquals(geoJson, result,
                "Default no-arg converter must be PASSTHROUGH (input returned unchanged)");
    }

    @Test
    public void testGeometry_PassthroughMode_DoesNotStripSrid() {
        // EWKT SRID prefix is NOT stripped in PASSTHROUGH — Milvus would reject
        // this input, but VTS hands it over byte-for-byte. The point of the
        // test is to lock in the "zero processing" contract.
        FieldSchema schema = FieldSchema.builder()
                .name("loc").dataType(DataType.Geometry).build();
        String ewkt = "SRID=4326;POINT(1 2)";
        Object result = converter.convertByMilvusType(schema, ewkt);
        Assertions.assertEquals(ewkt, result,
                "PASSTHROUGH must not strip SRID — that's a PARSE-mode behavior");
    }

    @Test
    public void testGeometry_ParseMode_DoesStripSrid() {
        // The exact same input under PARSE mode strips the SRID prefix.
        MilvusSinkConverter parseConverter = parseConverterWith(GeometryConverter.CoordinateOrder.LAT_LON);
        FieldSchema schema = FieldSchema.builder()
                .name("loc").dataType(DataType.Geometry).build();
        Object result = parseConverter.convertByMilvusType(schema, "SRID=4326;POINT(1 2)");
        Assertions.assertEquals("POINT(1 2)", result);
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

    private static CatalogTable buildCatalogTable(SeaTunnelRowType rowType) {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        String[] fieldNames = rowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            schemaBuilder.column(PhysicalColumn.of(
                    fieldNames[i],
                    rowType.getFieldType(i),
                    0L,
                    true,
                    null,
                    null,
                    null,
                    null));
        }
        return CatalogTable.of(
                TableIdentifier.of("test", TablePath.of("default", "test_collection")),
                schemaBuilder.build(),
                new HashMap<>(),
                new ArrayList<>(),
                "");
    }

    private static DescribeCollectionResp buildDescribeCollectionResp(FieldSchema... fields) {
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(true)
                .fieldSchemaList(Arrays.asList(fields))
                .functionList(new ArrayList<>())
                .build();
        return DescribeCollectionResp.builder()
                .collectionName("test_collection")
                .primaryFieldName("id")
                .enableDynamicField(true)
                .autoID(false)
                .collectionSchema(schema)
                .build();
    }
}
