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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize.source;

import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;

public class DefaultSeaTunnelRowDeserializerTest {

    // ==================== Date format map tests ====================

    @Test
    public void testDeserializeDateWithCustomSlashFormat() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name", "created_at"},
                        new SeaTunnelDataType[] {
                            STRING_TYPE, LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });

        Map<String, Map<String, Object>> columnOptionsMap = new HashMap<>();
        columnOptionsMap.put("created_at", Collections.singletonMap("format", "yyyy/MM/dd"));

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType, columnOptionsMap);

        Map<String, Object> doc = new HashMap<>();
        doc.put("name", "test");
        doc.put("created_at", "2025/01/15");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("name", "created_at"), "table1");
        SeaTunnelRow row = deserializer.deserialize(record);

        Assertions.assertEquals("test", row.getField(0));
        LocalDateTime dateTime = (LocalDateTime) row.getField(1);
        Assertions.assertEquals(2025, dateTime.getYear());
        Assertions.assertEquals(1, dateTime.getMonthValue());
        Assertions.assertEquals(15, dateTime.getDayOfMonth());
    }

    @Test
    public void testDeserializeDateWithEpochSecondFormat() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        Map<String, Map<String, Object>> columnOptionsMap = new HashMap<>();
        columnOptionsMap.put("ts", Collections.singletonMap("format", "epoch_second"));

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType, columnOptionsMap);

        Map<String, Object> doc = new HashMap<>();
        // 2025-01-26 00:00:00 UTC = 1737849600
        doc.put("ts", "1737849600");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("ts"), "table1");
        SeaTunnelRow row = deserializer.deserialize(record);

        LocalDateTime dateTime = (LocalDateTime) row.getField(0);
        Assertions.assertNotNull(dateTime);
        Assertions.assertEquals(2025, dateTime.getYear());
    }

    @Test
    public void testDeserializeDateWithEpochMillisFormat() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        Map<String, Map<String, Object>> columnOptionsMap = new HashMap<>();
        columnOptionsMap.put("ts", Collections.singletonMap("format", "epoch_millis"));

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType, columnOptionsMap);

        Map<String, Object> doc = new HashMap<>();
        doc.put("ts", "1737849600000");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("ts"), "table1");
        SeaTunnelRow row = deserializer.deserialize(record);

        LocalDateTime dateTime = (LocalDateTime) row.getField(0);
        Assertions.assertNotNull(dateTime);
        Assertions.assertEquals(2025, dateTime.getYear());
    }

    @Test
    public void testDeserializeDateWithMultiFormat() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        Map<String, Map<String, Object>> columnOptionsMap = new HashMap<>();
        columnOptionsMap.put(
                "ts",
                Collections.singletonMap("format", "strict_date_optional_time||epoch_millis"));

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType, columnOptionsMap);

        // Test with ISO date string
        Map<String, Object> doc1 = new HashMap<>();
        doc1.put("ts", "2025-01-15T10:30:00.000Z");
        ElasticsearchRecord record1 =
                new ElasticsearchRecord(doc1, Arrays.asList("ts"), "table1");
        SeaTunnelRow row1 = deserializer.deserialize(record1);
        LocalDateTime dt1 = (LocalDateTime) row1.getField(0);
        Assertions.assertEquals(2025, dt1.getYear());
        Assertions.assertEquals(1, dt1.getMonthValue());
        Assertions.assertEquals(15, dt1.getDayOfMonth());

        // Test with epoch millis
        Map<String, Object> doc2 = new HashMap<>();
        doc2.put("ts", "1737849600000");
        ElasticsearchRecord record2 =
                new ElasticsearchRecord(doc2, Arrays.asList("ts"), "table1");
        SeaTunnelRow row2 = deserializer.deserialize(record2);
        LocalDateTime dt2 = (LocalDateTime) row2.getField(0);
        Assertions.assertNotNull(dt2);
        Assertions.assertEquals(2025, dt2.getYear());
    }

    @Test
    public void testDeserializeDateWithDdMmYyyyFormat() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"date_field"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        Map<String, Map<String, Object>> columnOptionsMap = new HashMap<>();
        columnOptionsMap.put("date_field", Collections.singletonMap("format", "dd-MM-yyyy"));

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType, columnOptionsMap);

        Map<String, Object> doc = new HashMap<>();
        doc.put("date_field", "15-01-2025");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("date_field"), "table1");
        SeaTunnelRow row = deserializer.deserialize(record);

        LocalDateTime dateTime = (LocalDateTime) row.getField(0);
        Assertions.assertNotNull(dateTime);
        Assertions.assertEquals(2025, dateTime.getYear());
        Assertions.assertEquals(1, dateTime.getMonthValue());
        Assertions.assertEquals(15, dateTime.getDayOfMonth());
    }

    // ==================== Backward compatibility tests ====================

    @Test
    public void testDeserializeDateFallbackWithoutFormatMap() {
        // Without format map, should still work with the old heuristic
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType);

        Map<String, Object> doc = new HashMap<>();
        doc.put("ts", "2025-01-15 10:30:00");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("ts"), "table1");
        SeaTunnelRow row = deserializer.deserialize(record);

        LocalDateTime dateTime = (LocalDateTime) row.getField(0);
        Assertions.assertEquals(2025, dateTime.getYear());
        Assertions.assertEquals(10, dateTime.getHour());
        Assertions.assertEquals(30, dateTime.getMinute());
    }

    @Test
    public void testDeserializeDateFallbackWithEmptyFormatMap() {
        // Empty format map, should still work with the old heuristic
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType, Collections.emptyMap());

        Map<String, Object> doc = new HashMap<>();
        doc.put("ts", "2025-01-15 10:30:00.123");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("ts"), "table1");
        SeaTunnelRow row = deserializer.deserialize(record);

        LocalDateTime dateTime = (LocalDateTime) row.getField(0);
        Assertions.assertEquals(2025, dateTime.getYear());
        Assertions.assertEquals(123000000, dateTime.getNano());
    }

    @Test
    public void testDeserializeDateFallbackTimestamp() {
        // Epoch millis should work via fallback even without format map
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType);

        Map<String, Object> doc = new HashMap<>();
        doc.put("ts", "1737849600000");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("ts"), "table1");
        SeaTunnelRow row = deserializer.deserialize(record);

        LocalDateTime dateTime = (LocalDateTime) row.getField(0);
        Assertions.assertNotNull(dateTime);
        Assertions.assertEquals(2025, dateTime.getYear());
    }

    @Test
    public void testDeserializeDateFallbackIsoFormat() {
        // ISO format with T and Z should work via fallback
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType);

        Map<String, Object> doc = new HashMap<>();
        doc.put("ts", "2025-01-15T10:30:00Z");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("ts"), "table1");
        SeaTunnelRow row = deserializer.deserialize(record);

        // Z = UTC, converted to system default timezone
        LocalDateTime expected = LocalDateTime.ofInstant(
                Instant.parse("2025-01-15T10:30:00Z"), ZoneId.systemDefault());
        LocalDateTime dateTime = (LocalDateTime) row.getField(0);
        Assertions.assertEquals(expected, dateTime);
    }

    // ==================== Timezone offset fallback tests ====================

    @Test
    public void testDeserializeDateFallbackWithPositiveOffset() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType);

        Map<String, Object> doc = new HashMap<>();
        doc.put("ts", "2025-02-07 03:06:16.693985+00:00");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("ts"), "table1");
        SeaTunnelRow row = deserializer.deserialize(record);

        // +00:00 = UTC, converted to system default timezone
        LocalDateTime expected = LocalDateTime.ofInstant(
                Instant.parse("2025-02-07T03:06:16.693985Z"), ZoneId.systemDefault());
        LocalDateTime dateTime = (LocalDateTime) row.getField(0);
        Assertions.assertNotNull(dateTime);
        Assertions.assertEquals(expected, dateTime);
    }

    @Test
    public void testDeserializeDateFallbackWithNegativeOffset() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType);

        Map<String, Object> doc = new HashMap<>();
        doc.put("ts", "2025-02-07 03:06:16.693985-05:00");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("ts"), "table1");
        SeaTunnelRow row = deserializer.deserialize(record);

        // -05:00 means UTC+5h = 08:06:16 UTC, converted to system default timezone
        LocalDateTime expected = LocalDateTime.ofInstant(
                Instant.parse("2025-02-07T08:06:16.693985Z"), ZoneId.systemDefault());
        LocalDateTime dateTime = (LocalDateTime) row.getField(0);
        Assertions.assertNotNull(dateTime);
        Assertions.assertEquals(expected, dateTime);
    }

    @Test
    public void testDeserializeDateFallbackWithMillisPositiveOffset() {
        // Millis precision: "2025-02-07 03:06:16.693+08:00" (length 29)
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType);

        Map<String, Object> doc = new HashMap<>();
        doc.put("ts", "2025-02-07 03:06:16.693+08:00");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("ts"), "table1");
        SeaTunnelRow row = deserializer.deserialize(record);

        // +08:00 means UTC-8h = 2025-02-06 19:06:16 UTC, converted to system default timezone
        LocalDateTime expected = LocalDateTime.ofInstant(
                Instant.parse("2025-02-06T19:06:16.693Z"), ZoneId.systemDefault());
        LocalDateTime dateTime = (LocalDateTime) row.getField(0);
        Assertions.assertNotNull(dateTime);
        Assertions.assertEquals(expected, dateTime);
    }

    @Test
    public void testDeserializeDateFallbackWithMillisNegativeOffset() {
        // Millis precision with negative offset: "2025-02-07 03:06:16.693-05:30" (length 29)
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType);

        Map<String, Object> doc = new HashMap<>();
        doc.put("ts", "2025-02-07 03:06:16.693-05:30");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("ts"), "table1");
        SeaTunnelRow row = deserializer.deserialize(record);

        // -05:30 means UTC+5.5h = 08:36:16 UTC, converted to system default timezone
        LocalDateTime expected = LocalDateTime.ofInstant(
                Instant.parse("2025-02-07T08:36:16.693Z"), ZoneId.systemDefault());
        LocalDateTime dateTime = (LocalDateTime) row.getField(0);
        Assertions.assertNotNull(dateTime);
        Assertions.assertEquals(expected, dateTime);
    }

    @Test
    public void testDeserializeDateFallbackWithoutOffset() {
        // No offset: should not be affected by the offset stripping logic
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"ts"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType);

        Map<String, Object> doc = new HashMap<>();
        doc.put("ts", "2025-02-07 03:06:16.693985");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("ts"), "table1");
        SeaTunnelRow row = deserializer.deserialize(record);

        LocalDateTime dateTime = (LocalDateTime) row.getField(0);
        Assertions.assertNotNull(dateTime);
        Assertions.assertEquals(2025, dateTime.getYear());
        Assertions.assertEquals(3, dateTime.getHour());
        Assertions.assertEquals(6, dateTime.getMinute());
    }

    // ==================== Non-date fields unaffected ====================

    @Test
    public void testDeserializeNonDateFieldsUnaffected() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name", "age"},
                        new SeaTunnelDataType[] {
                            STRING_TYPE, org.apache.seatunnel.api.table.type.BasicType.INT_TYPE
                        });

        Map<String, Map<String, Object>> columnOptionsMap = new HashMap<>();

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType, columnOptionsMap);

        Map<String, Object> doc = new HashMap<>();
        doc.put("name", "alice");
        doc.put("age", "30");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("name", "age"), "table1");
        SeaTunnelRow row = deserializer.deserialize(record);

        Assertions.assertEquals("alice", row.getField(0));
        Assertions.assertEquals(30, row.getField(1));
    }

    // ==================== Unsupported format fallback failure ====================

    @Test
    public void testDeserializeDateUnsupportedFormatWithoutMapThrows() {
        // "yyyy/MM/dd" format without format map → old heuristic fails
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"date_field"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType);

        Map<String, Object> doc = new HashMap<>();
        doc.put("date_field", "2025/01/15");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("date_field"), "table1");

        // Without format info, "2025/01/15" can't be parsed by the length-based heuristic
        Assertions.assertThrows(
                ElasticsearchConnectorException.class, () -> deserializer.deserialize(record));
    }

    @Test
    public void testDeserializeDateCustomFormatFixesPreviousFailure() {
        // Same value "2025/01/15" but now WITH format map → succeeds
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"date_field"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});

        Map<String, Map<String, Object>> columnOptionsMap = new HashMap<>();
        columnOptionsMap.put("date_field", Collections.singletonMap("format", "yyyy/MM/dd"));

        DefaultSeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(rowType, columnOptionsMap);

        Map<String, Object> doc = new HashMap<>();
        doc.put("date_field", "2025/01/15");

        ElasticsearchRecord record =
                new ElasticsearchRecord(doc, Arrays.asList("date_field"), "table1");
        SeaTunnelRow row = deserializer.deserialize(record);

        LocalDateTime dateTime = (LocalDateTime) row.getField(0);
        Assertions.assertNotNull(dateTime);
        Assertions.assertEquals(2025, dateTime.getYear());
        Assertions.assertEquals(1, dateTime.getMonthValue());
        Assertions.assertEquals(15, dateTime.getDayOfMonth());
    }
}
