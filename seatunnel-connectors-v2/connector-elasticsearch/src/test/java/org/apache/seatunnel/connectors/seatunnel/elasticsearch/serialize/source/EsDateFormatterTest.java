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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.TimeZone;

public class EsDateFormatterTest {

    // ==================== resolveFormats tests ====================

    @Test
    public void testResolveFormatsNull() {
        List<EsDateFormatter.FormatEntry> entries = EsDateFormatter.resolveFormats(null);
        Assertions.assertTrue(entries.isEmpty());
    }

    @Test
    public void testResolveFormatsEmpty() {
        List<EsDateFormatter.FormatEntry> entries = EsDateFormatter.resolveFormats("");
        Assertions.assertTrue(entries.isEmpty());
    }

    @Test
    public void testResolveFormatsEpochMillis() {
        List<EsDateFormatter.FormatEntry> entries = EsDateFormatter.resolveFormats("epoch_millis");
        Assertions.assertEquals(1, entries.size());
        Assertions.assertEquals(EsDateFormatter.FormatType.EPOCH_MILLIS, entries.get(0).type);
    }

    @Test
    public void testResolveFormatsEpochSecond() {
        List<EsDateFormatter.FormatEntry> entries = EsDateFormatter.resolveFormats("epoch_second");
        Assertions.assertEquals(1, entries.size());
        Assertions.assertEquals(EsDateFormatter.FormatType.EPOCH_SECOND, entries.get(0).type);
    }

    @Test
    public void testResolveFormatsBuiltInName() {
        List<EsDateFormatter.FormatEntry> entries =
                EsDateFormatter.resolveFormats("strict_date_optional_time");
        Assertions.assertEquals(1, entries.size());
        Assertions.assertEquals(EsDateFormatter.FormatType.PATTERN, entries.get(0).type);
        Assertions.assertNotNull(entries.get(0).formatter);
    }

    @Test
    public void testResolveFormatsCustomPattern() {
        List<EsDateFormatter.FormatEntry> entries =
                EsDateFormatter.resolveFormats("yyyy/MM/dd");
        Assertions.assertEquals(1, entries.size());
        Assertions.assertEquals(EsDateFormatter.FormatType.PATTERN, entries.get(0).type);
    }

    @Test
    public void testResolveFormatsMultiFormat() {
        List<EsDateFormatter.FormatEntry> entries =
                EsDateFormatter.resolveFormats("strict_date_optional_time||epoch_millis");
        Assertions.assertEquals(2, entries.size());
        Assertions.assertEquals(EsDateFormatter.FormatType.PATTERN, entries.get(0).type);
        Assertions.assertEquals(EsDateFormatter.FormatType.EPOCH_MILLIS, entries.get(1).type);
    }

    @Test
    public void testResolveFormatsInvalidPatternThrows() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> EsDateFormatter.resolveFormats("epoch_millis||[[[invalid"));
    }

    // ==================== parseDate tests ====================

    @Test
    public void testParseDateEpochMillis() {
        long millis = 1706313600000L;
        LocalDateTime expected =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
        LocalDateTime result =
                EsDateFormatter.parseDate(String.valueOf(millis), "epoch_millis");
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testParseDateEpochSecond() {
        long seconds = 1706313600L;
        LocalDateTime expected =
                LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds), ZoneId.systemDefault());
        LocalDateTime result =
                EsDateFormatter.parseDate(String.valueOf(seconds), "epoch_second");
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testParseDateCustomFormatSlash() {
        LocalDateTime result = EsDateFormatter.parseDate("2025/01/15", "yyyy/MM/dd");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(1, result.getMonthValue());
        Assertions.assertEquals(15, result.getDayOfMonth());
        Assertions.assertEquals(0, result.getHour());
    }

    @Test
    public void testParseDateCustomFormatDdMmYyyy() {
        LocalDateTime result = EsDateFormatter.parseDate("15-01-2025", "dd-MM-yyyy");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(1, result.getMonthValue());
        Assertions.assertEquals(15, result.getDayOfMonth());
    }

    @Test
    public void testParseDateStrictDateOptionalTimeWithDateOnly() {
        LocalDateTime result =
                EsDateFormatter.parseDate("2025-01-15", "strict_date_optional_time");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(1, result.getMonthValue());
        Assertions.assertEquals(15, result.getDayOfMonth());
        Assertions.assertEquals(0, result.getHour());
    }

    @Test
    public void testParseDateStrictDateOptionalTimeWithFullDateTime() {
        LocalDateTime result =
                EsDateFormatter.parseDate(
                        "2025-01-15T10:30:00.000Z", "strict_date_optional_time");
        Assertions.assertNotNull(result);
        LocalDateTime expected = LocalDateTime.ofInstant(
                Instant.parse("2025-01-15T10:30:00.000Z"), ZoneId.systemDefault());
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testParseDateBuiltInDate() {
        LocalDateTime result = EsDateFormatter.parseDate("2025-01-15", "date");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(1, result.getMonthValue());
        Assertions.assertEquals(15, result.getDayOfMonth());
    }

    @Test
    public void testParseDateBasicDate() {
        LocalDateTime result = EsDateFormatter.parseDate("20250115", "basic_date");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(1, result.getMonthValue());
        Assertions.assertEquals(15, result.getDayOfMonth());
    }

    @Test
    public void testParseDateMultiFormatFallback() {
        // "strict_date_optional_time||epoch_millis"
        // Value is epoch millis, first format should fail, second should succeed
        long millis = 1706313600000L;
        LocalDateTime expected =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
        LocalDateTime result =
                EsDateFormatter.parseDate(
                        String.valueOf(millis), "strict_date_optional_time||epoch_millis");
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testParseDateMultiFormatFirstMatch() {
        // "strict_date_optional_time||epoch_millis"
        // Value is ISO date string, first format should succeed
        LocalDateTime result =
                EsDateFormatter.parseDate(
                        "2025-01-15", "strict_date_optional_time||epoch_millis");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(1, result.getMonthValue());
        Assertions.assertEquals(15, result.getDayOfMonth());
    }

    @Test
    public void testParseDateNoMatchReturnsNull() {
        LocalDateTime result = EsDateFormatter.parseDate("not-a-date", "yyyy-MM-dd");
        Assertions.assertNull(result);
    }

    @Test
    public void testParseDateNullFormatReturnsNull() {
        LocalDateTime result = EsDateFormatter.parseDate("2025-01-15", null);
        Assertions.assertNull(result);
    }

    @Test
    public void testParseDateYearMonth() {
        LocalDateTime result = EsDateFormatter.parseDate("2025-06", "year_month");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(6, result.getMonthValue());
        Assertions.assertEquals(1, result.getDayOfMonth());
    }

    @Test
    public void testParseDateYear() {
        LocalDateTime result = EsDateFormatter.parseDate("2025", "year");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(1, result.getMonthValue());
        Assertions.assertEquals(1, result.getDayOfMonth());
    }

    @Test
    public void testParseDateDateTimeNoMillis() {
        LocalDateTime result =
                EsDateFormatter.parseDate("2025-01-15T10:30:00", "date_time_no_millis");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(10, result.getHour());
        Assertions.assertEquals(30, result.getMinute());
        Assertions.assertEquals(0, result.getSecond());
    }

    @Test
    public void testParseDateDateHourMinuteSecond() {
        LocalDateTime result =
                EsDateFormatter.parseDate(
                        "2025-01-15T10:30:45", "date_hour_minute_second");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(10, result.getHour());
        Assertions.assertEquals(30, result.getMinute());
        Assertions.assertEquals(45, result.getSecond());
    }

    // ==================== Bug fix tests ====================

    @Test
    public void testParseDateBasicDateTimeWithZ() {
        LocalDateTime result =
                EsDateFormatter.parseDate("20250115T103045.123Z", "basic_date_time");
        Assertions.assertNotNull(result);
        LocalDateTime expected = LocalDateTime.ofInstant(
                Instant.parse("2025-01-15T10:30:45.123Z"), ZoneId.systemDefault());
        Assertions.assertEquals(expected.getYear(), result.getYear());
        Assertions.assertEquals(expected.getMonthValue(), result.getMonthValue());
        Assertions.assertEquals(expected.getDayOfMonth(), result.getDayOfMonth());
        Assertions.assertEquals(expected.getHour(), result.getHour());
        Assertions.assertEquals(expected.getMinute(), result.getMinute());
        Assertions.assertEquals(expected.getSecond(), result.getSecond());
    }

    @Test
    public void testParseDateBasicDateTimeWithoutZ() {
        LocalDateTime result =
                EsDateFormatter.parseDate("20250115T103045.123", "basic_date_time");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2025, result.getYear());
        Assertions.assertEquals(10, result.getHour());
    }

    @Test
    public void testEpochAndStringFormatConsistency() {
        // 2024-01-27T00:00:00.000Z = epoch 1706313600000
        LocalDateTime fromEpoch =
                EsDateFormatter.parseDate("1706313600000", "epoch_millis");
        LocalDateTime fromString =
                EsDateFormatter.parseDate(
                        "2024-01-27T00:00:00.000Z", "strict_date_optional_time");
        // Both represent the same instant, should produce the same LocalDateTime in systemDefault
        Assertions.assertEquals(fromString, fromEpoch);
    }

    @Test
    public void testEpochSecondConsistency() {
        // 2024-01-27T00:00:00Z = epoch_second 1706313600
        LocalDateTime result =
                EsDateFormatter.parseDate("1706313600", "epoch_second");
        Assertions.assertNotNull(result);
        LocalDateTime expected = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(1706313600L), ZoneId.systemDefault());
        Assertions.assertEquals(expected, result);
    }

    @Test
    public void testStringFormatWithNonUtcOffsetConvertsToSystemDefault() {
        // 2024-01-27T08:00:00+08:00 is the same instant as 2024-01-27T00:00:00Z
        LocalDateTime fromPlus8 =
                EsDateFormatter.parseDate(
                        "2024-01-27T08:00:00.000+08:00", "strict_date_optional_time");
        LocalDateTime fromZ =
                EsDateFormatter.parseDate(
                        "2024-01-27T00:00:00.000Z", "strict_date_optional_time");
        // Both represent the same instant, should produce the same LocalDateTime
        Assertions.assertEquals(fromZ, fromPlus8);
    }

    @Test
    public void testAllFormatsConsistentForSameInstant() {
        // 2024-01-27T00:00:00Z = epoch 1706313600000 = 2024-01-27T08:00:00+08:00
        LocalDateTime fromEpoch =
                EsDateFormatter.parseDate("1706313600000", "epoch_millis");
        LocalDateTime fromUtcString =
                EsDateFormatter.parseDate(
                        "2024-01-27T00:00:00.000Z", "strict_date_optional_time");
        LocalDateTime fromPlus8String =
                EsDateFormatter.parseDate(
                        "2024-01-27T08:00:00.000+08:00", "strict_date_optional_time");
        // All three represent the same instant
        Assertions.assertEquals(fromEpoch, fromUtcString);
        Assertions.assertEquals(fromEpoch, fromPlus8String);
    }

    @Test
    public void testDateTimeNoMillisWithOffsetConvertsToSystemDefault() {
        LocalDateTime result =
                EsDateFormatter.parseDate(
                        "2025-01-15T18:30:00+08:00", "date_time_no_millis");
        Assertions.assertNotNull(result);
        // 18:30 +08:00 = 10:30 UTC → convert to systemDefault
        LocalDateTime expected = LocalDateTime.ofInstant(
                Instant.parse("2025-01-15T10:30:00Z"), ZoneId.systemDefault());
        Assertions.assertEquals(expected.getHour(), result.getHour());
        Assertions.assertEquals(expected.getMinute(), result.getMinute());
    }

    // ==================== Timezone adaptation tests ====================

    /**
     * Verifies that EsDateFormatter correctly converts epoch_millis to systemDefault timezone.
     */
    @Test
    public void testEpochMillisConvertsToSystemDefault() {
        String[] zones = {"UTC", "Asia/Shanghai", "US/Eastern"};
        TimeZone original = TimeZone.getDefault();
        try {
            for (String zone : zones) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                LocalDateTime result =
                        EsDateFormatter.parseDate("1706313600000", "epoch_millis");
                LocalDateTime expected = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(1706313600000L), ZoneId.systemDefault());
                Assertions.assertEquals(expected, result, "mismatch in " + zone);
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    public void testEpochSecondConvertsToSystemDefault() {
        String[] zones = {"UTC", "Asia/Shanghai", "US/Eastern"};
        TimeZone original = TimeZone.getDefault();
        try {
            for (String zone : zones) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                LocalDateTime result =
                        EsDateFormatter.parseDate("1706313600", "epoch_second");
                LocalDateTime expected = LocalDateTime.ofInstant(
                        Instant.ofEpochSecond(1706313600L), ZoneId.systemDefault());
                Assertions.assertEquals(expected, result, "mismatch in " + zone);
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    public void testStringFormatWithZConvertsToSystemDefault() {
        String[] zones = {"UTC", "Asia/Shanghai", "US/Eastern"};
        TimeZone original = TimeZone.getDefault();
        try {
            for (String zone : zones) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                LocalDateTime result =
                        EsDateFormatter.parseDate(
                                "2024-01-27T00:00:00.000Z", "strict_date_optional_time");
                LocalDateTime expected = LocalDateTime.ofInstant(
                        Instant.parse("2024-01-27T00:00:00.000Z"), ZoneId.systemDefault());
                Assertions.assertEquals(expected, result, "mismatch in " + zone);
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    public void testStringFormatWithOffsetConvertsToSystemDefault() {
        // 2024-01-27T08:00:00+08:00 = 2024-01-27T00:00:00Z
        String[] zones = {"UTC", "Asia/Shanghai", "US/Eastern"};
        TimeZone original = TimeZone.getDefault();
        try {
            for (String zone : zones) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                LocalDateTime result =
                        EsDateFormatter.parseDate(
                                "2024-01-27T08:00:00.000+08:00", "strict_date_optional_time");
                LocalDateTime expected = LocalDateTime.ofInstant(
                        Instant.parse("2024-01-27T00:00:00.000Z"), ZoneId.systemDefault());
                Assertions.assertEquals(expected, result, "mismatch in " + zone);
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }

    @Test
    public void testAllFormatsConsistentAcrossTimezones() {
        // Same instant: epoch 1706313600000 = 2024-01-27T00:00:00Z = 2024-01-27T08:00:00+08:00
        String[] zones = {"UTC", "Asia/Shanghai", "US/Eastern"};
        TimeZone original = TimeZone.getDefault();
        try {
            for (String zone : zones) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                LocalDateTime fromEpoch =
                        EsDateFormatter.parseDate("1706313600000", "epoch_millis");
                LocalDateTime fromUtcString =
                        EsDateFormatter.parseDate(
                                "2024-01-27T00:00:00.000Z", "strict_date_optional_time");
                LocalDateTime fromPlus8String =
                        EsDateFormatter.parseDate(
                                "2024-01-27T08:00:00.000+08:00", "strict_date_optional_time");
                Assertions.assertEquals(
                        fromEpoch, fromUtcString,
                        "epoch vs UTC string mismatch in " + zone);
                Assertions.assertEquals(
                        fromEpoch, fromPlus8String,
                        "epoch vs +08:00 string mismatch in " + zone);
            }
        } finally {
            TimeZone.setDefault(original);
        }
    }
}
