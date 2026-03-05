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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utility class that resolves Elasticsearch date format strings to Java DateTimeFormatters. */
public class EsDateFormatter {

    private static final Map<String, DateTimeFormatter> BUILT_IN_FORMATS = new HashMap<>();

    static {
        // ISO-8601 variants
        DateTimeFormatter strictDateOptionalTime =
                new DateTimeFormatterBuilder()
                        .append(DateTimeFormatter.ISO_LOCAL_DATE)
                        .optionalStart()
                        .appendLiteral('T')
                        .append(DateTimeFormatter.ISO_LOCAL_TIME)
                        .optionalStart()
                        .appendOffset("+HH:MM", "Z")
                        .optionalEnd()
                        .optionalEnd()
                        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                        .toFormatter();

        BUILT_IN_FORMATS.put("strict_date_optional_time", strictDateOptionalTime);
        BUILT_IN_FORMATS.put("strict_date_optional_time_nanos", strictDateOptionalTime);
        BUILT_IN_FORMATS.put("date_optional_time", strictDateOptionalTime);

        // date: yyyy-MM-dd
        DateTimeFormatter dateOnly =
                new DateTimeFormatterBuilder()
                        .append(DateTimeFormatter.ISO_LOCAL_DATE)
                        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                        .toFormatter();
        BUILT_IN_FORMATS.put("date", dateOnly);
        BUILT_IN_FORMATS.put("strict_date", dateOnly);

        // date_time: yyyy-MM-dd'T'HH:mm:ss.SSSXXX
        DateTimeFormatter dateTime =
                new DateTimeFormatterBuilder()
                        .append(DateTimeFormatter.ISO_LOCAL_DATE)
                        .appendLiteral('T')
                        .append(DateTimeFormatter.ISO_LOCAL_TIME)
                        .optionalStart()
                        .appendOffset("+HH:MM", "Z")
                        .optionalEnd()
                        .toFormatter();
        BUILT_IN_FORMATS.put("date_time", dateTime);
        BUILT_IN_FORMATS.put("strict_date_time", dateTime);

        // date_time_no_millis: yyyy-MM-dd'T'HH:mm:ssXXX
        DateTimeFormatter dateTimeNoMillis =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[XXX]");
        BUILT_IN_FORMATS.put("date_time_no_millis", dateTimeNoMillis);
        BUILT_IN_FORMATS.put("strict_date_time_no_millis", dateTimeNoMillis);

        // basic_date: yyyyMMdd
        DateTimeFormatter basicDate =
                new DateTimeFormatterBuilder()
                        .appendPattern("yyyyMMdd")
                        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                        .toFormatter();
        BUILT_IN_FORMATS.put("basic_date", basicDate);
        BUILT_IN_FORMATS.put("strict_basic_date", basicDate);

        // basic_date_time: yyyyMMdd'T'HHmmss.SSSZ
        DateTimeFormatter basicDateTime =
                new DateTimeFormatterBuilder()
                        .appendPattern("yyyyMMdd'T'HHmmss.SSS")
                        .optionalStart()
                        .appendOffset("+HHMM", "Z")
                        .optionalEnd()
                        .toFormatter();
        BUILT_IN_FORMATS.put("basic_date_time", basicDateTime);
        BUILT_IN_FORMATS.put("strict_basic_date_time", basicDateTime);

        // date_hour_minute_second: yyyy-MM-dd'T'HH:mm:ss
        DateTimeFormatter dateHourMinuteSecond =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        BUILT_IN_FORMATS.put("date_hour_minute_second", dateHourMinuteSecond);
        BUILT_IN_FORMATS.put("strict_date_hour_minute_second", dateHourMinuteSecond);

        // hour_minute_second: HH:mm:ss
        BUILT_IN_FORMATS.put("hour_minute_second", DateTimeFormatter.ofPattern("HH:mm:ss"));
        BUILT_IN_FORMATS.put(
                "strict_hour_minute_second", DateTimeFormatter.ofPattern("HH:mm:ss"));

        // year: yyyy
        DateTimeFormatter yearOnly =
                new DateTimeFormatterBuilder()
                        .appendPattern("yyyy")
                        .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
                        .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
                        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                        .toFormatter();
        BUILT_IN_FORMATS.put("year", yearOnly);
        BUILT_IN_FORMATS.put("strict_year", yearOnly);

        // year_month: yyyy-MM
        DateTimeFormatter yearMonth =
                new DateTimeFormatterBuilder()
                        .appendPattern("yyyy-MM")
                        .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
                        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                        .toFormatter();
        BUILT_IN_FORMATS.put("year_month", yearMonth);
        BUILT_IN_FORMATS.put("strict_year_month", yearMonth);

        // year_month_day: yyyy-MM-dd
        BUILT_IN_FORMATS.put("year_month_day", dateOnly);
        BUILT_IN_FORMATS.put("strict_year_month_day", dateOnly);
    }

    /**
     * Parses an Elasticsearch format string (potentially containing || separators) and returns a
     * list of resolved format entries.
     */
    public static List<FormatEntry> resolveFormats(String esFormatString) {
        List<FormatEntry> entries = new ArrayList<>();
        if (esFormatString == null || esFormatString.isEmpty()) {
            return entries;
        }
        for (String part : esFormatString.split("\\|\\|")) {
            String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if ("epoch_millis".equals(trimmed)) {
                entries.add(new FormatEntry(FormatType.EPOCH_MILLIS, null));
            } else if ("epoch_second".equals(trimmed)) {
                entries.add(new FormatEntry(FormatType.EPOCH_SECOND, null));
            } else if (BUILT_IN_FORMATS.containsKey(trimmed)) {
                entries.add(new FormatEntry(FormatType.PATTERN, BUILT_IN_FORMATS.get(trimmed)));
            } else {
                // Custom pattern - pass through to DateTimeFormatter
                try {
                    DateTimeFormatter custom =
                            new DateTimeFormatterBuilder()
                                    .appendPattern(trimmed)
                                    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                                    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                                    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                                    .toFormatter();
                    entries.add(new FormatEntry(FormatType.PATTERN, custom));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                            "Invalid ES date format pattern: '" + trimmed + "'", e);
                }
            }
        }
        return entries;
    }

    /**
     * Attempts to parse a date string using the given ES format string. Returns null if parsing
     * fails with all formats.
     */
    public static LocalDateTime parseDate(String value, String esFormatString) {
        List<FormatEntry> entries = resolveFormats(esFormatString);
        for (FormatEntry entry : entries) {
            try {
                switch (entry.type) {
                    case EPOCH_MILLIS:
                        long millis = Long.parseLong(value);
                        return LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(millis), ZoneId.systemDefault());
                    case EPOCH_SECOND:
                        long seconds = Long.parseLong(value);
                        return LocalDateTime.ofInstant(
                                Instant.ofEpochSecond(seconds), ZoneId.systemDefault());
                    case PATTERN:
                        TemporalAccessor parsed = entry.formatter.parse(value);
                        LocalDateTime localDateTime = LocalDateTime.from(parsed);
                        if (parsed.isSupported(ChronoField.OFFSET_SECONDS)) {
                            int offsetSeconds = parsed.get(ChronoField.OFFSET_SECONDS);
                            ZoneOffset parsedOffset =
                                    ZoneOffset.ofTotalSeconds(offsetSeconds);
                            Instant instant = localDateTime.toInstant(parsedOffset);
                            return LocalDateTime.ofInstant(
                                    instant, ZoneId.systemDefault());
                        }
                        return localDateTime;
                }
            } catch (NumberFormatException | DateTimeParseException e) {
                // Try next format
            }
        }
        return null;
    }

    public enum FormatType {
        EPOCH_MILLIS,
        EPOCH_SECOND,
        PATTERN
    }

    public static class FormatEntry {
        final FormatType type;
        final DateTimeFormatter formatter;

        FormatEntry(FormatType type, DateTimeFormatter formatter) {
            this.type = type;
            this.formatter = formatter;
        }
    }
}
