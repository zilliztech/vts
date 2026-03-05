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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql;

import org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcFieldTypeUtils;

import javax.annotation.Nullable;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final String PG_GEOMETRY = "GEOMETRY";
    private static final String PG_GEOGRAPHY = "GEOGRAPHY";

    @Override
    public String converterName() {
        return DatabaseIdentifier.POSTGRESQL;
    }

    @Override
    public SeaTunnelRow toInternal(ResultSet rs, TableSchema tableSchema) throws SQLException {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[typeInfo.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            int resultSetIndex = fieldIndex + 1;
            String metaDataColumnType =
                    rs.getMetaData().getColumnTypeName(resultSetIndex).toUpperCase(Locale.ROOT);
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    if (metaDataColumnType.equals(PG_GEOMETRY)
                            || metaDataColumnType.equals(PG_GEOGRAPHY)) {
                        fields[fieldIndex] =
                                rs.getObject(resultSetIndex) == null
                                        ? null
                                        : rs.getObject(resultSetIndex).toString();
                    } else {
                        fields[fieldIndex] = JdbcFieldTypeUtils.getString(rs, resultSetIndex);
                    }
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBoolean(rs, resultSetIndex);
                    break;
                case TINYINT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getByte(rs, resultSetIndex);
                    break;
                case SMALLINT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getShort(rs, resultSetIndex);
                    break;
                case INT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getInt(rs, resultSetIndex);
                    break;
                case BIGINT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getLong(rs, resultSetIndex);
                    break;
                case FLOAT:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getFloat(rs, resultSetIndex);
                    break;
                case DOUBLE:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getDouble(rs, resultSetIndex);
                    break;
                case DECIMAL:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBigDecimal(rs, resultSetIndex);
                    break;
                case DATE:
                    Date sqlDate = JdbcFieldTypeUtils.getDate(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlDate).map(e -> e.toLocalDate()).orElse(null);
                    break;
                case TIME:
                    Time sqlTime = JdbcFieldTypeUtils.getTime(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTime).map(e -> e.toLocalTime()).orElse(null);
                    break;
                case TIMESTAMP:
                    Timestamp sqlTimestamp = JdbcFieldTypeUtils.getTimestamp(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTimestamp)
                                    .map(e -> e.toLocalDateTime())
                                    .orElse(null);
                    break;
                case TIMESTAMP_TZ:
                    fields[fieldIndex] = getPostgresOffsetDateTime(rs, resultSetIndex);
                    break;
                case BYTES:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBytes(rs, resultSetIndex);
                    break;
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case ARRAY:
                    Array jdbcArray = rs.getArray(resultSetIndex);
                    if (jdbcArray == null) {
                        fields[fieldIndex] = null;
                        break;
                    }

                    Object arrayObject = jdbcArray.getArray();
                    if (((ArrayType) seaTunnelDataType)
                            .getTypeClass()
                            .equals(arrayObject.getClass())) {
                        fields[fieldIndex] = arrayObject;
                    } else {
                        throw new JdbcConnectorException(
                                CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                                "Unexpected value: " + seaTunnelDataType.getTypeClass());
                    }
                    break;
                case FLOAT_VECTOR:
                    String vectorString = JdbcFieldTypeUtils.getString(rs, resultSetIndex);
                    String[] vectorArrayString =
                            vectorString.replace("[", "").replace("]", "").split(",");
                    Float[] vectorArray = new Float[vectorArrayString.length];
                    for (int index = 0; index < vectorArrayString.length; index++) {
                        vectorArray[index] = Float.parseFloat(vectorArrayString[index]);
                    }
                    fields[fieldIndex] = BufferUtils.toByteBuffer(vectorArray);
                    break;
                case MAP:
                case ROW:
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType);
            }
        }
        return new SeaTunnelRow(fields);
    }

    private OffsetDateTime getPostgresOffsetDateTime(ResultSet rs, int columnIndex)
            throws SQLException {
        // Read the value once to avoid drivers returning null on subsequent reads
        final Object obj = rs.getObject(columnIndex);

        if (obj == null) {
            return null;
        }

        // Direct types
        if (obj instanceof OffsetDateTime) {
            return (OffsetDateTime) obj;
        }
        if (obj instanceof Timestamp) {
            return ((Timestamp) obj).toInstant().atOffset(ZoneOffset.UTC);
        }
        if (obj instanceof java.time.ZonedDateTime) {
            return ((java.time.ZonedDateTime) obj).toOffsetDateTime();
        }
        if (obj instanceof java.util.Date) {
            return ((java.util.Date) obj).toInstant().atOffset(ZoneOffset.UTC);
        }

        // Remaining PostgreSQL-specific or driver types: fall back to string representation
        return parseTimestampFromObjectString(obj);
    }

    @Nullable
    private OffsetDateTime parseTimestampFromObjectString(Object obj) throws SQLException {
        final String str;
        try {
            str = String.valueOf(obj);
        } catch (Throwable e) {
            log.debug(
                    "Failed to get PostgreSQL timestamp object string representation from class: {}",
                    obj.getClass().getName(),
                    e);
            return null;
        }
        return parsePostgresTimestampTz(str);
    }


    private OffsetDateTime parsePostgresTimestampTz(String str) throws SQLException {
        String normalized = normalizeIsoTimestamp(str);
        if (normalized == null) {
            return null;
        }

        try {
            return OffsetDateTime.parse(normalized);
        } catch (Exception primary) {
            log.debug("Failed to parse PostgreSQL timestamptz as ISO-8601: {}", str, primary);
            try {
                String withoutOffset =
                        normalized.replaceFirst("([+-]\\d{2}:?\\d{2}|\\s+UTC|[zZ])$", "");
                String fallback = withoutOffset.replace('T', ' ').trim();
                Timestamp ts = Timestamp.valueOf(fallback);
                return ts.toInstant().atOffset(ZoneOffset.UTC);
            } catch (Exception secondary) {
                log.debug(
                        "Failed to parse PostgreSQL timestamptz as UTC timestamp: {}",
                        str,
                        secondary);
                throw new SQLException(
                        "Failed to parse PostgreSQL timestamptz string: " + str, secondary);
            }
        }
    }


    private String normalizeIsoTimestamp(String value) {
        // PostgreSQL timestamptz format examples:
        // "2023-12-25 10:30:45.123456+08:00"
        // "2023-12-25 10:30:45+08"
        // "2023-12-25 10:30:45.123456 UTC"
        String normalized = StringUtils.trimToNull(value);
        if (normalized == null) {
            return null;
        }
        // Handle UTC timezone
        if (normalized.endsWith(" UTC")) {
            normalized = normalized.substring(0, normalized.length() - 4) + "Z";
        }
        // Normalize to ISO-8601 format examples:
        // "2024-01-01T10:15:30+08:00"
        // "2024-01-01T10:15:30Z"
        normalized = normalized.replace(' ', 'T');
        if (!normalized.isEmpty()) {
            char lastChar = normalized.charAt(normalized.length() - 1);
            if (lastChar == 'z' || lastChar == 'Z') {
                normalized = normalized.substring(0, normalized.length() - 1) + "Z";
            }
        }
        // Add colon to offsets like +HH -> +HH:00
        if (normalized.matches(".*[+-]\\d{2}$")) {
            return normalized + ":00";
        }
        if (normalized.matches(".*[+-]\\d{4}$")) {
            // Add colon to offsets like +HHMM -> +HH:MM
            return normalized.substring(0, normalized.length() - 2)
                    + ":"
                    + normalized.substring(normalized.length() - 2);
        }
        return normalized;
    }

}
