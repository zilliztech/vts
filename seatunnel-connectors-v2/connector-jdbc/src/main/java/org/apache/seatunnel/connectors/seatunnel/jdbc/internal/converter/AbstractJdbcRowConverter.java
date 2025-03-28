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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter;

import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcFieldTypeUtils;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** Base class for all converters that convert between JDBC object and SeaTunnel internal object. */
@Slf4j
public abstract class AbstractJdbcRowConverter implements JdbcRowConverter {

    protected static final String[] TYPE_ARRAY_STRING = new String[0];
    protected static final Boolean[] TYPE_ARRAY_BOOLEAN = new Boolean[0];
    protected static final Byte[] TYPE_ARRAY_BYTE = new Byte[0];
    protected static final Short[] TYPE_ARRAY_SHORT = new Short[0];
    protected static final Integer[] TYPE_ARRAY_INTEGER = new Integer[0];
    protected static final Long[] TYPE_ARRAY_LONG = new Long[0];
    protected static final Float[] TYPE_ARRAY_FLOAT = new Float[0];
    protected static final Double[] TYPE_ARRAY_DOUBLE = new Double[0];
    protected static final BigDecimal[] TYPE_ARRAY_BIG_DECIMAL = new BigDecimal[0];
    protected static final LocalDate[] TYPE_ARRAY_LOCAL_DATE = new LocalDate[0];
    protected static final LocalDateTime[] TYPE_ARRAY_LOCAL_DATETIME = new LocalDateTime[0];

    public abstract String converterName();

    public AbstractJdbcRowConverter() {}

    @Override
    public SeaTunnelRow toInternal(ResultSet rs, TableSchema tableSchema) throws SQLException {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[typeInfo.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            String fieldName = typeInfo.getFieldName(fieldIndex);
            int resultSetIndex = fieldIndex + 1;
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getString(rs, resultSetIndex);
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
                    fields[fieldIndex] = readTime(rs, resultSetIndex);
                    break;
                case TIMESTAMP:
                    Timestamp sqlTimestamp = JdbcFieldTypeUtils.getTimestamp(rs, resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTimestamp)
                                    .map(e -> e.toLocalDateTime())
                                    .orElse(null);
                    break;
                case BYTES:
                    fields[fieldIndex] = JdbcFieldTypeUtils.getBytes(rs, resultSetIndex);
                    break;
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case ARRAY:
                    fields[fieldIndex] =
                            convertToArray(rs, resultSetIndex, seaTunnelDataType, fieldName);
                    break;
                case MAP:
                case ROW:
                default:
                    throw CommonError.unsupportedDataType(
                            converterName(), seaTunnelDataType.getSqlType().toString(), fieldName);
            }
        }
        return new SeaTunnelRow(fields);
    }

    protected LocalTime readTime(ResultSet rs, int resultSetIndex) throws SQLException {
        Time sqlTime = JdbcFieldTypeUtils.getTime(rs, resultSetIndex);
        return Optional.ofNullable(sqlTime).map(e -> e.toLocalTime()).orElse(null);
    }

    public Object[] convertToArray(
            ResultSet rs,
            int resultSetIndex,
            SeaTunnelDataType<?> seaTunnelDataType,
            String fieldName)
            throws SQLException {
        Array array = rs.getArray(resultSetIndex);
        if (array != null) {
            Object[] elementArr = (Object[]) array.getArray();
            List<Object> origArray = Arrays.asList(elementArr);
            SeaTunnelDataType<?> elementType =
                    ((ArrayType<?, ?>) seaTunnelDataType).getElementType();
            switch (elementType.getSqlType()) {
                case STRING:
                    return origArray.toArray(TYPE_ARRAY_STRING);
                case BOOLEAN:
                    return origArray.toArray(TYPE_ARRAY_BOOLEAN);
                case TINYINT:
                    return origArray.toArray(TYPE_ARRAY_BYTE);
                case SMALLINT:
                    return origArray.toArray(TYPE_ARRAY_SHORT);
                case INT:
                    return origArray.toArray(TYPE_ARRAY_INTEGER);
                case BIGINT:
                    return origArray.toArray(TYPE_ARRAY_LONG);
                case FLOAT:
                    return origArray.toArray(TYPE_ARRAY_FLOAT);
                case DOUBLE:
                    return origArray.toArray(TYPE_ARRAY_DOUBLE);
                case DECIMAL:
                    return origArray.toArray(TYPE_ARRAY_BIG_DECIMAL);
                default:
                    String type = String.format("Array[%s]", elementType.getSqlType());
                    throw CommonError.unsupportedDataType(converterName(), type, fieldName);
            }
        } else {
            return null;
        }
    }

    @Override
    public PreparedStatement toExternal(
            TableSchema tableSchema, SeaTunnelRow row, PreparedStatement statement)
            throws SQLException {
        return toExternal(tableSchema, null, row, statement);
    }

    @Override
    public PreparedStatement toExternal(
            TableSchema tableSchema,
            @Nullable TableSchema databaseTableSchema,
            SeaTunnelRow row,
            PreparedStatement statement)
            throws SQLException {
        SeaTunnelRowType rowType = tableSchema.toPhysicalRowDataType();
        for (int fieldIndex = 0; fieldIndex < rowType.getTotalFields(); fieldIndex++) {
            try {
                SeaTunnelDataType<?> seaTunnelDataType = rowType.getFieldType(fieldIndex);
                String fieldName = rowType.getFieldName(fieldIndex);
                int statementIndex = fieldIndex + 1;
                Object fieldValue = row.getField(fieldIndex);
                if (fieldValue == null) {
                    statement.setObject(statementIndex, null);
                    continue;
                }
                String sourceType = null;
                if (databaseTableSchema != null && databaseTableSchema.contains(fieldName)) {
                    sourceType = databaseTableSchema.getColumn(fieldName).getSourceType();
                }
                setValueToStatementByDataType(
                        row.getField(fieldIndex),
                        statement,
                        seaTunnelDataType,
                        statementIndex,
                        sourceType);
            } catch (Exception e) {
                throw new JdbcConnectorException(
                        JdbcConnectorErrorCode.DATA_TYPE_CAST_FAILED,
                        "error field:" + rowType.getFieldNames()[fieldIndex],
                        e);
            }
        }
        return statement;
    }

    protected void setValueToStatementByDataType(
            Object value,
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        switch (seaTunnelDataType.getSqlType()) {
            case STRING:
                statement.setString(statementIndex, (String) value);
                break;
            case BOOLEAN:
                statement.setBoolean(statementIndex, (Boolean) value);
                break;
            case TINYINT:
                statement.setByte(statementIndex, (Byte) value);
                break;
            case SMALLINT:
                statement.setShort(statementIndex, (Short) value);
                break;
            case INT:
                statement.setInt(statementIndex, (Integer) value);
                break;
            case BIGINT:
                statement.setLong(statementIndex, (Long) value);
                break;
            case FLOAT:
                statement.setFloat(statementIndex, (Float) value);
                break;
            case DOUBLE:
                statement.setDouble(statementIndex, (Double) value);
                break;
            case DECIMAL:
                statement.setBigDecimal(statementIndex, (BigDecimal) value);
                break;
            case DATE:
                LocalDate localDate = (LocalDate) value;
                statement.setDate(statementIndex, Date.valueOf(localDate));
                break;
            case TIME:
                writeTime(statement, statementIndex, (LocalTime) value);
                break;
            case TIMESTAMP:
                LocalDateTime localDateTime = (LocalDateTime) value;
                statement.setTimestamp(statementIndex, Timestamp.valueOf(localDateTime));
                break;
            case BYTES:
                statement.setBytes(statementIndex, (byte[]) value);
                break;
            case NULL:
                statement.setNull(statementIndex, java.sql.Types.NULL);
                break;
            case ARRAY:
                SeaTunnelDataType elementType = ((ArrayType) seaTunnelDataType).getElementType();
                Object[] array = (Object[]) value;
                if (array == null) {
                    statement.setNull(statementIndex, java.sql.Types.ARRAY);
                    break;
                }
                if (SqlType.TINYINT.equals(elementType.getSqlType())) {
                    Short[] shortArray = new Short[array.length];
                    for (int i = 0; i < array.length; i++) {
                        shortArray[i] = Short.valueOf(array[i].toString());
                    }
                    statement.setObject(statementIndex, shortArray);
                } else {
                    statement.setObject(statementIndex, array);
                }
                break;
            case MAP:
            case ROW:
            default:
                throw new JdbcConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "Unexpected value: " + seaTunnelDataType);
        }
    }

    protected void writeTime(PreparedStatement statement, int index, LocalTime time)
            throws SQLException {
        statement.setTime(index, java.sql.Time.valueOf(time));
    }
}
