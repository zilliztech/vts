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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverter;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverterFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Optional;

/** PostgreSQL-specific Debezium converters for types not handled by the CDC base module. */
public final class PostgresDebeziumDeserializationConverterFactory
        implements DebeziumDeserializationConverterFactory {

    public static final PostgresDebeziumDeserializationConverterFactory INSTANCE =
            new PostgresDebeziumDeserializationConverterFactory();

    private static final int PGVECTOR_HEADER_BYTES = 4;

    private PostgresDebeziumDeserializationConverterFactory() {}

    @Override
    public Optional<DebeziumDeserializationConverter> createUserDefinedConverter(
            SeaTunnelDataType<?> type, ZoneId serverTimeZone) {
        if (type.getSqlType() == SqlType.FLOAT_VECTOR) {
            return Optional.of(
                    new DebeziumDeserializationConverter() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Object convert(
                                Object dbzObj, org.apache.kafka.connect.data.Schema schema) {
                            return convertFloatVector(dbzObj);
                        }
                    });
        }
        return Optional.empty();
    }

    private static ByteBuffer convertFloatVector(Object value) {
        if (value instanceof CharSequence) {
            return parseTextVector(value.toString());
        }
        if (value instanceof byte[]) {
            return parseVectorBytes((byte[]) value);
        }
        if (value instanceof ByteBuffer) {
            ByteBuffer source = ((ByteBuffer) value).duplicate();
            byte[] bytes = new byte[source.remaining()];
            source.get(bytes);
            return parseVectorBytes(bytes);
        }
        throw new IllegalArgumentException(
                "Unsupported PostgreSQL vector value type: " + value.getClass().getName());
    }

    private static ByteBuffer parseVectorBytes(byte[] bytes) {
        String text = new String(bytes, StandardCharsets.UTF_8).trim();
        if (text.startsWith("[") && text.endsWith("]")) {
            return parseTextVector(text);
        }

        ByteBuffer binary = ByteBuffer.wrap(bytes);
        if (binary.remaining() < PGVECTOR_HEADER_BYTES) {
            throw new IllegalArgumentException("Invalid PostgreSQL vector binary value");
        }
        int dimension = Short.toUnsignedInt(binary.getShort());
        int reserved = Short.toUnsignedInt(binary.getShort());
        int expectedBytes = PGVECTOR_HEADER_BYTES + dimension * Float.BYTES;
        if (dimension == 0 || reserved != 0 || bytes.length != expectedBytes) {
            throw new IllegalArgumentException("Invalid PostgreSQL vector binary value");
        }

        Float[] values = new Float[dimension];
        for (int i = 0; i < dimension; i++) {
            values[i] = binary.getFloat();
        }
        return BufferUtils.toByteBuffer(values);
    }

    private static ByteBuffer parseTextVector(String value) {
        String text = value.trim();
        if (!text.startsWith("[") || !text.endsWith("]")) {
            throw new IllegalArgumentException("Invalid PostgreSQL vector text value");
        }
        String body = text.substring(1, text.length() - 1).trim();
        if (body.isEmpty()) {
            throw new IllegalArgumentException("PostgreSQL vector must not be empty");
        }

        String[] components = body.split(",");
        Float[] values = new Float[components.length];
        for (int i = 0; i < components.length; i++) {
            values[i] = Float.parseFloat(components[i].trim());
        }
        return BufferUtils.toByteBuffer(values);
    }
}
