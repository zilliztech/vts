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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverter;

import org.apache.kafka.connect.data.SchemaBuilder;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresDebeziumDeserializationConverterFactoryTest {

    @Test
    void shouldIgnoreNonVectorTypes() {
        assertFalse(
                PostgresDebeziumDeserializationConverterFactory.INSTANCE
                        .createUserDefinedConverter(BasicType.STRING_TYPE, ZoneId.systemDefault())
                        .isPresent());
    }

    @Test
    void shouldConvertVectorText() throws Exception {
        assertVectorEquals(
                new Float[] {0.1F, -0.2F, 3.5F},
                converter().convert("[0.1,-0.2,3.5]", SchemaBuilder.string().build()));
    }

    @Test
    void shouldConvertVectorTextBytes() throws Exception {
        assertVectorEquals(
                new Float[] {0.1F, -0.2F, 3.5F},
                converter()
                        .convert(
                                "[0.1,-0.2,3.5]".getBytes(StandardCharsets.UTF_8),
                                SchemaBuilder.bytes().build()));
    }

    @Test
    void shouldConvertPgvectorBinary() throws Exception {
        ByteBuffer pgvector = ByteBuffer.allocate(4 + 3 * Float.BYTES);
        pgvector.putShort((short) 3);
        pgvector.putShort((short) 0);
        pgvector.putFloat(0.1F);
        pgvector.putFloat(-0.2F);
        pgvector.putFloat(3.5F);

        assertVectorEquals(
                new Float[] {0.1F, -0.2F, 3.5F},
                converter().convert(pgvector.array(), SchemaBuilder.bytes().build()));
    }

    private DebeziumDeserializationConverter converter() {
        Optional<DebeziumDeserializationConverter> converter =
                PostgresDebeziumDeserializationConverterFactory.INSTANCE.createUserDefinedConverter(
                        VectorType.VECTOR_FLOAT_TYPE, ZoneId.systemDefault());
        assertTrue(converter.isPresent());
        return converter.get();
    }

    private void assertVectorEquals(Float[] expected, Object actual) {
        assertTrue(actual instanceof ByteBuffer);
        assertArrayEquals(expected, BufferUtils.toFloatArray(((ByteBuffer) actual).duplicate()));
    }
}
