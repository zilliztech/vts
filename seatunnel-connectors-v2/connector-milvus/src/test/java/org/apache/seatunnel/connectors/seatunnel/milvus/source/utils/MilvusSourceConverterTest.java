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

package org.apache.seatunnel.connectors.seatunnel.milvus.source.utils;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.milvus.response.QueryResultsWrapper;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

class MilvusSourceConverterTest {

    private TableSchema schemaWithFloatFields() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.builder()
                                .name("id")
                                .dataType(BasicType.LONG_TYPE)
                                .build(),
                        PhysicalColumn.builder()
                                .name("float_arr")
                                .dataType(ArrayType.FLOAT_ARRAY_TYPE)
                                .build(),
                        PhysicalColumn.builder()
                                .name("double_arr")
                                .dataType(ArrayType.DOUBLE_ARRAY_TYPE)
                                .build(),
                        PhysicalColumn.builder()
                                .name("embedding")
                                .dataType(VectorType.VECTOR_FLOAT_TYPE)
                                .scale(3)
                                .build());
        return TableSchema.builder().columns(columns).build();
    }

    private QueryResultsWrapper.RowRecord recordWithFloatFields() {
        QueryResultsWrapper.RowRecord record = new QueryResultsWrapper.RowRecord();
        record.put("id", 1L);
        record.put("float_arr", Arrays.asList(0.1f, -2.5f, Float.MAX_VALUE));
        record.put("double_arr", Arrays.asList(0.1d, -2.5d, Double.MAX_VALUE));
        record.put("embedding", Arrays.asList(0.1f, 0.2f, 0.3f));
        return record;
    }

    @Test
    void testFloatArrayElementsPassThroughBitIdentical() {
        TableSchema tableSchema = schemaWithFloatFields();
        MilvusSourceConverter converter = new MilvusSourceConverter(tableSchema);
        List<Float> input = Arrays.asList(0.1f, -2.5f, Float.MAX_VALUE);

        SeaTunnelRow row =
                converter.convertToSeaTunnelRow(
                        recordWithFloatFields(), tableSchema, "coll", "part");

        Float[] output = (Float[]) row.getField(1);
        Assertions.assertEquals(input.size(), output.length);
        for (int i = 0; i < input.size(); i++) {
            Assertions.assertEquals(
                    Float.floatToRawIntBits(input.get(i)), Float.floatToRawIntBits(output[i]));
        }
    }

    @Test
    void testDoubleArrayElementsPassThroughBitIdentical() {
        TableSchema tableSchema = schemaWithFloatFields();
        MilvusSourceConverter converter = new MilvusSourceConverter(tableSchema);
        List<Double> input = Arrays.asList(0.1d, -2.5d, Double.MAX_VALUE);

        SeaTunnelRow row =
                converter.convertToSeaTunnelRow(
                        recordWithFloatFields(), tableSchema, "coll", "part");

        Double[] output = (Double[]) row.getField(2);
        Assertions.assertEquals(input.size(), output.length);
        for (int i = 0; i < input.size(); i++) {
            Assertions.assertEquals(
                    Double.doubleToRawLongBits(input.get(i)),
                    Double.doubleToRawLongBits(output[i]));
        }
    }

    @Test
    void testFloatVectorElementsPassThroughBitIdentical() {
        TableSchema tableSchema = schemaWithFloatFields();
        MilvusSourceConverter converter = new MilvusSourceConverter(tableSchema);
        List<Float> input = Arrays.asList(0.1f, 0.2f, 0.3f);

        SeaTunnelRow row =
                converter.convertToSeaTunnelRow(
                        recordWithFloatFields(), tableSchema, "coll", "part");

        ByteBuffer buffer = (ByteBuffer) row.getField(3);
        Assertions.assertEquals(input.size() * Float.BYTES, buffer.remaining());
        for (int i = 0; i < input.size(); i++) {
            Assertions.assertEquals(
                    Float.floatToRawIntBits(input.get(i)),
                    Float.floatToRawIntBits(buffer.getFloat(i * Float.BYTES)));
        }
    }

    @Test
    void testSmallintFieldStaysShort() {
        // Regression test for the scalar SMALLINT branch missing a break and
        // falling through to INT, which overwrote the Short with an Integer.
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                Arrays.asList(
                                        PhysicalColumn.builder()
                                                .name("id")
                                                .dataType(BasicType.LONG_TYPE)
                                                .build(),
                                        PhysicalColumn.builder()
                                                .name("age")
                                                .dataType(BasicType.SHORT_TYPE)
                                                .build()))
                        .build();
        MilvusSourceConverter converter = new MilvusSourceConverter(tableSchema);
        QueryResultsWrapper.RowRecord record = new QueryResultsWrapper.RowRecord();
        record.put("id", 1L);
        record.put("age", (short) 7);

        SeaTunnelRow row = converter.convertToSeaTunnelRow(record, tableSchema, "coll", "part");

        Assertions.assertEquals(Short.valueOf((short) 7), row.getField(1));
    }

    @Test
    void testStringElementInFloatArrayFailsFast() {
        TableSchema tableSchema = schemaWithFloatFields();
        MilvusSourceConverter converter = new MilvusSourceConverter(tableSchema);
        QueryResultsWrapper.RowRecord record = new QueryResultsWrapper.RowRecord();
        record.put("id", 1L);
        // RowRecord.put refuses to overwrite an existing key, so the string
        // elements must be set on a fresh record rather than via re-put.
        record.put("float_arr", Arrays.asList("0.1", "0.2"));
        record.put("double_arr", Arrays.asList(0.1d, -2.5d, Double.MAX_VALUE));
        record.put("embedding", Arrays.asList(0.1f, 0.2f, 0.3f));

        Assertions.assertThrows(
                ClassCastException.class,
                () -> converter.convertToSeaTunnelRow(record, tableSchema, "coll", "part"));
    }
}
