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

package org.apache.seatunnel.api.table.type;

import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class NestedArrayTypeTest {
    @Test
    void constructsAndParsesNestedArrays() {
        ArrayType<Float[][], Float[]> matrix = ArrayType.of(ArrayType.FLOAT_ARRAY_TYPE);
        Assertions.assertEquals(Float[][].class, matrix.getTypeClass());
        Assertions.assertEquals(matrix, SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                "matrix", "ARRAY<ARRAY<FLOAT>>"));
        ArrayType<Float[][][], Float[][]> nested = ArrayType.of(matrix);
        Assertions.assertEquals(Float[][][].class, nested.getTypeClass());
        Assertions.assertEquals(nested, SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                "nested", nested.toString()));
        Assertions.assertThrows(RuntimeException.class, () -> ArrayType.of(null));
    }

    @Test
    void countsNestedArrayBytesWithAndWithoutSchema() {
        Float[][] matrix = {{1.0f, null, 2.0f}, null, {}, {3.0f}};
        SeaTunnelRowType type = new SeaTunnelRowType(new String[] {"matrix"},
                new SeaTunnelDataType<?>[] {ArrayType.of(ArrayType.FLOAT_ARRAY_TYPE)});
        Assertions.assertEquals(12, new SeaTunnelRow(new Object[] {matrix}).getBytesSize(type));
        Assertions.assertEquals(12, new SeaTunnelRow(new Object[] {matrix}).getBytesSize());
        Float[][][] nested = {matrix, null, {}};
        SeaTunnelRowType nestedType = new SeaTunnelRowType(new String[] {"nested"},
                new SeaTunnelDataType<?>[] {ArrayType.of(ArrayType.of(ArrayType.FLOAT_ARRAY_TYPE))});
        Assertions.assertEquals(12, new SeaTunnelRow(new Object[] {nested}).getBytesSize(nestedType));
        Assertions.assertEquals(12, new SeaTunnelRow(new Object[] {nested}).getBytesSize());
    }
}
