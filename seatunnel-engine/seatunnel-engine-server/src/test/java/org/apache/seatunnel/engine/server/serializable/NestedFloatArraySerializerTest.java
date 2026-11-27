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

package org.apache.seatunnel.engine.server.serializable;

import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

class NestedFloatArraySerializerTest {
    @Test
    void roundTripsNestedFloatArraysWithExistingRecordSerializer() {
        InternalSerializationService service = new DefaultSerializationServiceBuilder().build();
        try {
            for (int count : new int[] {0, 1, 3, 4097}) {
                Float[][] values = new Float[count][];
                for (int i = 0; i < count; i++) {
                    values[i] = new Float[] {(float) i, (float) -i};
                }
                if (count == 3) {
                    values[1] = null;
                    values[2] = new Float[] {null, 2.0f, 3.0f};
                }
                SeaTunnelRow row = new SeaTunnelRow(new Object[] {7L, values, "payload"});
                row.setTableId("documents");
                row.setRowKind(RowKind.UPDATE_AFTER);
                Record<?> decoded = service.toObject(service.toData(new Record<>(row)));
                SeaTunnelRow copy = (SeaTunnelRow) decoded.getData();
                Assertions.assertEquals("documents", copy.getTableId());
                Assertions.assertEquals(RowKind.UPDATE_AFTER, copy.getRowKind());
                Assertions.assertEquals(7L, copy.getField(0));
                Assertions.assertEquals("payload", copy.getField(2));
                Assertions.assertEquals(Float[][].class, copy.getField(1).getClass());
                Assertions.assertTrue(Arrays.deepEquals(values, (Float[][]) copy.getField(1)));
            }
        } finally {
            service.dispose();
        }
    }
}
