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

package org.apache.seatunnel.connectors.seatunnel.milvus;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq.FieldSchema;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConverter;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusSchemaConverter;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusSinkConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class Int8VectorSupportTest {

    @Test
    public void testSourceSchemaConvertsInt8Vector() {
        FieldSchema sourceField = FieldSchema.builder()
                .name("embedding")
                .dataType(DataType.Int8Vector)
                .dimension(32)
                .build();

        PhysicalColumn column = MilvusSourceConverter.convertColumn(sourceField);

        Assertions.assertEquals("embedding", column.getName());
        Assertions.assertEquals(VectorType.VECTOR_INT8_TYPE, column.getDataType());
        Assertions.assertEquals(32, column.getScale());
        Assertions.assertEquals("Int8Vector", column.getSourceType());
    }

    @Test
    public void testSourceRowPreservesInt8VectorByteBuffer() {
        byte[] bytes = new byte[]{1, -2, 127, -128};
        TableSchema tableSchema = TableSchema.builder()
                .column(PhysicalColumn.of(
                        "embedding",
                        VectorType.VECTOR_INT8_TYPE,
                        null,
                        4,
                        true,
                        null,
                        null))
                .build();
        QueryResultsWrapper.RowRecord record = new QueryResultsWrapper.RowRecord();
        record.put("embedding", ByteBuffer.wrap(bytes));

        SeaTunnelRow row = new MilvusSourceConverter(tableSchema)
                .convertToSeaTunnelRow(record, tableSchema, "collection", "_default");

        Assertions.assertArrayEquals(bytes, ((ByteBuffer) row.getField(0)).array());
    }

    @Test
    public void testSinkSchemaConvertsInt8Vector() {
        Column column = PhysicalColumn.of(
                "embedding",
                VectorType.VECTOR_INT8_TYPE,
                null,
                32,
                true,
                null,
                null);

        FieldSchema fieldSchema = MilvusSchemaConverter.convertToFieldType(column, null);

        Assertions.assertEquals(DataType.Int8Vector, fieldSchema.getDataType());
        Assertions.assertEquals(32, fieldSchema.getDimension());
    }

    @Test
    public void testSinkValueConvertsInt8VectorByteBuffer() {
        byte[] bytes = new byte[]{1, -2, 127, -128};
        MilvusSinkConverter converter = new MilvusSinkConverter();

        Object bySeaTunnelType = converter.convertBySeaTunnelType(
                VectorType.VECTOR_INT8_TYPE, false, ByteBuffer.wrap(bytes));
        Object byMilvusType = converter.convertByMilvusType(
                FieldSchema.builder().name("embedding").dataType(DataType.Int8Vector).dimension(4).build(),
                ByteBuffer.wrap(bytes));

        Assertions.assertEquals("[1,-2,127,-128]", bySeaTunnelType.toString());
        Assertions.assertEquals("[1,-2,127,-128]", byMilvusType.toString());
    }

    @Test
    public void testBuildMilvusDataWritesInt8Vector() {
        byte[] bytes = new byte[]{1, -2, 127, -128};
        TableSchema tableSchema = TableSchema.builder()
                .column(PhysicalColumn.of(
                        "embedding",
                        VectorType.VECTOR_INT8_TYPE,
                        null,
                        4,
                        true,
                        null,
                        null))
                .build();
        CatalogTable catalogTable = CatalogTable.of(
                TableIdentifier.of("test", TablePath.of("default", "int8_collection")),
                tableSchema,
                new HashMap<>(),
                new ArrayList<>(),
                "");
        DescribeCollectionResp describeCollectionResp = DescribeCollectionResp.builder()
                .collectionName("int8_collection")
                .autoID(false)
                .collectionSchema(CreateCollectionReq.CollectionSchema.builder()
                        .enableDynamicField(false)
                        .fieldSchemaList(Collections.singletonList(FieldSchema.builder()
                                .name("embedding")
                                .dataType(DataType.Int8Vector)
                                .dimension(4)
                                .build()))
                        .functionList(new ArrayList<>())
                        .build())
                .enableDynamicField(false)
                .build();

        JsonObject data = new MilvusSinkConverter().buildMilvusData(
                catalogTable,
                describeCollectionResp,
                new HashMap<>(),
                new SeaTunnelRow(new Object[]{ByteBuffer.wrap(bytes)}));

        JsonArray array = data.getAsJsonArray("embedding");
        Assertions.assertEquals(4, array.size());
        Assertions.assertEquals(1, array.get(0).getAsInt());
        Assertions.assertEquals(-2, array.get(1).getAsInt());
        Assertions.assertEquals(127, array.get(2).getAsInt());
        Assertions.assertEquals(-128, array.get(3).getAsInt());
    }
}
