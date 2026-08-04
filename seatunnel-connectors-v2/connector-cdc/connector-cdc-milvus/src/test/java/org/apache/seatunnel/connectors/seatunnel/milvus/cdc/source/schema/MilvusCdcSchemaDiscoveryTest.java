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

package org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.schema;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.MilvusCdcSourceTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.MILVUS_INTERNAL_DYNAMIC_FIELD;

class MilvusCdcSchemaDiscoveryTest {

    @Test
    void convertNullableColumn() throws Exception {
        CreateCollectionReq.FieldSchema fieldSchema =
                CreateCollectionReq.FieldSchema.builder()
                        .name("nullable_field")
                        .dataType(DataType.Int64)
                        .isNullable(true)
                        .build();

        Method convertColumn =
                MilvusCdcSchemaDiscovery.class.getDeclaredMethod(
                        "convertColumn", CreateCollectionReq.FieldSchema.class);
        convertColumn.setAccessible(true);

        PhysicalColumn column =
                (PhysicalColumn) convertColumn.invoke(new MilvusCdcSchemaDiscovery(), fieldSchema);

        Assertions.assertTrue(column.isNullable());
    }

    @Test
    void convertNullableStructColumn() throws Exception {
        CreateCollectionReq.StructFieldSchema structFieldSchema =
                CreateCollectionReq.StructFieldSchema.builder()
                        .name("nullable_structs")
                        .nullable(true)
                        .build();

        Method convertStructField =
                MilvusCdcSchemaDiscovery.class.getDeclaredMethod(
                        "convertStructField", CreateCollectionReq.StructFieldSchema.class);
        convertStructField.setAccessible(true);

        PhysicalColumn column =
                (PhysicalColumn)
                        convertStructField.invoke(
                                new MilvusCdcSchemaDiscovery(), structFieldSchema);

        Assertions.assertTrue(column.isNullable());
    }

    @Test
    void convertInt8VectorColumn() throws Exception {
        CreateCollectionReq.FieldSchema fieldSchema =
                CreateCollectionReq.FieldSchema.builder()
                        .name("int8_vec")
                        .dataType(DataType.Int8Vector)
                        .dimension(4)
                        .build();

        Method convertColumn =
                MilvusCdcSchemaDiscovery.class.getDeclaredMethod(
                        "convertColumn", CreateCollectionReq.FieldSchema.class);
        convertColumn.setAccessible(true);

        PhysicalColumn column =
                (PhysicalColumn) convertColumn.invoke(new MilvusCdcSchemaDiscovery(), fieldSchema);

        Assertions.assertEquals(VectorType.VECTOR_INT8_TYPE, column.getDataType());
        Assertions.assertEquals("Int8Vector", column.getSourceType());
        Assertions.assertEquals(4, column.getScale());
    }

    @Test
    void dynamicColumnUsesMilvusInternalMetaName() throws Exception {
        CreateCollectionReq.FieldSchema idField =
                CreateCollectionReq.FieldSchema.builder()
                        .name("id")
                        .dataType(DataType.Int64)
                        .isPrimaryKey(true)
                        .isPartitionKey(true)
                        .build();
        CreateCollectionReq.FieldSchema internalDynamicField =
                CreateCollectionReq.FieldSchema.builder()
                        .name(MILVUS_INTERNAL_DYNAMIC_FIELD)
                        .dataType(DataType.JSON)
                        .build();
        CreateCollectionReq.CollectionSchema collectionSchema =
                CreateCollectionReq.CollectionSchema.builder()
                        .fieldSchemaList(Arrays.asList(idField, internalDynamicField))
                        .enableDynamicField(true)
                        .build();
        DescribeCollectionResp describeCollectionResp =
                DescribeCollectionResp.builder()
                        .collectionName("collection_a")
                        .autoID(false)
                        .enableDynamicField(true)
                        .collectionSchema(collectionSchema)
                        .build();

        Method buildCatalogTable =
                MilvusCdcSchemaDiscovery.class.getDeclaredMethod(
                        "buildCatalogTable",
                        MilvusClientV2.class,
                        String.class,
                        String.class,
                        DescribeCollectionResp.class);
        buildCatalogTable.setAccessible(true);

        CatalogTable catalogTable =
                (CatalogTable)
                        buildCatalogTable.invoke(
                                new MilvusCdcSchemaDiscovery(),
                                null,
                                "default",
                                "collection_a",
                                describeCollectionResp);

        long dynamicColumnCount =
                catalogTable.getTableSchema().getColumns().stream()
                        .filter(column -> MILVUS_INTERNAL_DYNAMIC_FIELD.equals(column.getName()))
                        .count();
        Column dynamicColumn =
                catalogTable.getTableSchema().getColumns().stream()
                        .filter(this::isMetadataColumn)
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        Assertions.assertEquals(1L, dynamicColumnCount);
        Assertions.assertEquals(MILVUS_INTERNAL_DYNAMIC_FIELD, dynamicColumn.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, dynamicColumn.getDataType());
        Assertions.assertFalse(
                Boolean.TRUE.equals(dynamicColumn.getOptions().get(CommonOptions.JSON.getName())));

        Method buildSchema =
                MilvusCdcSchemaDiscovery.class.getDeclaredMethod(
                        "buildSchema", MilvusCdcSourceTable.class, CatalogTable.class);
        buildSchema.setAccessible(true);
        MilvusCdcCollectionSchema collectionSchemaResult =
                (MilvusCdcCollectionSchema)
                        buildSchema.invoke(
                                new MilvusCdcSchemaDiscovery(),
                                MilvusCdcSourceTable.builder()
                                        .database("default")
                                        .collection("collection_a")
                                        .build(),
                                catalogTable);
        Assertions.assertTrue(collectionSchemaResult.isEnableDynamicField());
    }

    private boolean isMetadataColumn(Column column) {
        return column.getOptions() != null
                && Boolean.TRUE.equals(column.getOptions().get(CommonOptions.METADATA.getName()));
    }
}
