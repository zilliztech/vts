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

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.milvus.common.MilvusConstants;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusSchemaConverter;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusSinkConverter;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.utils.MilvusSourceConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq.FieldSchema;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;

import java.util.ArrayList;
import java.util.Collections;

public class TextSupportTest {

    @Test
    public void testSourceSchemaConvertsTextAndPreservesMilvusType() {
        FieldSchema sourceField = textField();

        PhysicalColumn column = MilvusSourceConverter.convertColumn(sourceField);

        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals("Text", column.getSourceType());
        Assertions.assertEquals(
                DataType.Text.getCode(), column.getOptions().get(MilvusConstants.MILVUS_DATA_TYPE));
        Assertions.assertEquals(true, column.getOptions().get(MilvusConstants.ENABLE_ANALYZER));
        Assertions.assertEquals(true, column.getOptions().get(MilvusConstants.ENABLE_MATCH));
        Assertions.assertEquals(
                "{\"type\":\"standard\"}",
                column.getOptions().get(MilvusConstants.ANALYZER_PARAMS));
        Assertions.assertFalse(column.getOptions().containsKey(MilvusConstants.MAX_LENGTH));
    }

    @Test
    public void testVarCharSourceDoesNotPreserveMilvusType() {
        FieldSchema sourceField =
                FieldSchema.builder()
                        .name("title")
                        .dataType(DataType.VarChar)
                        .maxLength(128)
                        .build();

        PhysicalColumn column = MilvusSourceConverter.convertColumn(sourceField);

        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(128, column.getOptions().get(MilvusConstants.MAX_LENGTH));
        Assertions.assertFalse(
                column.getOptions().containsKey(MilvusConstants.MILVUS_DATA_TYPE));
    }

    @Test
    public void testMilvusToMilvusSchemaRestoresText() {
        PhysicalColumn column = MilvusSourceConverter.convertColumn(textField());

        FieldSchema targetField = MilvusSchemaConverter.convertToFieldType(column, null);

        Assertions.assertEquals(DataType.Text, targetField.getDataType());
        Assertions.assertTrue(targetField.getEnableAnalyzer());
        Assertions.assertTrue(targetField.getEnableMatch());
        Assertions.assertEquals(
                Collections.singletonMap("type", "standard"), targetField.getAnalyzerParams());
    }

    @Test
    public void testPlainStringStillMapsToVarChar() {
        PhysicalColumn column =
                PhysicalColumn.of(
                        "content", BasicType.STRING_TYPE, 0L, true, null, null, null, null);

        FieldSchema targetField = MilvusSchemaConverter.convertToFieldType(column, null);

        Assertions.assertEquals(DataType.VarChar, targetField.getDataType());
        Assertions.assertEquals(65535, targetField.getMaxLength());
    }

    @Test
    public void testSinkConvertsTextValue() {
        String value = "hello";

        Object converted = new MilvusSinkConverter().convertByMilvusType(textField(), value);

        Assertions.assertEquals(value, converted);
    }

    @Test
    public void testBulkWriterSchemaUsesGrpcTextType() {
        DescribeCollectionResp describeCollectionResp =
                DescribeCollectionResp.builder()
                        .collectionName("text_collection")
                        .autoID(false)
                        .enableDynamicField(false)
                        .collectionSchema(
                                CreateCollectionReq.CollectionSchema.builder()
                                        .fieldSchemaList(Collections.singletonList(textField()))
                                        .functionList(new ArrayList<>())
                                        .build())
                        .build();

        CollectionSchemaParam schema =
                MilvusSinkConverter.convertToMilvusSchema(describeCollectionResp);

        Assertions.assertEquals(1, schema.getFieldTypes().size());
        Assertions.assertEquals(
                io.milvus.grpc.DataType.Text, schema.getFieldTypes().get(0).getDataType());
    }

    private FieldSchema textField() {
        return FieldSchema.builder()
                .name("content")
                .dataType(DataType.Text)
                .isNullable(true)
                .enableAnalyzer(true)
                .enableMatch(true)
                .analyzerParams(Collections.singletonMap("type", "standard"))
                .build();
    }
}
