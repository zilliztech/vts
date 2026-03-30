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

package org.apache.seatunnel.connectors.seatunnel.chromadb.utils;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ChromaDBConverterTest {

    private TableSchema buildFullSchema() {
        return TableSchema.builder()
                .primaryKey(PrimaryKey.of("id", Collections.singletonList("id")))
                .columns(
                        Arrays.<Column>asList(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build(),
                                PhysicalColumn.builder()
                                        .name("embedding")
                                        .dataType(VectorType.VECTOR_FLOAT_TYPE)
                                        .scale(3)
                                        .build(),
                                PhysicalColumn.builder()
                                        .name("document")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build(),
                                PhysicalColumn.builder()
                                        .name("uri")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build(),
                                PhysicalColumn.builder()
                                        .name(CommonOptions.METADATA.getName())
                                        .dataType(BasicType.STRING_TYPE)
                                        .build()))
                .build();
    }

    private TableSchema buildSchemaWithoutVector() {
        return TableSchema.builder()
                .primaryKey(PrimaryKey.of("id", Collections.singletonList("id")))
                .columns(
                        Arrays.<Column>asList(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build(),
                                PhysicalColumn.builder()
                                        .name("document")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build(),
                                PhysicalColumn.builder()
                                        .name(CommonOptions.METADATA.getName())
                                        .dataType(BasicType.STRING_TYPE)
                                        .build()))
                .build();
    }

    // --- Basic conversion tests ---

    @Test
    public void testConvertFullRecord() {
        TableSchema schema = buildFullSchema();
        List<Float> embedding = Arrays.asList(1.0f, 2.0f, 3.0f);
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("category", "test");
        metadata.put("score", 42);

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "id1", embedding, "hello world", null, metadata);

        Assertions.assertEquals(RowKind.INSERT, row.getRowKind());
        Assertions.assertEquals("id1", row.getField(0));
        Assertions.assertInstanceOf(ByteBuffer.class, row.getField(1));
        Assertions.assertEquals("hello world", row.getField(2));
        Assertions.assertNull(row.getField(3)); // uri

        String metadataJson = (String) row.getField(4);
        Assertions.assertTrue(metadataJson.contains("\"category\":\"test\""));
        Assertions.assertTrue(metadataJson.contains("\"score\":42"));
    }

    @Test
    public void testConvertWithNullEmbedding() {
        TableSchema schema = buildFullSchema();
        Map<String, Object> metadata = Collections.singletonMap("key", "val");

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "id2", null, "doc text", null, metadata);

        Assertions.assertEquals("id2", row.getField(0));
        Assertions.assertNull(row.getField(1));
        Assertions.assertEquals("doc text", row.getField(2));
    }

    @Test
    public void testConvertWithEmptyEmbedding() {
        TableSchema schema = buildFullSchema();

        SeaTunnelRow row =
                ChromaDBConverter.convert(
                        schema, "id3", Collections.emptyList(), "doc", null, null);

        Assertions.assertNull(row.getField(1));
    }

    @Test
    public void testConvertWithNullDocument() {
        TableSchema schema = buildFullSchema();
        List<Float> embedding = Arrays.asList(1.0f, 2.0f, 3.0f);

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "id4", embedding, null, null, null);

        Assertions.assertEquals("id4", row.getField(0));
        Assertions.assertNotNull(row.getField(1));
        Assertions.assertNull(row.getField(2));
        Assertions.assertEquals("{}", row.getField(4));
    }

    @Test
    public void testConvertWithNullMetadata() {
        TableSchema schema = buildFullSchema();

        SeaTunnelRow row =
                ChromaDBConverter.convert(
                        schema, "id5", Arrays.asList(1.0f), "doc", null, null);

        Assertions.assertEquals("{}", row.getField(4));
    }

    @Test
    public void testConvertWithoutVectorColumn() {
        TableSchema schema = buildSchemaWithoutVector();
        Map<String, Object> metadata = Collections.singletonMap("k", "v");

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "id6", null, "doc text", null, metadata);

        Assertions.assertEquals(3, row.getArity());
        Assertions.assertEquals("id6", row.getField(0));
        Assertions.assertEquals("doc text", row.getField(1));
        Assertions.assertTrue(((String) row.getField(2)).contains("\"k\":\"v\""));
    }

    // --- Edge case: uri field ---

    @Test
    public void testConvertWithUri() {
        TableSchema schema = buildFullSchema();
        List<Float> embedding = Arrays.asList(1.0f, 2.0f, 3.0f);

        SeaTunnelRow row =
                ChromaDBConverter.convert(
                        schema, "id-uri", embedding, "doc", "http://example.com/img.png", null);

        Assertions.assertEquals("id-uri", row.getField(0));
        Assertions.assertNotNull(row.getField(1));
        Assertions.assertEquals("doc", row.getField(2));
        Assertions.assertEquals("http://example.com/img.png", row.getField(3));
        Assertions.assertEquals("{}", row.getField(4));
    }

    @Test
    public void testConvertWithNullUri() {
        TableSchema schema = buildFullSchema();

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "id-no-uri", null, "doc", null, null);

        Assertions.assertNull(row.getField(3)); // uri is null
    }

    @Test
    public void testVectorByteBufferSize() {
        TableSchema schema = buildFullSchema();
        List<Float> embedding = Arrays.asList(1.0f, 2.0f, 3.0f);

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "id7", embedding, "doc", null, null);

        ByteBuffer buffer = (ByteBuffer) row.getField(1);
        // 3 floats × 4 bytes = 12 bytes
        Assertions.assertEquals(12, buffer.capacity());
    }

    // --- Edge case: metadata with nested structures ---

    @Test
    public void testMetadataWithNestedMap() {
        TableSchema schema = buildFullSchema();
        Map<String, Object> nested = new HashMap<>();
        nested.put("inner_key", "inner_val");
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("outer", nested);

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "id", null, null, null, metadata);

        String json = (String) row.getField(4);
        Assertions.assertTrue(json.contains("\"outer\""));
        Assertions.assertTrue(json.contains("\"inner_key\":\"inner_val\""));
    }

    @Test
    public void testMetadataWithList() {
        TableSchema schema = buildFullSchema();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("tags", Arrays.asList("a", "b", "c"));

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "id", null, null, null, metadata);

        String json = (String) row.getField(4);
        Assertions.assertTrue(json.contains("\"tags\":[\"a\",\"b\",\"c\"]"));
    }

    @Test
    public void testMetadataWithEmptyMap() {
        TableSchema schema = buildFullSchema();
        Map<String, Object> metadata = new HashMap<>();

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "id", null, null, null, metadata);

        Assertions.assertEquals("{}", row.getField(4));
    }

    // --- Edge case: special characters ---

    @Test
    public void testMetadataWithUnicode() {
        TableSchema schema = buildFullSchema();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("中文key", "中文value");
        metadata.put("emoji", "\uD83D\uDE00");

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "id", null, null, null, metadata);

        String json = (String) row.getField(4);
        Assertions.assertTrue(json.contains("中文key"));
        Assertions.assertTrue(json.contains("中文value"));
    }

    @Test
    public void testDocumentWithSpecialChars() {
        TableSchema schema = buildFullSchema();
        String doc = "line1\nline2\ttab\"quote\\backslash";

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "id", null, doc, null, null);

        Assertions.assertEquals(doc, row.getField(2));
    }

    @Test
    public void testIdWithSpecialChars() {
        TableSchema schema = buildFullSchema();

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "id with spaces/slashes", null, null, null, null);

        Assertions.assertEquals("id with spaces/slashes", row.getField(0));
    }

    @Test
    public void testEmptyStringId() {
        TableSchema schema = buildFullSchema();

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "", null, null, null, null);

        Assertions.assertEquals("", row.getField(0));
    }

    // --- Edge case: high-dimension vector ---

    @Test
    public void testHighDimensionVector() {
        int dim = 1536; // OpenAI ada-002 dimension
        TableSchema schema =
                TableSchema.builder()
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id")))
                        .columns(
                                Arrays.<Column>asList(
                                        PhysicalColumn.builder()
                                                .name("id")
                                                .dataType(BasicType.STRING_TYPE)
                                                .build(),
                                        PhysicalColumn.builder()
                                                .name("embedding")
                                                .dataType(VectorType.VECTOR_FLOAT_TYPE)
                                                .scale(dim)
                                                .build(),
                                        PhysicalColumn.builder()
                                                .name(CommonOptions.METADATA.getName())
                                                .dataType(BasicType.STRING_TYPE)
                                                .build()))
                        .build();

        List<Float> embedding = new ArrayList<>(dim);
        for (int i = 0; i < dim; i++) {
            embedding.add((float) i / dim);
        }

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "id", embedding, null, null, null);

        ByteBuffer buffer = (ByteBuffer) row.getField(1);
        Assertions.assertEquals(dim * 4, buffer.capacity());
    }

    // --- Edge case: all fields null except id ---

    @Test
    public void testAllFieldsNullExceptId() {
        TableSchema schema = buildFullSchema();

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "only-id", null, null, null, null);

        Assertions.assertEquals("only-id", row.getField(0));
        Assertions.assertNull(row.getField(1));
        Assertions.assertNull(row.getField(2));
        Assertions.assertNull(row.getField(3)); // uri
        Assertions.assertEquals("{}", row.getField(4));
        Assertions.assertEquals(RowKind.INSERT, row.getRowKind());
    }

    // --- Edge case: metadata with numeric types ---

    @Test
    public void testMetadataWithMixedNumericTypes() {
        TableSchema schema = buildFullSchema();
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("int_val", 42);
        metadata.put("float_val", 3.14);
        metadata.put("long_val", 9999999999L);
        metadata.put("bool_val", true);
        metadata.put("null_val", null);

        SeaTunnelRow row =
                ChromaDBConverter.convert(schema, "id", null, null, null, metadata);

        String json = (String) row.getField(4);
        Assertions.assertTrue(json.contains("\"int_val\":42"));
        Assertions.assertTrue(json.contains("\"float_val\":3.14"));
        Assertions.assertTrue(json.contains("\"bool_val\":true"));
    }
}
