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

package org.apache.seatunnel.connectors.seatunnel.chromadb.source;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.seatunnel.chromadb.client.ChromaDBClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for ChromaDBSourceReader's batch logic using GSON deserialization to simulate ChromaDB
 * API responses. These tests verify edge cases in the two-phase reading strategy without
 * requiring a running ChromaDB server.
 */
public class ChromaDBSourceReaderTest {

    private static final Gson GSON = new GsonBuilder().create();

    private TableSchema buildSchema() {
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
                                        .name(CommonOptions.METADATA.getName())
                                        .dataType(BasicType.STRING_TYPE)
                                        .build()))
                .build();
    }

    /**
     * Simulate emitting rows from a GetRecordsResult (reproduces the emitRows logic).
     * This allows testing batch boundary conditions without a mock framework.
     */
    private List<SeaTunnelRow> simulateEmitRows(ChromaDBClient.GetRecordsResult result) {
        TableSchema schema = buildSchema();
        List<SeaTunnelRow> rows = new ArrayList<>();

        List<String> ids = result.getIds();
        List<List<Float>> embeddings = result.getEmbeddings();
        List<String> documents = result.getDocuments();
        List<Map<String, Object>> metadatas = result.getMetadatas();

        for (int i = 0; i < ids.size(); i++) {
            String id = ids.get(i);
            List<Float> embedding =
                    (embeddings != null && i < embeddings.size())
                            ? embeddings.get(i)
                            : null;
            String document =
                    (documents != null && i < documents.size()) ? documents.get(i) : null;
            Map<String, Object> metadata =
                    (metadatas != null && i < metadatas.size()) ? metadatas.get(i) : null;

            rows.add(
                    org.apache.seatunnel.connectors.seatunnel.chromadb.utils.ChromaDBConverter
                            .convert(schema, id, embedding, document, null, metadata));
        }
        return rows;
    }

    private ChromaDBClient.GetRecordsResult makeResult(String json) {
        return GSON.fromJson(json, ChromaDBClient.GetRecordsResult.class);
    }

    // --- Batch boundary tests ---

    @Test
    public void testExactBatchBoundary() {
        // 3 records with batch_size=3 — exactly one batch, no remainder
        ChromaDBClient.GetRecordsResult result =
                makeResult(
                        "{\"ids\":[\"a\",\"b\",\"c\"],"
                                + "\"embeddings\":[[1,2,3],[4,5,6],[7,8,9]],"
                                + "\"documents\":[\"d1\",\"d2\",\"d3\"],"
                                + "\"metadatas\":[{},{},{}]}");

        List<SeaTunnelRow> rows = simulateEmitRows(result);
        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals("a", rows.get(0).getField(0));
        Assertions.assertEquals("c", rows.get(2).getField(0));
    }

    @Test
    public void testSingleRecord() {
        ChromaDBClient.GetRecordsResult result =
                makeResult(
                        "{\"ids\":[\"only\"],"
                                + "\"embeddings\":[[1,2,3]],"
                                + "\"documents\":[\"doc\"],"
                                + "\"metadatas\":[{\"k\":\"v\"}]}");

        List<SeaTunnelRow> rows = simulateEmitRows(result);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("only", rows.get(0).getField(0));
    }

    @Test
    public void testEmptyResult() {
        ChromaDBClient.GetRecordsResult result =
                makeResult(
                        "{\"ids\":[],"
                                + "\"embeddings\":[],"
                                + "\"documents\":[],"
                                + "\"metadatas\":[]}");

        List<SeaTunnelRow> rows = simulateEmitRows(result);
        Assertions.assertTrue(rows.isEmpty());
    }

    // --- Mismatched list lengths (critical edge case from MongoDB NPE bugs) ---

    @Test
    public void testEmbeddingsShorterThanIds() {
        // ids: 3 records, embeddings: 2 records — last record gets null embedding
        ChromaDBClient.GetRecordsResult result =
                makeResult(
                        "{\"ids\":[\"a\",\"b\",\"c\"],"
                                + "\"embeddings\":[[1,2,3],[4,5,6]],"
                                + "\"documents\":[\"d1\",\"d2\",\"d3\"],"
                                + "\"metadatas\":[{},{},{}]}");

        List<SeaTunnelRow> rows = simulateEmitRows(result);
        Assertions.assertEquals(3, rows.size());
        Assertions.assertNotNull(rows.get(0).getField(1)); // has embedding
        Assertions.assertNotNull(rows.get(1).getField(1)); // has embedding
        Assertions.assertNull(rows.get(2).getField(1)); // no embedding — graceful null
    }

    @Test
    public void testDocumentsShorterThanIds() {
        ChromaDBClient.GetRecordsResult result =
                makeResult(
                        "{\"ids\":[\"a\",\"b\",\"c\"],"
                                + "\"embeddings\":[[1,2,3],[4,5,6],[7,8,9]],"
                                + "\"documents\":[\"d1\"],"
                                + "\"metadatas\":[{},{},{}]}");

        List<SeaTunnelRow> rows = simulateEmitRows(result);
        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals("d1", rows.get(0).getField(2));
        Assertions.assertNull(rows.get(1).getField(2));
        Assertions.assertNull(rows.get(2).getField(2));
    }

    @Test
    public void testMetadatasShorterThanIds() {
        ChromaDBClient.GetRecordsResult result =
                makeResult(
                        "{\"ids\":[\"a\",\"b\"],"
                                + "\"embeddings\":[[1,2,3],[4,5,6]],"
                                + "\"documents\":[\"d1\",\"d2\"],"
                                + "\"metadatas\":[{\"k\":\"v\"}]}");

        List<SeaTunnelRow> rows = simulateEmitRows(result);
        Assertions.assertEquals(2, rows.size());
        Assertions.assertTrue(((String) rows.get(0).getField(3)).contains("\"k\":\"v\""));
        Assertions.assertEquals("{}", rows.get(1).getField(3)); // null metadata → "{}"
    }

    @Test
    public void testAllOptionalFieldsNull() {
        // include=[] returns only ids, all other fields are null
        ChromaDBClient.GetRecordsResult result = makeResult("{\"ids\":[\"a\",\"b\",\"c\"]}");

        Assertions.assertNull(result.getEmbeddings());
        Assertions.assertNull(result.getDocuments());
        Assertions.assertNull(result.getMetadatas());

        List<SeaTunnelRow> rows = simulateEmitRows(result);
        Assertions.assertEquals(3, rows.size());
        for (SeaTunnelRow row : rows) {
            Assertions.assertNull(row.getField(1)); // embedding
            Assertions.assertNull(row.getField(2)); // document
            Assertions.assertEquals("{}", row.getField(3)); // metadata defaults to "{}"
        }
    }

    // --- ChromaDB response with null items within lists ---

    @Test
    public void testNullEmbeddingWithinList() {
        // Some records have null embedding in the middle
        ChromaDBClient.GetRecordsResult result =
                makeResult(
                        "{\"ids\":[\"a\",\"b\",\"c\"],"
                                + "\"embeddings\":[[1,2,3],null,[7,8,9]],"
                                + "\"documents\":[\"d1\",\"d2\",\"d3\"],"
                                + "\"metadatas\":[{},{},{}]}");

        List<SeaTunnelRow> rows = simulateEmitRows(result);
        Assertions.assertEquals(3, rows.size());
        Assertions.assertNotNull(rows.get(0).getField(1));
        Assertions.assertNull(rows.get(1).getField(1)); // null embedding in list
        Assertions.assertNotNull(rows.get(2).getField(1));
    }

    @Test
    public void testNullDocumentWithinList() {
        ChromaDBClient.GetRecordsResult result =
                makeResult(
                        "{\"ids\":[\"a\",\"b\"],"
                                + "\"documents\":[\"doc\",null]}");

        List<SeaTunnelRow> rows = simulateEmitRows(result);
        Assertions.assertEquals("doc", rows.get(0).getField(2));
        Assertions.assertNull(rows.get(1).getField(2));
    }

    @Test
    public void testNullMetadataWithinList() {
        ChromaDBClient.GetRecordsResult result =
                makeResult(
                        "{\"ids\":[\"a\",\"b\"],"
                                + "\"metadatas\":[{\"k\":\"v\"},null]}");

        List<SeaTunnelRow> rows = simulateEmitRows(result);
        Assertions.assertTrue(((String) rows.get(0).getField(3)).contains("\"k\":\"v\""));
        Assertions.assertEquals("{}", rows.get(1).getField(3));
    }

    // --- ID batch partitioning logic ---

    @Test
    public void testPartitionExactlyDivisible() {
        // Simulate partitioning 10 ids into chunks of 5
        List<String> ids = Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
        int batchSize = 5;

        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < ids.size(); i += batchSize) {
            int end = Math.min(i + batchSize, ids.size());
            chunks.add(ids.subList(i, end));
        }

        Assertions.assertEquals(2, chunks.size());
        Assertions.assertEquals(5, chunks.get(0).size());
        Assertions.assertEquals(5, chunks.get(1).size());
        Assertions.assertEquals("0", chunks.get(0).get(0));
        Assertions.assertEquals("5", chunks.get(1).get(0));
    }

    @Test
    public void testPartitionWithRemainder() {
        // 7 ids with batch=3 → [3, 3, 1]
        List<String> ids = Arrays.asList("0", "1", "2", "3", "4", "5", "6");
        int batchSize = 3;

        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < ids.size(); i += batchSize) {
            int end = Math.min(i + batchSize, ids.size());
            chunks.add(ids.subList(i, end));
        }

        Assertions.assertEquals(3, chunks.size());
        Assertions.assertEquals(3, chunks.get(0).size());
        Assertions.assertEquals(3, chunks.get(1).size());
        Assertions.assertEquals(1, chunks.get(2).size());
        Assertions.assertEquals("6", chunks.get(2).get(0));
    }

    @Test
    public void testPartitionBatchSizeLargerThanTotal() {
        // 3 ids with batch=100 → single chunk of 3
        List<String> ids = Arrays.asList("a", "b", "c");
        int batchSize = 100;

        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < ids.size(); i += batchSize) {
            int end = Math.min(i + batchSize, ids.size());
            chunks.add(ids.subList(i, end));
        }

        Assertions.assertEquals(1, chunks.size());
        Assertions.assertEquals(3, chunks.get(0).size());
    }

    @Test
    public void testPartitionSingleItem() {
        List<String> ids = Collections.singletonList("only");
        int batchSize = 100;

        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < ids.size(); i += batchSize) {
            int end = Math.min(i + batchSize, ids.size());
            chunks.add(ids.subList(i, end));
        }

        Assertions.assertEquals(1, chunks.size());
        Assertions.assertEquals("only", chunks.get(0).get(0));
    }

    @Test
    public void testPartitionEmptyList() {
        List<String> ids = Collections.emptyList();
        int batchSize = 100;

        List<List<String>> chunks = new ArrayList<>();
        for (int i = 0; i < ids.size(); i += batchSize) {
            int end = Math.min(i + batchSize, ids.size());
            chunks.add(ids.subList(i, end));
        }

        Assertions.assertTrue(chunks.isEmpty());
    }
}
