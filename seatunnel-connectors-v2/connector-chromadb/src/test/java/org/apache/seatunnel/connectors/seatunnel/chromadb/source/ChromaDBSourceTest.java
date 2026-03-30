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

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.seatunnel.chromadb.exception.ChromaDBConnectorException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for ChromaDBSource schema detection using MockWebServer. Verifies buildCatalogTable logic
 * across different collection states. Schema is now detected from collection info's schema.keys
 * instead of sampling data.
 */
@ExtendWith(MockitoExtension.class)
public class ChromaDBSourceTest {

    private MockWebServer server;
    @Mock private SourceReader.Context mockReaderContext;

    @BeforeEach
    void setup() throws Exception {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void teardown() throws Exception {
        server.shutdown();
    }

    private ReadonlyConfig makeConfig(String collectionName) {
        Map<String, Object> map = new HashMap<>();
        map.put("url", server.url("/").toString());
        map.put("token", "");
        map.put("tenant", "default_tenant");
        map.put("database", "default_database");
        map.put("collections", java.util.Collections.singletonList(collectionName));
        return ReadonlyConfig.fromMap(map);
    }

    /** Helper to build a collection info JSON with schema.keys. */
    private String collectionJson(
            String name, Integer dimension, String hnswSpace, boolean hasEmbedding,
            boolean hasDocument) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"id\":\"col-uuid-").append(name).append("\",\"name\":\"").append(name).append("\"");
        if (dimension != null) {
            sb.append(",\"dimension\":").append(dimension);
        }
        if (hnswSpace != null) {
            sb.append(",\"metadata\":{\"hnsw:space\":\"").append(hnswSpace).append("\"}");
        } else {
            sb.append(",\"metadata\":{}");
        }
        // schema.keys
        sb.append(",\"schema\":{\"keys\":{");
        boolean first = true;
        if (hasEmbedding) {
            sb.append("\"#embedding\":{\"float_list\":{}}");
            first = false;
        }
        if (hasDocument) {
            if (!first) sb.append(",");
            sb.append("\"#document\":{\"string\":{}}");
        }
        sb.append("}}");
        sb.append("}");
        return sb.toString();
    }

    // --- Schema detection ---

    @Test
    void testSchemaDetectionWithFullData() {
        // collection has embedding (dim=3), document, and cosine metric
        server.enqueue(
                new MockResponse()
                        .setBody(collectionJson("test_col", 3, "cosine", true, true)));

        ChromaDBSource source = new ChromaDBSource(makeConfig("test_col"));

        Assertions.assertEquals("ChromaDB", source.getPluginName());
        Assertions.assertEquals(Boundedness.BOUNDED, source.getBoundedness());

        List<CatalogTable> tables = source.getProducedCatalogTables();
        Assertions.assertEquals(1, tables.size());

        CatalogTable table = tables.get(0);
        TableSchema schema = table.getTableSchema();
        List<Column> columns = schema.getColumns();

        // id, embedding, document, uri, metadata
        Assertions.assertEquals(5, columns.size());
        Assertions.assertEquals("id", columns.get(0).getName());
        Assertions.assertEquals("embedding", columns.get(1).getName());
        Assertions.assertEquals(VectorType.VECTOR_FLOAT_TYPE, columns.get(1).getDataType());
        Assertions.assertEquals(3, columns.get(1).getScale());
        Assertions.assertEquals("document", columns.get(2).getName());
        Assertions.assertEquals("uri", columns.get(3).getName());

        // Index creation is left to the user on the target side
        Assertions.assertTrue(table.getOptions().isEmpty());
    }

    @Test
    void testSchemaDetectionDimensionFromCollectionInfo() {
        // dimension=128 from collection info
        server.enqueue(
                new MockResponse()
                        .setBody(collectionJson("test_col", 128, null, true, true)));

        ChromaDBSource source = new ChromaDBSource(makeConfig("test_col"));
        TableSchema schema = source.getProducedCatalogTables().get(0).getTableSchema();

        Column vectorCol = schema.getColumns().get(1);
        Assertions.assertEquals(128, vectorCol.getScale());
    }

    @Test
    void testSchemaDetectionEmptyCollectionSkipped() {
        // empty collection: dimension is null → skipped (Milvus requires vector field)
        server.enqueue(
                new MockResponse()
                        .setBody(collectionJson("empty_col", null, null, true, true)));

        Assertions.assertThrows(
                ChromaDBConnectorException.class,
                () -> new ChromaDBSource(makeConfig("empty_col")));
    }

    @Test
    void testSchemaDetectionNoDocuments() {
        // collection has embedding but no document in schema — document column is still added (nullable)
        server.enqueue(
                new MockResponse()
                        .setBody(collectionJson("no_doc", 2, null, true, false)));

        ChromaDBSource source = new ChromaDBSource(makeConfig("no_doc"));
        TableSchema schema = source.getProducedCatalogTables().get(0).getTableSchema();

        // id, embedding, document, uri, metadata — always 5 columns
        Assertions.assertEquals(5, schema.getColumns().size());
        Assertions.assertEquals("embedding", schema.getColumns().get(1).getName());
        Assertions.assertEquals("document", schema.getColumns().get(2).getName());
    }

    @Test
    void testSchemaDetectionNoEmbeddings() {
        // dimension=null → collection skipped → exception because no migratable collections
        server.enqueue(
                new MockResponse()
                        .setBody(collectionJson("no_emb", null, null, false, true)));

        Assertions.assertThrows(
                ChromaDBConnectorException.class,
                () -> new ChromaDBSource(makeConfig("no_emb")));
    }

    @Test
    void testSchemaDetectionNoSchemaKeys() {
        // old ChromaDB or missing schema/dimension → collection skipped → exception
        server.enqueue(
                new MockResponse()
                        .setBody(
                                "{\"id\":\"col-uuid\",\"name\":\"old_col\","
                                        + "\"metadata\":{}}"));

        Assertions.assertThrows(
                ChromaDBConnectorException.class,
                () -> new ChromaDBSource(makeConfig("old_col")));
    }

    @Test
    void testSchemaDetectionNullMetadata() {
        // dimension=10 so collection is not skipped; metadata is null
        server.enqueue(
                new MockResponse()
                        .setBody(
                                "{\"id\":\"col-uuid\",\"name\":\"test\","
                                        + "\"metadata\":null,\"dimension\":10,"
                                        + "\"schema\":{\"keys\":"
                                        + "{\"#embedding\":{},\"#document\":{}}}}"));

        ChromaDBSource source = new ChromaDBSource(makeConfig("test"));
        CatalogTable table = source.getProducedCatalogTables().get(0);

        Assertions.assertTrue(table.getOptions().isEmpty());
    }

    @Test
    void testCreateReaderReturnsNonNull() {
        server.enqueue(
                new MockResponse()
                        .setBody(collectionJson("test", 3, null, true, true)));

        ChromaDBSource source = new ChromaDBSource(makeConfig("test"));
        Assertions.assertNotNull(source.createReader(mockReaderContext));
    }

    @Test
    void testEmptyCollectionsConfigThrows() {
        // Empty collections is not allowed — must specify at least one
        Map<String, Object> map = new HashMap<>();
        map.put("url", server.url("/").toString());
        // No "collections" key → should throw
        ReadonlyConfig config = ReadonlyConfig.fromMap(map);

        Assertions.assertThrows(
                ChromaDBConnectorException.class, () -> new ChromaDBSource(config));
    }

    @Test
    void testEmptyCollectionThrows() {
        // A collection with unknown dimension (empty, no records) should throw
        server.enqueue(
                new MockResponse()
                        .setBody(collectionJson("empty_col", null, null, true, true)));

        Map<String, Object> map = new HashMap<>();
        map.put("url", server.url("/").toString());
        map.put("collections", java.util.Collections.singletonList("empty_col"));
        ReadonlyConfig config = ReadonlyConfig.fromMap(map);

        Assertions.assertThrows(
                ChromaDBConnectorException.class, () -> new ChromaDBSource(config));
    }

    @Test
    void testMixedEmptyAndNonEmptyCollectionThrows() {
        // Even one empty collection in the list should cause failure
        server.enqueue(
                new MockResponse()
                        .setBody(collectionJson("good", 4, null, true, true)));
        server.enqueue(
                new MockResponse()
                        .setBody(collectionJson("empty", null, null, true, true)));

        Map<String, Object> map = new HashMap<>();
        map.put("url", server.url("/").toString());
        map.put("collections", java.util.Arrays.asList("good", "empty"));
        ReadonlyConfig config = ReadonlyConfig.fromMap(map);

        Assertions.assertThrows(
                ChromaDBConnectorException.class, () -> new ChromaDBSource(config));
    }

    @Test
    void testMultipleCollectionsProduceMultipleTables() {
        // 3 collections with different dimensions
        server.enqueue(
                new MockResponse()
                        .setBody(collectionJson("col_a", 3, "cosine", true, true)));
        server.enqueue(
                new MockResponse()
                        .setBody(collectionJson("col_b", 8, null, true, false)));

        Map<String, Object> map = new HashMap<>();
        map.put("url", server.url("/").toString());
        map.put("collections", java.util.Arrays.asList("col_a", "col_b"));
        ReadonlyConfig config = ReadonlyConfig.fromMap(map);

        ChromaDBSource source = new ChromaDBSource(config);
        List<CatalogTable> tables = source.getProducedCatalogTables();
        Assertions.assertEquals(2, tables.size());
    }
}
