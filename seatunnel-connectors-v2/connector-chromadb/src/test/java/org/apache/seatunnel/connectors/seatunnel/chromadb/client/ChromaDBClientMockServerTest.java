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

package org.apache.seatunnel.connectors.seatunnel.chromadb.client;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.chromadb.exception.ChromaDBConnectorException;
import org.apache.seatunnel.connectors.seatunnel.chromadb.utils.ChromaDBConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for ChromaDBClient HTTP interaction using MockWebServer.
 * Covers auth headers, error handling, and request/response format.
 */
public class ChromaDBClientMockServerTest {

    private MockWebServer server;

    @BeforeEach
    void setup() throws Exception {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void teardown() throws Exception {
        server.shutdown();
    }

    private ChromaDBClient createClient(String token) {
        return createClient(token, "BEARER");
    }

    private ChromaDBClient createClient(String token, String authType) {
        Map<String, Object> map = new HashMap<>();
        map.put("url", server.url("/").toString());
        map.put("token", token);
        map.put("auth_type", authType);
        return new ChromaDBClient(ReadonlyConfig.fromMap(map));
    }

    private ChromaDBClient createClientBasic(String username, String password) {
        Map<String, Object> map = new HashMap<>();
        map.put("url", server.url("/").toString());
        map.put("auth_type", "BASIC");
        map.put("username", username);
        map.put("password", password);
        return new ChromaDBClient(ReadonlyConfig.fromMap(map));
    }

    // --- Successful requests ---

    @Test
    void testGetCollectionSuccess() throws Exception {
        server.enqueue(
                new MockResponse()
                        .setBody(
                                "{\"id\":\"uuid-123\",\"name\":\"test_col\","
                                        + "\"metadata\":{\"hnsw:space\":\"cosine\"},\"dimension\":128,"
                                        + "\"configuration_json\":{\"key\":\"value\"}}"));

        try (ChromaDBClient client = createClient("")) {
            ChromaDBClient.CollectionInfo info = client.getCollection("test_col");

            Assertions.assertEquals("uuid-123", info.getId());
            Assertions.assertEquals("test_col", info.getName());
            Assertions.assertEquals(128, info.getDimension());
            Assertions.assertEquals("cosine", info.getMetadata().get("hnsw:space"));
            Assertions.assertNotNull(info.getConfigurationJson());
            Assertions.assertEquals("value", info.getConfigurationJson().get("key"));

            RecordedRequest req = server.takeRequest();
            Assertions.assertEquals("GET", req.getMethod());
            Assertions.assertTrue(req.getPath().contains("/collections/test_col"));
        }
    }

    @Test
    void testGetPostSuccess() throws Exception {
        server.enqueue(
                new MockResponse()
                        .setBody(
                                "{\"ids\":[\"a\",\"b\"],\"embeddings\":[[1,2],[3,4]],"
                                        + "\"documents\":[\"d1\",\"d2\"],\"metadatas\":[{},{}]}"));

        try (ChromaDBClient client = createClient("")) {
            ChromaDBClient.GetRecordsResult result =
                    client.getRecords(
                            "col-id",
                            new ChromaDBClient.GetRecordsRequest()
                                    .setLimit(100)
                                    .setInclude(
                                            Arrays.asList(
                                                    "embeddings", "documents", "metadatas")));

            Assertions.assertEquals(2, result.getIds().size());
            Assertions.assertEquals(2, result.getEmbeddings().size());

            RecordedRequest req = server.takeRequest();
            Assertions.assertEquals("POST", req.getMethod());
            String body = req.getBody().readUtf8();
            Assertions.assertTrue(body.contains("\"limit\":100"));
        }
    }

    @Test
    void testGetByIdsPostBody() throws Exception {
        server.enqueue(new MockResponse().setBody("{\"ids\":[\"a\"]}"));

        try (ChromaDBClient client = createClient("")) {
            client.getRecords(
                    "col-id",
                    new ChromaDBClient.GetRecordsRequest()
                            .setIds(Arrays.asList("id1", "id2"))
                            .setInclude(Collections.emptyList()));

            RecordedRequest req = server.takeRequest();
            String body = req.getBody().readUtf8();
            Assertions.assertTrue(body.contains("\"ids\":[\"id1\",\"id2\"]"));
            Assertions.assertTrue(body.contains("\"include\":[]"));
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {-300L, 0L, 300L, 9007199254740993L, Long.MIN_VALUE, Long.MAX_VALUE})
    void testIntegerMetadataSurvivesHttpAndRowConversion(long value) {
        // The sampled row has no late_int; a later row introduces it.
        server.enqueue(
                new MockResponse()
                        .setBody(
                                "{\"ids\":[\"sample\",\"later\"],\"metadatas\":[{},"
                                        + "{\"late_int\":"
                                        + value
                                        + ",\"ratio\":37.5,\"flag\":true,\"text\":\"hello\"}]}"));
        try (ChromaDBClient client = createClient("")) {
            ChromaDBClient.GetRecordsResult result =
                    client.getRecords(
                            "col-id", new ChromaDBClient.GetRecordsRequest().setLimit(2));
            Assertions.assertTrue(result.getMetadatas().get(0).isEmpty());
            Map<String, Object> metadata = result.getMetadatas().get(1);
            Assertions.assertEquals(Long.valueOf(value), metadata.get("late_int"));
            Assertions.assertEquals(37.5, metadata.get("ratio"));
            Assertions.assertEquals(Boolean.TRUE, metadata.get("flag"));
            Assertions.assertEquals("hello", metadata.get("text"));

            TableSchema schema =
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.builder()
                                            .name(CommonOptions.METADATA.getName())
                                            .dataType(BasicType.STRING_TYPE)
                                            .build())
                            .build();
            SeaTunnelRow row =
                    ChromaDBConverter.convert(schema, "later", null, null, null, metadata);
            JsonObject json = JsonParser.parseString((String) row.getField(0)).getAsJsonObject();
            // INT64 fixed-field conversion parses the serialized numeric value as a long.
            Assertions.assertEquals(value, Long.parseLong(json.get("late_int").toString()));
            Assertions.assertEquals(37.5, json.get("ratio").getAsDouble());
            Assertions.assertThrows(
                    NumberFormatException.class,
                    () -> Long.parseLong(json.get("ratio").toString()));
        }
    }

    // --- Auth header ---

    @Test
    void testAuthHeaderSentWhenTokenProvided() throws Exception {
        server.enqueue(
                new MockResponse()
                        .setBody("{\"id\":\"u\",\"name\":\"c\",\"dimension\":3}"));

        try (ChromaDBClient client = createClient("my-secret-token")) {
            client.getCollection("c");

            RecordedRequest req = server.takeRequest();
            Assertions.assertEquals("Bearer my-secret-token", req.getHeader("Authorization"));
        }
    }

    @Test
    void testXChromaTokenHeaderSent() throws Exception {
        server.enqueue(
                new MockResponse()
                        .setBody("{\"id\":\"u\",\"name\":\"c\",\"dimension\":3}"));

        try (ChromaDBClient client = createClient("my-token", "X_CHROMA_TOKEN")) {
            client.getCollection("c");

            RecordedRequest req = server.takeRequest();
            Assertions.assertEquals("my-token", req.getHeader("X-Chroma-Token"));
            Assertions.assertNull(req.getHeader("Authorization"));
        }
    }

    @Test
    void testBasicAuthHeaderSent() throws Exception {
        server.enqueue(
                new MockResponse()
                        .setBody("{\"id\":\"u\",\"name\":\"c\",\"dimension\":3}"));

        try (ChromaDBClient client = createClientBasic("admin", "my-password")) {
            client.getCollection("c");

            RecordedRequest req = server.takeRequest();
            String expected =
                    "Basic "
                            + java.util.Base64.getEncoder()
                                    .encodeToString("admin:my-password".getBytes());
            Assertions.assertEquals(expected, req.getHeader("Authorization"));
        }
    }

    @Test
    void testNoAuthHeaderWhenTokenEmpty() throws Exception {
        server.enqueue(
                new MockResponse()
                        .setBody("{\"id\":\"u\",\"name\":\"c\",\"dimension\":3}"));

        try (ChromaDBClient client = createClient("")) {
            client.getCollection("c");

            RecordedRequest req = server.takeRequest();
            Assertions.assertNull(req.getHeader("Authorization"));
        }
    }

    // --- Error responses ---

    @Test
    void test404ThrowsCollectionNotFound() {
        server.enqueue(new MockResponse().setResponseCode(404).setBody("not found"));

        try (ChromaDBClient client = createClient("")) {
            ChromaDBConnectorException ex =
                    Assertions.assertThrows(
                            ChromaDBConnectorException.class,
                            () -> client.getCollection("missing"));
            Assertions.assertTrue(
                    ex.getMessage().contains("CHROMADB-02"),
                    "Should use COLLECTION_NOT_FOUND error code, got: " + ex.getMessage());
        }
    }

    @Test
    void test500ThrowsConnectFailed() {
        server.enqueue(new MockResponse().setResponseCode(500).setBody("server error"));

        try (ChromaDBClient client = createClient("")) {
            ChromaDBConnectorException ex =
                    Assertions.assertThrows(
                            ChromaDBConnectorException.class,
                            () -> client.getCollection("col"));
            Assertions.assertTrue(
                    ex.getMessage().contains("CHROMADB-01"),
                    "Should use CONNECT_FAILED error code, got: " + ex.getMessage());
        }
    }

    // --- Retry ---

    @Test
    void testRetrySucceedsAfter503() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(503).setBody("unavailable"));
        server.enqueue(
                new MockResponse()
                        .setBody("{\"id\":\"u\",\"name\":\"c\",\"dimension\":3}"));

        try (ChromaDBClient client = createClient("")) {
            client.setRetryPolicy(1, 0);
            ChromaDBClient.CollectionInfo info = client.getCollection("c");
            Assertions.assertEquals("u", info.getId());
            Assertions.assertEquals(2, server.getRequestCount());
        }
    }

    @Test
    void testRetrySucceedsAfter429() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(429).setBody("rate limited"));
        server.enqueue(new MockResponse().setResponseCode(429).setBody("rate limited"));
        server.enqueue(
                new MockResponse()
                        .setBody(
                                "{\"ids\":[\"a\"],\"embeddings\":[[1,2]],"
                                        + "\"documents\":[\"d1\"],\"metadatas\":[{}]}"));

        try (ChromaDBClient client = createClient("")) {
            client.setRetryPolicy(2, 0);
            ChromaDBClient.GetRecordsResult result =
                    client.getRecords(
                            "col-id",
                            new ChromaDBClient.GetRecordsRequest().setLimit(10));
            Assertions.assertEquals(1, result.getIds().size());
            Assertions.assertEquals(3, server.getRequestCount());
        }
    }

    // --- URL construction ---

    @Test
    void testUrlPathContainsTenantAndDatabase() throws Exception {
        server.enqueue(
                new MockResponse()
                        .setBody("{\"id\":\"u\",\"name\":\"my-col\",\"dimension\":3}"));

        try (ChromaDBClient client = createClient("")) {
            client.getCollection("my-col");

            RecordedRequest req = server.takeRequest();
            Assertions.assertTrue(
                    req.getPath()
                            .contains(
                                    "/api/v2/tenants/default_tenant/databases/default_database"));
        }
    }

    @Test
    void testCollectionNameWithSpecialCharsEncoded() throws Exception {
        server.enqueue(
                new MockResponse()
                        .setBody("{\"id\":\"u\",\"name\":\"col with spaces\",\"dimension\":3}"));

        try (ChromaDBClient client = createClient("")) {
            client.getCollection("col with spaces");

            RecordedRequest req = server.takeRequest();
            // spaces should be encoded as %20
            Assertions.assertTrue(
                    req.getPath().contains("col%20with%20spaces"),
                    "Collection name should be URL-encoded, got: " + req.getPath());
        }
    }

    // --- Retry exhaustion ---

    @Test
    void testRetryExhaustionThrowsException() {
        server.enqueue(new MockResponse().setResponseCode(503).setBody("unavailable"));
        server.enqueue(new MockResponse().setResponseCode(503).setBody("unavailable"));

        try (ChromaDBClient client = createClient("")) {
            client.setRetryPolicy(1, 0); // 1 retry = 2 total attempts
            ChromaDBConnectorException ex =
                    Assertions.assertThrows(
                            ChromaDBConnectorException.class,
                            () -> client.getCollection("col"));
            Assertions.assertTrue(
                    ex.getMessage().contains("failed after"),
                    "Should indicate retry exhaustion, got: " + ex.getMessage());
            Assertions.assertEquals(2, server.getRequestCount());
        }
    }
}
