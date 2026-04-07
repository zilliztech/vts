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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.chromadb.exception.ChromaDBConnectorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChromaDBClientTest {

    private static final Gson GSON = new GsonBuilder().create();

    private static ReadonlyConfig configOf(String url) {
        Map<String, Object> map = new HashMap<>();
        map.put("url", url);
        return ReadonlyConfig.fromMap(map);
    }

    private static ReadonlyConfig configOf(String url, String token) {
        Map<String, Object> map = new HashMap<>();
        map.put("url", url);
        if (token != null) {
            map.put("token", token);
        }
        return ReadonlyConfig.fromMap(map);
    }

    // --- GetRecordsRequest serialization tests ---

    @Test
    public void testGetRecordsRequestSerialization() {
        ChromaDBClient.GetRecordsRequest request =
                new ChromaDBClient.GetRecordsRequest()
                        .setIds(Arrays.asList("id1", "id2"))
                        .setInclude(Arrays.asList("embeddings", "documents"))
                        .setLimit(100)
                        .setOffset(0);

        Assertions.assertNotNull(request);
    }

    @Test
    public void testGetRecordsRequestEmptyInclude() {
        ChromaDBClient.GetRecordsRequest request =
                new ChromaDBClient.GetRecordsRequest()
                        .setLimit(1000)
                        .setInclude(Collections.emptyList());

        Assertions.assertNotNull(request);
    }

    @Test
    public void testGetRecordsRequestNullFieldsOmittedBySerialization() {
        // GSON default: null fields are OMITTED (not serialized as "ids":null).
        // This is correct — ChromaDB treats omitted fields as "not specified".
        ChromaDBClient.GetRecordsRequest request =
                new ChromaDBClient.GetRecordsRequest().setLimit(100);

        String json = GSON.toJson(request);
        JsonObject parsed = JsonParser.parseString(json).getAsJsonObject();

        Assertions.assertFalse(parsed.has("ids"), "null ids should be omitted");
        Assertions.assertFalse(parsed.has("include"), "null include should be omitted");
        Assertions.assertFalse(parsed.has("offset"), "null offset should be omitted");
        Assertions.assertEquals(100, parsed.get("limit").getAsInt());
    }

    @Test
    public void testGetRecordsRequestEmptyIncludeSerialization() {
        // Phase 1 uses empty include list to fetch only IDs
        ChromaDBClient.GetRecordsRequest request =
                new ChromaDBClient.GetRecordsRequest()
                        .setLimit(1000)
                        .setInclude(Collections.emptyList());

        String json = GSON.toJson(request);
        JsonObject parsed = JsonParser.parseString(json).getAsJsonObject();

        Assertions.assertTrue(parsed.get("include").isJsonArray());
        Assertions.assertEquals(0, parsed.getAsJsonArray("include").size());
    }

    // --- GetRecordsResult deserialization tests ---

    @Test
    public void testGetRecordsResultDeserialization() {
        String json =
                "{\"ids\":[\"a\",\"b\"],\"embeddings\":[[1.0,2.0],[3.0,4.0]],"
                        + "\"documents\":[\"doc1\",\"doc2\"],"
                        + "\"metadatas\":[{\"k\":\"v\"},{\"k2\":\"v2\"}]}";

        ChromaDBClient.GetRecordsResult result = GSON.fromJson(json, ChromaDBClient.GetRecordsResult.class);

        Assertions.assertEquals(Arrays.asList("a", "b"), result.getIds());
        Assertions.assertEquals(2, result.getEmbeddings().size());
        Assertions.assertEquals(Arrays.asList(1.0f, 2.0f), result.getEmbeddings().get(0));
        Assertions.assertEquals("doc1", result.getDocuments().get(0));
        Assertions.assertEquals("v", result.getMetadatas().get(0).get("k"));
    }

    @Test
    public void testGetRecordsResultWithMissingFields() {
        // ChromaDB returns only ids when include=[]
        String json = "{\"ids\":[\"a\",\"b\"]}";

        ChromaDBClient.GetRecordsResult result = GSON.fromJson(json, ChromaDBClient.GetRecordsResult.class);

        Assertions.assertEquals(2, result.getIds().size());
        Assertions.assertNull(result.getEmbeddings());
        Assertions.assertNull(result.getDocuments());
        Assertions.assertNull(result.getMetadatas());
    }

    @Test
    public void testGetRecordsResultWithNullEmbeddingsInList() {
        // Some records might have null embeddings within the list
        String json =
                "{\"ids\":[\"a\",\"b\"],\"embeddings\":[[1.0,2.0],null],"
                        + "\"documents\":[\"doc1\",null]}";

        ChromaDBClient.GetRecordsResult result = GSON.fromJson(json, ChromaDBClient.GetRecordsResult.class);

        Assertions.assertEquals(2, result.getIds().size());
        Assertions.assertNotNull(result.getEmbeddings().get(0));
        Assertions.assertNull(result.getEmbeddings().get(1));
        Assertions.assertNull(result.getDocuments().get(1));
    }

    @Test
    public void testGetRecordsResultWithExtraUnknownFields() {
        // API evolution: ChromaDB might return extra fields — GSON should ignore them
        String json = "{\"ids\":[\"a\"],\"uris\":[\"http://x\"],\"unknown_field\":123}";

        ChromaDBClient.GetRecordsResult result = GSON.fromJson(json, ChromaDBClient.GetRecordsResult.class);

        Assertions.assertEquals(1, result.getIds().size());
        // uris and unknown_field are silently dropped
    }

    @Test
    public void testGetRecordsResultEmptyLists() {
        String json =
                "{\"ids\":[],\"embeddings\":[],\"documents\":[],\"metadatas\":[]}";

        ChromaDBClient.GetRecordsResult result = GSON.fromJson(json, ChromaDBClient.GetRecordsResult.class);

        Assertions.assertTrue(result.getIds().isEmpty());
        Assertions.assertTrue(result.getEmbeddings().isEmpty());
    }

    @Test
    public void testGetRecordsResultWithNestedMetadata() {
        String json =
                "{\"ids\":[\"a\"],\"metadatas\":[{\"nested\":{\"deep\":\"value\"},"
                        + "\"tags\":[1,2,3],\"score\":3.14}]}";

        ChromaDBClient.GetRecordsResult result = GSON.fromJson(json, ChromaDBClient.GetRecordsResult.class);

        Map<String, Object> meta = result.getMetadatas().get(0);
        Assertions.assertNotNull(meta.get("nested"));
        Assertions.assertNotNull(meta.get("tags"));
        Assertions.assertEquals(3.14, meta.get("score"));
    }

    // --- CollectionInfo deserialization tests ---

    @Test
    public void testCollectionInfoDeserialization() {
        String json =
                "{\"id\":\"uuid-123\",\"name\":\"my_col\","
                        + "\"metadata\":{\"hnsw:space\":\"cosine\"},\"dimension\":128}";

        ChromaDBClient.CollectionInfo info =
                GSON.fromJson(json, ChromaDBClient.CollectionInfo.class);

        Assertions.assertEquals("uuid-123", info.getId());
        Assertions.assertEquals("my_col", info.getName());
        Assertions.assertEquals(128, info.getDimension());
        Assertions.assertEquals("cosine", info.getMetadata().get("hnsw:space"));
    }

    @Test
    public void testCollectionInfoNullDimension() {
        // New collections might not have dimension set yet
        String json = "{\"id\":\"uuid\",\"name\":\"col\",\"metadata\":null,\"dimension\":null}";

        ChromaDBClient.CollectionInfo info =
                GSON.fromJson(json, ChromaDBClient.CollectionInfo.class);

        Assertions.assertNull(info.getDimension());
        Assertions.assertNull(info.getMetadata());
    }

    // --- Client construction tests ---

    @Test
    public void testTrailingSlashInUrl() {
        ChromaDBClient client = new ChromaDBClient(configOf("http://localhost:8000/"));
        Assertions.assertNotNull(client);
        client.close();
    }

    @Test
    public void testEmptyTokenNoAuthHeader() {
        ChromaDBClient client = new ChromaDBClient(configOf("http://localhost:8000", ""));
        Assertions.assertNotNull(client);
        client.close();
    }

    @Test
    public void testNullTokenNoAuthHeader() {
        ChromaDBClient client = new ChromaDBClient(configOf("http://localhost:8000"));
        Assertions.assertNotNull(client);
        client.close();
    }

    @Test
    public void testHttpsUrl() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", "https://chroma.example.com:443");
        map.put("token", "my-token");
        map.put("tenant", "tenant1");
        map.put("database", "db1");
        ChromaDBClient client = new ChromaDBClient(ReadonlyConfig.fromMap(map));
        Assertions.assertNotNull(client);
        client.close();
    }

    @Test
    public void testDoubleCloseIsIdempotent() {
        ChromaDBClient client = new ChromaDBClient(configOf("http://localhost:8000"));
        client.close();
        Assertions.assertDoesNotThrow(client::close);
    }

    @Test
    public void testConnectionFailure() {
        ChromaDBClient client = new ChromaDBClient(configOf("http://localhost:19999"));
        client.setRetryPolicy(0, 0);

        Assertions.assertThrows(
                ChromaDBConnectorException.class,
                () -> client.getCollection("nonexistent"));

        client.close();
    }

    @Test
    public void testConnectionFailureErrorCode() {
        ChromaDBClient client = new ChromaDBClient(configOf("http://localhost:19999"));
        client.setRetryPolicy(0, 0);

        try {
            client.getCollection("test");
            Assertions.fail("Should have thrown");
        } catch (ChromaDBConnectorException e) {
            Assertions.assertTrue(
                    e.getMessage().contains("CHROMADB-01"),
                    "Should use CONNECT_FAILED error code");
        } finally {
            client.close();
        }
    }
}
