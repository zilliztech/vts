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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.client;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ShardInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for the {@code Request} builders in {@link EsRestClient}.
 *
 * <p>Verifies that query parameters are set via {@link Request#addParameter(String, String)}
 * with raw unencoded values (the ES rest client encodes on send), and that no query string
 * pollutes the endpoint path. This is important because earlier code hand-concatenated
 * {@code "&preference=" + value} which would break on special characters.
 */
public class EsRestClientRequestBuilderTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // ==================== createPointInTime ====================

    @Test
    public void testCreatePitRequestBasic() {
        Request req = EsRestClient.buildCreatePointInTimeRequest("my_index", 60000L, null);

        Assertions.assertEquals("POST", req.getMethod());
        Assertions.assertEquals("/my_index/_pit", req.getEndpoint());
        // endpoint must NOT contain query string — params live in the parameters map
        Assertions.assertFalse(req.getEndpoint().contains("?"));
        Assertions.assertFalse(req.getEndpoint().contains("&"));

        Map<String, String> params = req.getParameters();
        Assertions.assertEquals("60000ms", params.get("keep_alive"));
        Assertions.assertFalse(params.containsKey("preference"));
    }

    @Test
    public void testCreatePitRequestPreservesIndexCasing() {
        // Pass-through — consistent with searchByScroll/getSearchShards/etc. ES enforces
        // lowercase itself; silently lowercasing here would mask user mistakes.
        Request req = EsRestClient.buildCreatePointInTimeRequest("My_Index", 1000L, null);
        Assertions.assertEquals("/My_Index/_pit", req.getEndpoint());
    }

    @Test
    public void testCreatePitRequestWithPreference() {
        Request req = EsRestClient.buildCreatePointInTimeRequest("idx", 5000L, "_shards:3");

        Map<String, String> params = req.getParameters();
        Assertions.assertEquals("5000ms", params.get("keep_alive"));
        // The value in the map must be the RAW string, unmodified. ES client
        // does the URL encoding when serializing the request over the wire.
        Assertions.assertEquals("_shards:3", params.get("preference"));
    }

    @Test
    public void testCreatePitRequestPreferenceWithUrlReservedChars() {
        // These characters (&, #, ?, space, +) would break a hand-concatenated
        // query string. They must end up in the parameters map as-is.
        String tricky = "user#123&foo ?bar+baz";
        Request req = EsRestClient.buildCreatePointInTimeRequest("idx", 60000L, tricky);

        // Raw value preserved in the parameters map
        Assertions.assertEquals(tricky, req.getParameters().get("preference"));
        // Endpoint stays clean — no query string leaking
        Assertions.assertEquals("/idx/_pit", req.getEndpoint());
    }

    @Test
    public void testCreatePitRequestNullPreferenceOmitsKey() {
        Request req = EsRestClient.buildCreatePointInTimeRequest("idx", 60000L, null);
        Assertions.assertFalse(req.getParameters().containsKey("preference"));
    }

    // ==================== searchByScroll ====================

    @Test
    public void testSearchByScrollRequestBasic() throws Exception {
        Map<String, Object> query = new HashMap<>();
        query.put("match_all", Collections.emptyMap());

        Request req =
                EsRestClient.buildSearchByScrollRequest(
                        "my_index",
                        Arrays.asList("field1", "field2"),
                        query,
                        "1m",
                        100,
                        null);

        Assertions.assertEquals("POST", req.getMethod());
        Assertions.assertEquals("/my_index/_search", req.getEndpoint());
        Assertions.assertFalse(req.getEndpoint().contains("?"));

        Map<String, String> params = req.getParameters();
        Assertions.assertEquals("1m", params.get("scroll"));
        Assertions.assertFalse(params.containsKey("preference"));

        JsonNode body = OBJECT_MAPPER.readTree(EntityUtils.toString(req.getEntity()));
        Assertions.assertEquals(100, body.get("size").asInt());
        Assertions.assertTrue(body.has("query"));
        Assertions.assertTrue(body.has("_source"));
        // track_total_hits=true so hits.total.value is exact (ES defaults to
        // capping at 10000 with relation=gte, which would break the reader's
        // totalProcessed reconciliation for any shard holding more than 10k docs).
        Assertions.assertTrue(body.has("track_total_hits"));
        Assertions.assertTrue(body.get("track_total_hits").asBoolean());
    }

    @Test
    public void testSearchByScrollRequestWithPreference() throws Exception {
        Map<String, Object> query = new HashMap<>();
        query.put("match_all", Collections.emptyMap());

        Request req =
                EsRestClient.buildSearchByScrollRequest(
                        "idx",
                        Collections.emptyList(),
                        query,
                        "5m",
                        500,
                        "_shards:0|_only_nodes:node-1");

        Map<String, String> params = req.getParameters();
        Assertions.assertEquals("5m", params.get("scroll"));
        // Raw preference with '|' (URL-reserved in query strings) preserved as-is
        Assertions.assertEquals("_shards:0|_only_nodes:node-1", params.get("preference"));
        Assertions.assertEquals("/idx/_search", req.getEndpoint());
    }

    @Test
    public void testSearchByScrollRequestPreferenceWithSpecialChars() {
        // A pathological preference value that would have corrupted the old
        // "&preference=" + value concatenation.
        String tricky = "a&b=c#d ?e";
        Request req =
                EsRestClient.buildSearchByScrollRequest(
                        "idx",
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        "1m",
                        10,
                        tricky);

        Assertions.assertEquals(tricky, req.getParameters().get("preference"));
        Assertions.assertEquals("/idx/_search", req.getEndpoint());
    }

    // ==================== parseSearchShardsResponse ====================

    @Test
    public void testParseSearchShardsHappyPath() throws Exception {
        String json = "{\n"
                + "  \"shards\": [\n"
                + "    [\n"
                + "      {\"state\": \"STARTED\", \"primary\": true,  \"node\": \"nodeA\", \"shard\": 0, \"index\": \"idx\"},\n"
                + "      {\"state\": \"STARTED\", \"primary\": false, \"node\": \"nodeB\", \"shard\": 0, \"index\": \"idx\"}\n"
                + "    ],\n"
                + "    [\n"
                + "      {\"state\": \"STARTED\", \"primary\": false, \"node\": \"nodeA\", \"shard\": 1, \"index\": \"idx\"},\n"
                + "      {\"state\": \"STARTED\", \"primary\": true,  \"node\": \"nodeB\", \"shard\": 1, \"index\": \"idx\"}\n"
                + "    ]\n"
                + "  ]\n"
                + "}";
        JsonNode root = OBJECT_MAPPER.readTree(json);

        List<ShardInfo> shards =
                EsRestClient.parseSearchShardsResponse(root, "/idx/_search_shards");

        Assertions.assertEquals(2, shards.size());
        Assertions.assertEquals("nodeA", shards.get(0).getNodeId());
        Assertions.assertEquals(0, shards.get(0).getShardId());
        Assertions.assertEquals("nodeB", shards.get(1).getNodeId());
        Assertions.assertEquals(1, shards.get(1).getShardId());
    }

    @Test
    public void testParseSearchShardsThrowsOnEmptyShardGroup() throws Exception {
        // Empty copy list per shard — we must NOT silently skip. Throwing
        // keeps VTS's "pure transport, fail loudly" invariant.
        String json = "{\"shards\": [ [] ]}";
        JsonNode root = OBJECT_MAPPER.readTree(json);

        ElasticsearchConnectorException ex =
                Assertions.assertThrows(
                        ElasticsearchConnectorException.class,
                        () ->
                                EsRestClient.parseSearchShardsResponse(
                                        root, "/idx/_search_shards"));
        Assertions.assertTrue(ex.getMessage().contains("zero copies"));
    }

    @Test
    public void testParseSearchShardsThrowsOnUnreadablePrimaryState() throws Exception {
        // Primary is UNASSIGNED — fail at enumeration with a clear message instead
        // of letting ES return search_phase_execution_exception at query time.
        String json = "{\n"
                + "  \"shards\": [\n"
                + "    [\n"
                + "      {\"state\": \"UNASSIGNED\", \"primary\": true, \"shard\": 0, \"index\": \"idx\"}\n"
                + "    ]\n"
                + "  ]\n"
                + "}";
        JsonNode root = OBJECT_MAPPER.readTree(json);

        ElasticsearchConnectorException ex =
                Assertions.assertThrows(
                        ElasticsearchConnectorException.class,
                        () ->
                                EsRestClient.parseSearchShardsResponse(
                                        root, "/idx/_search_shards"));
        Assertions.assertTrue(ex.getMessage().contains("UNASSIGNED"));
    }

    @Test
    public void testParseSearchShardsAcceptsRelocatingPrimary() throws Exception {
        // RELOCATING primaries can still serve reads (ES proxies through the
        // source node), so don't reject the enumeration.
        String json = "{\n"
                + "  \"shards\": [\n"
                + "    [\n"
                + "      {\"state\": \"RELOCATING\", \"primary\": true, \"node\": \"nodeA\", \"shard\": 0, \"index\": \"idx\"}\n"
                + "    ]\n"
                + "  ]\n"
                + "}";
        JsonNode root = OBJECT_MAPPER.readTree(json);

        List<ShardInfo> shards =
                EsRestClient.parseSearchShardsResponse(root, "/idx/_search_shards");
        Assertions.assertEquals(1, shards.size());
        Assertions.assertEquals("nodeA", shards.get(0).getNodeId());
    }

    @Test
    public void testParseSearchShardsAcceptsMissingState() throws Exception {
        // Older ES versions omit the "state" field. Don't reject them.
        String json = "{\n"
                + "  \"shards\": [\n"
                + "    [\n"
                + "      {\"primary\": true, \"node\": \"nodeA\", \"shard\": 0, \"index\": \"idx\"}\n"
                + "    ]\n"
                + "  ]\n"
                + "}";
        JsonNode root = OBJECT_MAPPER.readTree(json);

        List<ShardInfo> shards =
                EsRestClient.parseSearchShardsResponse(root, "/idx/_search_shards");
        Assertions.assertEquals(1, shards.size());
    }

    @Test
    public void testParseSearchShardsAcceptsMissingNode() throws Exception {
        String json = "{\n"
                + "  \"shards\": [\n"
                + "    [\n"
                + "      {\"state\": \"STARTED\", \"primary\": true, \"shard\": 0, \"index\": \"idx\"}\n"
                + "    ]\n"
                + "  ]\n"
                + "}";
        JsonNode root = OBJECT_MAPPER.readTree(json);

        List<ShardInfo> shards =
                EsRestClient.parseSearchShardsResponse(root, "/idx/_search_shards");
        Assertions.assertEquals(1, shards.size());
        Assertions.assertNull(shards.get(0).getNodeId());
    }

    @Test
    public void testParseSearchShardsFallsBackWhenNoPrimaryFlagged() throws Exception {
        // No copy flagged primary — shouldn't happen on a healthy cluster, but
        // we fall back to the first entry rather than hard-failing. Verified
        // here that the fallback yields a valid ShardInfo (log.warn is observed
        // via normal logging infra, not asserted here).
        String json = "{\n"
                + "  \"shards\": [\n"
                + "    [\n"
                + "      {\"state\": \"STARTED\", \"primary\": false, \"node\": \"nodeA\", \"shard\": 0, \"index\": \"idx\"},\n"
                + "      {\"state\": \"STARTED\", \"primary\": false, \"node\": \"nodeB\", \"shard\": 0, \"index\": \"idx\"}\n"
                + "    ]\n"
                + "  ]\n"
                + "}";
        JsonNode root = OBJECT_MAPPER.readTree(json);

        List<ShardInfo> shards =
                EsRestClient.parseSearchShardsResponse(root, "/idx/_search_shards");
        Assertions.assertEquals(1, shards.size());
        Assertions.assertEquals("nodeA", shards.get(0).getNodeId());
    }

    // ==================== extractTotalHits ====================

    @Test
    public void testExtractTotalHitsEs7ObjectForm() throws Exception {
        // ES 7+ / OpenSearch 2+: hits.total is {value, relation}
        JsonNode hitsOuter =
                OBJECT_MAPPER.readTree(
                        "{\"total\": {\"value\": 100, \"relation\": \"eq\"}}");
        Assertions.assertEquals(100L, EsRestClient.extractTotalHits(hitsOuter));
    }

    @Test
    public void testExtractTotalHitsEs6PlainNumberForm() throws Exception {
        // ES 6.x / OpenSearch 1.x: hits.total is a plain number. Without
        // defensive handling for this form, scroll mode would NPE on any
        // ES 6.x cluster.
        JsonNode hitsOuter = OBJECT_MAPPER.readTree("{\"total\": 100}");
        Assertions.assertEquals(100L, EsRestClient.extractTotalHits(hitsOuter));
    }

    @Test
    public void testExtractTotalHitsMissingField() throws Exception {
        // track_total_hits=false or older ES — total may be absent. Reader
        // treats -1 as "unknown, skip reconciliation".
        JsonNode hitsOuter = OBJECT_MAPPER.readTree("{}");
        Assertions.assertEquals(-1L, EsRestClient.extractTotalHits(hitsOuter));
    }

    @Test
    public void testExtractTotalHitsObjectMissingValue() throws Exception {
        // Malformed object form — defensive fallback to -1 rather than NPE.
        JsonNode hitsOuter = OBJECT_MAPPER.readTree("{\"total\": {\"relation\": \"eq\"}}");
        Assertions.assertEquals(-1L, EsRestClient.extractTotalHits(hitsOuter));
    }

    @Test
    public void testExtractTotalHitsUnexpectedType() throws Exception {
        // Pathological response (total as string). Return -1 rather than crash.
        JsonNode hitsOuter = OBJECT_MAPPER.readTree("{\"total\": \"lots\"}");
        Assertions.assertEquals(-1L, EsRestClient.extractTotalHits(hitsOuter));
    }
}
