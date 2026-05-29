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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.PointInTimeResult;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;

public class EsRestClientPitResponseParserTest {

    private static EsRestClient esRestClient;

    @BeforeAll
    static void setUp() throws Exception {
        // Create an EsRestClient instance via reflection with null RestClient,
        // since parsing methods don't use the HTTP client.
        Constructor<EsRestClient> ctor =
                EsRestClient.class.getDeclaredConstructor(
                        org.elasticsearch.client.RestClient.class);
        ctor.setAccessible(true);
        esRestClient = ctor.newInstance((org.elasticsearch.client.RestClient) null);
    }

    @Test
    public void testParseResponseWithMultipleHits() throws Exception {
        String json = "{\n"
                + "  \"pit_id\": \"updated_pit_id_123\",\n"
                + "  \"_shards\": {\"total\": 1, \"successful\": 1, \"failed\": 0},\n"
                + "  \"hits\": {\n"
                + "    \"total\": {\"value\": 2, \"relation\": \"eq\"},\n"
                + "    \"hits\": [\n"
                + "      {\n"
                + "        \"_index\": \"my_index\",\n"
                + "        \"_id\": \"1\",\n"
                + "        \"_source\": {\"name\": \"alice\", \"vector\": [1.0, 2.0]},\n"
                + "        \"sort\": [100]\n"
                + "      },\n"
                + "      {\n"
                + "        \"_index\": \"my_index\",\n"
                + "        \"_id\": \"2\",\n"
                + "        \"_source\": {\"name\": \"bob\", \"vector\": [3.0, 4.0]},\n"
                + "        \"sort\": [200]\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "}";

        PointInTimeResult result = parse(json, "old_pit_id");

        // pit_id should be updated from the response
        Assertions.assertEquals("updated_pit_id_123", result.getPitId());
        Assertions.assertEquals(2, result.getDocs().size());

        // First doc: string stays plain String, array stays ArrayNode
        Map<String, Object> doc1 = result.getDocs().get(0);
        Assertions.assertEquals("my_index", doc1.get("_index"));
        Assertions.assertEquals("1", doc1.get("_id"));
        Assertions.assertEquals("alice", doc1.get("name"));
        Assertions.assertInstanceOf(String.class, doc1.get("name"));
        Assertions.assertInstanceOf(ArrayNode.class, doc1.get("vector"));

        Map<String, Object> doc2 = result.getDocs().get(1);
        Assertions.assertEquals("bob", doc2.get("name"));

        // searchAfter from the last doc's sort (single long — _shard_doc style)
        Object[] searchAfter = result.getSearchAfter();
        Assertions.assertNotNull(searchAfter);
        Assertions.assertEquals(1, searchAfter.length);
        Assertions.assertEquals(200L, searchAfter[0]);

        Assertions.assertEquals(2, result.getTotalHits());
        Assertions.assertFalse(result.getDocs().isEmpty());
    }

    @Test
    public void testParseResponseWithEmptyHits() throws Exception {
        String json = "{\n"
                + "  \"pit_id\": \"pit_123\",\n"
                + "  \"_shards\": {\"total\": 1, \"successful\": 1, \"failed\": 0},\n"
                + "  \"hits\": {\n"
                + "    \"total\": {\"value\": 0, \"relation\": \"eq\"},\n"
                + "    \"hits\": []\n"
                + "  }\n"
                + "}";

        PointInTimeResult result = parse(json, "pit_123");

        Assertions.assertEquals("pit_123", result.getPitId());
        Assertions.assertEquals(0, result.getDocs().size());
        Assertions.assertNull(result.getSearchAfter());
        Assertions.assertTrue(result.getDocs().isEmpty());
        Assertions.assertEquals(0, result.getTotalHits());
    }

    @Test
    public void testParseResponseKeepsOriginalPitIdWhenResponseMissingIt() throws Exception {
        String json = "{\n"
                + "  \"_shards\": {\"total\": 1, \"successful\": 1, \"failed\": 0},\n"
                + "  \"hits\": {\n"
                + "    \"hits\": [\n"
                + "      {\n"
                + "        \"_index\": \"idx\",\n"
                + "        \"_id\": \"1\",\n"
                + "        \"_source\": {\"field\": \"value\"},\n"
                + "        \"sort\": [1]\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "}";

        PointInTimeResult result = parse(json, "original_pit_id");

        Assertions.assertEquals("original_pit_id", result.getPitId());
        Assertions.assertEquals(1, result.getDocs().size());
        // totalHits absent (track_total_hits=false path) → -1
        Assertions.assertEquals(-1, result.getTotalHits());
    }

    @Test
    public void testParseResponseLongSortValues() throws Exception {
        // The only accepted sort value type — matches our hardcoded
        // sort=["_shard_doc"] which always returns long.
        String json = "{\n"
                + "  \"pit_id\": \"p\",\n"
                + "  \"_shards\": {\"total\": 1, \"successful\": 1, \"failed\": 0},\n"
                + "  \"hits\": {\n"
                + "    \"hits\": [\n"
                + "      {\n"
                + "        \"_index\": \"idx\",\n"
                + "        \"_id\": \"1\",\n"
                + "        \"_source\": {},\n"
                + "        \"sort\": [4611686018427387904]\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "}";

        PointInTimeResult result = parse(json, "p");
        Object[] sortValues = result.getSearchAfter();

        Assertions.assertNotNull(sortValues);
        Assertions.assertEquals(1, sortValues.length);
        Assertions.assertEquals(4611686018427387904L, sortValues[0]);
    }

    @Test
    public void testParseResponseThrowsOnFloatSortValue() {
        // Float sort value would slip by silent coercion; we explicitly reject
        // to catch accidents if someone later broadens the sort config.
        String json = "{\n"
                + "  \"pit_id\": \"p\",\n"
                + "  \"_shards\": {\"total\": 1, \"successful\": 1, \"failed\": 0},\n"
                + "  \"hits\": {\n"
                + "    \"hits\": [\n"
                + "      {\n"
                + "        \"_index\": \"idx\",\n"
                + "        \"_id\": \"1\",\n"
                + "        \"_source\": {},\n"
                + "        \"sort\": [3.14]\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "}";

        InvocationTargetException ite =
                Assertions.assertThrows(InvocationTargetException.class, () -> parse(json, "p"));
        Assertions.assertInstanceOf(ElasticsearchConnectorException.class, ite.getCause());
        Assertions.assertTrue(
                ite.getCause().getMessage().contains("Unexpected sort value"),
                "error message should explain the cause: " + ite.getCause().getMessage());
    }

    @Test
    public void testParseResponseThrowsOnStringSortValue() {
        String json = "{\n"
                + "  \"pit_id\": \"p\",\n"
                + "  \"_shards\": {\"total\": 1, \"successful\": 1, \"failed\": 0},\n"
                + "  \"hits\": {\n"
                + "    \"hits\": [\n"
                + "      {\n"
                + "        \"_index\": \"idx\",\n"
                + "        \"_id\": \"1\",\n"
                + "        \"_source\": {},\n"
                + "        \"sort\": [\"abc\"]\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "}";

        InvocationTargetException ite =
                Assertions.assertThrows(InvocationTargetException.class, () -> parse(json, "p"));
        Assertions.assertInstanceOf(ElasticsearchConnectorException.class, ite.getCause());
    }

    @Test
    public void testParseResponseThrowsOnNullSortValue() {
        String json = "{\n"
                + "  \"pit_id\": \"p\",\n"
                + "  \"_shards\": {\"total\": 1, \"successful\": 1, \"failed\": 0},\n"
                + "  \"hits\": {\n"
                + "    \"hits\": [\n"
                + "      {\n"
                + "        \"_index\": \"idx\",\n"
                + "        \"_id\": \"1\",\n"
                + "        \"_source\": {},\n"
                + "        \"sort\": [null]\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "}";

        InvocationTargetException ite =
                Assertions.assertThrows(InvocationTargetException.class, () -> parse(json, "p"));
        Assertions.assertInstanceOf(ElasticsearchConnectorException.class, ite.getCause());
    }

    @Test
    public void testParseResponseThrowsOnTimedOut() {
        String json = "{\n"
                + "  \"pit_id\": \"p\",\n"
                + "  \"timed_out\": true,\n"
                + "  \"_shards\": {\"total\": 1, \"successful\": 1, \"failed\": 0},\n"
                + "  \"hits\": {\"hits\": []}\n"
                + "}";

        InvocationTargetException ite =
                Assertions.assertThrows(InvocationTargetException.class, () -> parse(json, "p"));
        Assertions.assertInstanceOf(ElasticsearchConnectorException.class, ite.getCause());
    }

    @Test
    public void testParseResponseThrowsOnShardFailures() {
        String json = "{\n"
                + "  \"pit_id\": \"p\",\n"
                + "  \"_shards\": {\"total\": 3, \"successful\": 2, \"failed\": 1},\n"
                + "  \"hits\": {\"hits\": []}\n"
                + "}";

        InvocationTargetException ite =
                Assertions.assertThrows(InvocationTargetException.class, () -> parse(json, "p"));
        Assertions.assertInstanceOf(ElasticsearchConnectorException.class, ite.getCause());
    }

    @Test
    public void testPointInTimeResultSearchAfterIsObjectArray() {
        Object[] searchAfter = new Object[] {100L, "abc"};
        PointInTimeResult result = PointInTimeResult.builder()
                .pitId("pit1")
                .docs(new ArrayList<>())
                .totalHits(0)
                .searchAfter(searchAfter)
                .build();

        Assertions.assertNotNull(result.getSearchAfter());
        Assertions.assertInstanceOf(Object[].class, result.getSearchAfter());
        Assertions.assertEquals(100L, result.getSearchAfter()[0]);
        Assertions.assertEquals("abc", result.getSearchAfter()[1]);
    }

    private PointInTimeResult parse(String json, String pitId) throws Exception {
        InputStream stream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
        Method method =
                EsRestClient.class.getDeclaredMethod(
                        "parsePointInTimeResponse", InputStream.class, String.class);
        method.setAccessible(true);
        return (PointInTimeResult) method.invoke(esRestClient, stream, pitId);
    }
}
