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

import org.apache.http.Header;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.message.BasicHeader;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.KNN_VECTOR;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.VECTOR;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.TextNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.EsClusterConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.BulkResponse;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.ElasticsearchClusterInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.IndexDocsCount;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.PointInTimeResult;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ScrollResult;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ShardInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.util.SSLUtils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLContext;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.AGGREGATE_METRIC_DOUBLE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.ALIAS;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.DATE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.DATE_NANOS;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.DENSE_VECTOR;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.OBJECT;

@Slf4j
public class EsRestClient implements Closeable {

    private static final int CONNECTION_REQUEST_TIMEOUT = 10 * 1000;

    private static final int SOCKET_TIMEOUT = 5 * 60 * 1000;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final RestClient restClient;

    private EsRestClient(RestClient restClient) {
        this.restClient = restClient;
    }

    /**
     * Retry read-path request with exponential backoff, capped per-sleep at 5s.
     * 6 attempts total, with 500ms, 1s, 2s, 4s, 5s sleeps between them → 12.5s of
     * pure backoff. Total wall-clock depends on per-attempt failure mode: fast
     * fail (connection refused) ≈ 12.5s end-to-end; slow fail (burns the 5-minute
     * SOCKET_TIMEOUT each attempt) ≈ 30 minutes worst case.
     *
     * <p>4xx responses are <b>not</b> retried — those are logic errors (expired scroll_id,
     * bad query, auth). Retry covers only network IOException and 5xx.
     *
     * <p><b>Known trade-off — orphan PIT / scroll contexts on mid-response failure</b>:
     * For resource-creating calls ({@link #createPointInTime}, {@link #searchByScroll}),
     * if ES successfully created the resource but the response is lost in transit
     * (e.g., TCP reset mid-body, socket read timeout after ES processed the request),
     * the client sees IOException and retries, causing ES to create a second resource
     * while the first becomes an orphan that only expires when its {@code keep_alive}
     * elapses (default 1 hour).
     *
     * <p>The probability is bounded: only the "ES processed, response lost" subset
     * of IOException produces orphans — not the common "connection refused" /
     * "connect timeout" cases. In data-center networks the rate is typically
     * &lt;0.01% per call; a 1000-shard migration with this rate expects &lt;5
     * orphans total, well below ES's default {@code search.max_open_scroll_context}
     * limit of 500.
     *
     * <p>If you hit {@code "Trying to create too many scroll contexts"} errors during
     * a run (likely only on high-latency/lossy networks or with ES configured with a
     * tight context cap), lower the retry budget for {@code createPointInTime} and
     * {@code searchByScroll} specifically — the limit on those paths caps the orphan
     * ceiling at {@code (retries - 1) × shards}. The data-fetching calls
     * ({@link #doSearchWithPointInTime}, {@link #getSearchShards}, etc.) are
     * idempotent and safe to retry at full budget.
     */
    private Response performRequestWithRetry(Request request) throws IOException {
        int max = 6;
        for (int i = 1; ; i++) {
            try {
                return restClient.performRequest(request);
            } catch (ResponseException re) {
                int code = re.getResponse().getStatusLine().getStatusCode();
                if (code >= 400 && code < 500) throw re;
                if (i >= max) throw re;
                sleepForRetry(request, i, max, re);
            } catch (IOException e) {
                if (i >= max) throw e;
                sleepForRetry(request, i, max, e);
            }
        }
    }

    private void sleepForRetry(Request request, int attempt, int max, IOException cause)
            throws IOException {
        long sleep = Math.min(5000L, 500L * (1L << (attempt - 1)));
        log.warn("ES {} {} failed (attempt {}/{}), sleep {}ms then retry: {}",
                request.getMethod(), request.getEndpoint(), attempt, max, sleep, cause.getMessage());
        try {
            Thread.sleep(sleep);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            InterruptedIOException iie = new InterruptedIOException(
                    "Retry sleep interrupted after attempt " + attempt + "/" + max);
            iie.initCause(cause);
            throw iie;
        }
    }

    public static EsRestClient createInstance(ReadonlyConfig config) {
        List<String> hosts = config.get(EsClusterConnectionConfig.HOSTS);
        Optional<String> cloudId = config.getOptional(EsClusterConnectionConfig.CLOUD_ID);
        Optional<String> username = config.getOptional(EsClusterConnectionConfig.USERNAME);
        Optional<String> password = config.getOptional(EsClusterConnectionConfig.PASSWORD);
        Optional<String> apiKey = config.getOptional(EsClusterConnectionConfig.API_KEY);
        Optional<String> keystorePath = Optional.empty();
        Optional<String> keystorePassword = Optional.empty();
        Optional<String> truststorePath = Optional.empty();
        Optional<String> truststorePassword = Optional.empty();
        boolean tlsVerifyCertificate = config.get(EsClusterConnectionConfig.TLS_VERIFY_CERTIFICATE);
        if (tlsVerifyCertificate) {
            keystorePath = config.getOptional(EsClusterConnectionConfig.TLS_KEY_STORE_PATH);
            keystorePassword = config.getOptional(EsClusterConnectionConfig.TLS_KEY_STORE_PASSWORD);
            truststorePath = config.getOptional(EsClusterConnectionConfig.TLS_TRUST_STORE_PATH);
            truststorePassword =
                    config.getOptional(EsClusterConnectionConfig.TLS_TRUST_STORE_PASSWORD);
        }

        boolean tlsVerifyHostnames = config.get(EsClusterConnectionConfig.TLS_VERIFY_HOSTNAME);
        return createInstance(
                hosts,
                cloudId,
                username,
                password,
                apiKey,
                tlsVerifyCertificate,
                tlsVerifyHostnames,
                keystorePath,
                keystorePassword,
                truststorePath,
                truststorePassword);
    }

    public static EsRestClient createInstance(
            List<String> hosts,
            Optional<String> cloudId,
            Optional<String> username,
            Optional<String> password,
            Optional<String> apiKey,
            boolean tlsVerifyCertificate,
            boolean tlsVerifyHostnames,
            Optional<String> keystorePath,
            Optional<String> keystorePassword,
            Optional<String> truststorePath,
            Optional<String> truststorePassword) {
        RestClientBuilder restClientBuilder =
                getRestClientBuilder(
                        hosts,
                        cloudId,
                        username,
                        password,
                        apiKey,
                        tlsVerifyCertificate,
                        tlsVerifyHostnames,
                        keystorePath,
                        keystorePassword,
                        truststorePath,
                        truststorePassword);
        return new EsRestClient(restClientBuilder.build());
    }

    private static RestClientBuilder getRestClientBuilder(
            List<String> hosts,
            Optional<String> cloudId,
            Optional<String> username,
            Optional<String> password,
            Optional<String> apiKey,
            boolean tlsVerifyCertificate,
            boolean tlsVerifyHostnames,
            Optional<String> keystorePath,
            Optional<String> keystorePassword,
            Optional<String> truststorePath,
            Optional<String> truststorePassword) {
        RestClientBuilder restClientBuilder;
        if (StringUtils.isNotEmpty(cloudId.orElse(null))) {
            restClientBuilder = RestClient.builder(cloudId.get())
                    .setRequestConfigCallback(
                            requestConfigBuilder ->
                                    requestConfigBuilder
                                            .setConnectionRequestTimeout(
                                                    CONNECTION_REQUEST_TIMEOUT)
                                            .setSocketTimeout(SOCKET_TIMEOUT));
        } else {
            restClientBuilder = RestClient.builder(buildHttpHosts(hosts))
                    .setRequestConfigCallback(
                            requestConfigBuilder ->
                                    requestConfigBuilder
                                            .setConnectionRequestTimeout(
                                                    CONNECTION_REQUEST_TIMEOUT)
                                            .setSocketTimeout(SOCKET_TIMEOUT));
        }

        restClientBuilder.setHttpClientConfigCallback(
                httpClientBuilder -> {
                    if (username.isPresent()) {
                        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(
                                AuthScope.ANY,
                                new UsernamePasswordCredentials(username.get(), password.get()));
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    } else if (apiKey.isPresent()) {
                        Header apiKeyHeader = new BasicHeader("Authorization", "ApiKey " + apiKey.get());
                        httpClientBuilder.addInterceptorFirst((HttpRequestInterceptor) (request, context) -> {
                            request.addHeader(apiKeyHeader);
                        });
                    }

                    try {
                        if (tlsVerifyCertificate) {
                            Optional<SSLContext> sslContext =
                                    SSLUtils.buildSSLContext(
                                            keystorePath,
                                            keystorePassword,
                                            truststorePath,
                                            truststorePassword);
                            sslContext.ifPresent(httpClientBuilder::setSSLContext);
                        } else {
                            SSLContext sslContext =
                                    SSLContexts.custom()
                                            .loadTrustMaterial(new TrustAllStrategy())
                                            .build();
                            httpClientBuilder.setSSLContext(sslContext);
                        }
                        if (!tlsVerifyHostnames) {
                            httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return httpClientBuilder;
                });
        return restClientBuilder;
    }

    private static HttpHost[] buildHttpHosts(List<String> hosts) {
        if(hosts == null || hosts.isEmpty()){
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.FAILED_CONNECT_ES,
                    "es hosts is empty");
        }
        HttpHost[] httpHosts = new HttpHost[hosts.size()];
        for (int i = 0; i < hosts.size(); i++) {
            httpHosts[i] = HttpHost.create(hosts.get(i));
        }
        return httpHosts;
    }

    public BulkResponse bulk(String requestBody) {
        Request request = new Request("POST", "/_bulk");
        request.setJsonEntity(requestBody);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                        "bulk es Response is null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                JsonNode json = OBJECT_MAPPER.readTree(entity);
                int took = json.get("took").asInt();
                boolean errors = json.get("errors").asBoolean();
                return new BulkResponse(errors, took, entity);
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                        String.format(
                                "bulk es response status code=%d,request body=%s",
                                response.getStatusLine().getStatusCode(), requestBody));
            }
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                    String.format("bulk es error,request body=%s", requestBody),
                    e);
        }
    }

    public ElasticsearchClusterInfo getClusterInfo() {
        Request request = new Request("GET", "/");
        try {
            Response response = restClient.performRequest(request);
            String result = EntityUtils.toString(response.getEntity());
            JsonNode jsonNode = OBJECT_MAPPER.readTree(result);
            JsonNode versionNode = jsonNode.get("version");
            return ElasticsearchClusterInfo.builder()
                    .clusterVersion(versionNode.get("number").asText())
                    .distribution(
                            Optional.ofNullable(versionNode.get("distribution"))
                                    .map(JsonNode::asText)
                                    .orElse(null))
                    .build();
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.GET_ES_VERSION_FAILED,
                    "fail to get elasticsearch version.",
                    e);
        }
    }

    public List<ShardInfo> getSearchShards(String index) {
        String endpoint = "/" + index + "/_search_shards";
        Request request = new Request("GET", endpoint);
        try {
            Response response = performRequestWithRetry(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SEARCH_SHARDS_FAILED,
                        "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                JsonNode root = OBJECT_MAPPER.readTree(entity);
                return parseSearchShardsResponse(root, endpoint);
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SEARCH_SHARDS_FAILED,
                        String.format(
                                "GET %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.SEARCH_SHARDS_FAILED,
                    String.format("GET %s error", endpoint),
                    e);
        }
    }

    /**
     * Parse a {@code _search_shards} response into {@link ShardInfo} per shard.
     * Package-private for unit testing.
     *
     * <p>Fails loudly on pathological shapes — empty copy list, unreadable state,
     * missing required fields — rather than skipping; as a data transport tool VTS
     * must never silently drop a shard's worth of data.
     */
    static List<ShardInfo> parseSearchShardsResponse(JsonNode root, String endpoint) {
        JsonNode shardsArray = root.get("shards");
        if (shardsArray == null || !shardsArray.isArray()) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.SEARCH_SHARDS_FAILED,
                    "GET " + endpoint + " response missing 'shards' array");
        }
        List<ShardInfo> result = new ArrayList<>();
        for (JsonNode shardGroup : shardsArray) {
            if (!shardGroup.isArray() || shardGroup.size() == 0) {
                // ES returned no copies for this shard. Not a silent-skip case —
                // we don't know the shard index/id to name it, so the user has to
                // check the raw response. Data-integrity > best effort.
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SEARCH_SHARDS_FAILED,
                        String.format(
                                "GET %s: a shard group reported zero copies. "
                                        + "The cluster may be red or the shard is unassigned — "
                                        + "fix shard allocation before retrying.",
                                endpoint));
            }
            // Pick the primary copy by flag — order isn't guaranteed by ES.
            JsonNode primary = null;
            for (JsonNode copy : shardGroup) {
                JsonNode primaryFlag = copy.get("primary");
                if (primaryFlag != null && primaryFlag.asBoolean()) {
                    primary = copy;
                    break;
                }
            }
            if (primary == null) {
                // Healthy ES always flags exactly one copy as primary. If we hit
                // this branch the cluster is in an unusual state — log loudly so
                // ops can investigate, then fall back to the first entry to keep
                // the job moving.
                log.warn(
                        "GET {}: no copy in shard group flagged primary; "
                                + "falling back to first entry. group={}",
                        endpoint, shardGroup);
                primary = shardGroup.get(0);
            }
            // Soft state check: ES _search_shards returns a "state" per copy
            // (STARTED, RELOCATING, INITIALIZING, UNASSIGNED). Only STARTED and
            // RELOCATING can actually serve reads (the latter via proxying). If
            // state is reported and is neither, fail early with a clear message
            // rather than opening a scroll/PIT that ES will reject anyway.
            // "state" may be absent on older ES versions — skip the check then.
            String state = primary.path("state").asText("");
            if (!state.isEmpty()
                    && !"STARTED".equals(state)
                    && !"RELOCATING".equals(state)) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SEARCH_SHARDS_FAILED,
                        String.format(
                                "GET %s: shard %s/%s state=%s is not readable "
                                        + "(need STARTED or RELOCATING). "
                                        + "Fix shard allocation before retrying.",
                                endpoint,
                                primary.path("index").asText("?"),
                                primary.path("shard").asText("?"),
                                state));
            }
            String concreteIndex = primary.get("index").asText();
            int shardId = primary.get("shard").asInt();
            String nodeId = primary.path("node").asText(null);
            result.add(new ShardInfo(concreteIndex, shardId, nodeId));
        }
        return result;
    }

    public Map<String, String> getNodesHttpAddresses() {
        String endpoint = "/_nodes/http";
        Request request = new Request("GET", endpoint);
        try {
            Response response = performRequestWithRetry(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.GET_NODE_HTTP_ADDRESSES_FAILED,
                        "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                JsonNode root = OBJECT_MAPPER.readTree(entity);
                JsonNode nodesNode = root.get("nodes");
                Map<String, String> nodeAddresses = new HashMap<>();
                if (nodesNode == null || !nodesNode.isObject()) {
                    return nodeAddresses;
                }
                Iterator<Map.Entry<String, JsonNode>> fields = nodesNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    String nodeId = entry.getKey();
                    JsonNode nodeInfo = entry.getValue();
                    JsonNode httpNode = nodeInfo.get("http");
                    if (httpNode != null && httpNode.has("publish_address")) {
                        String publishAddress = httpNode.get("publish_address").asText();
                        // publish_address is either "hostname/ip:port" (ES attaches the
                        // reverse-resolved hostname before the bind address) or plain
                        // "ip:port" when no hostname is available. Take the portion
                        // after the slash — that's always the bindable address.
                        int slashIndex = publishAddress.indexOf('/');
                        if (slashIndex >= 0) {
                            publishAddress = publishAddress.substring(slashIndex + 1);
                        }
                        nodeAddresses.put(nodeId, publishAddress);
                    }
                }
                return nodeAddresses;
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.GET_NODE_HTTP_ADDRESSES_FAILED,
                        String.format(
                                "GET %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.GET_NODE_HTTP_ADDRESSES_FAILED,
                    String.format("GET %s error", endpoint),
                    e);
        }
    }

    @Override
    public void close() {
        try {
            restClient.close();
        } catch (IOException e) {
            log.warn("close elasticsearch connection error", e);
        }
    }

    /**
     * first time to request search documents by scroll call /${index}/_search?scroll=${scroll}
     *
     * @param index index name
     * @param source select fields
     * @param scrollTime such as:1m
     * @param scrollSize fetch documents count in one request
     */
    public ScrollResult searchByScroll(
            String index,
            List<String> source,
            Map<String, Object> query,
            String scrollTime,
            int scrollSize) {
        return searchByScroll(index, source, query, scrollTime, scrollSize, null);
    }

    public ScrollResult searchByScroll(
            String index,
            List<String> source,
            Map<String, Object> query,
            String scrollTime,
            int scrollSize,
            String preference) {
        // allowRetry=true: creating a new scroll context is idempotent — if
        // the request fails, nothing is consumed on ES, retrying just opens
        // another scroll context.
        return getDocsFromScrollRequest(
                buildSearchByScrollRequest(
                        index, source, query, scrollTime, scrollSize, preference),
                /* allowRetry */ true);
    }

    // Package-private for unit tests: verifies endpoint/method/params assembly
    // without issuing an HTTP call.
    static Request buildSearchByScrollRequest(
            String index,
            List<String> source,
            Map<String, Object> query,
            String scrollTime,
            int scrollSize,
            String preference) {
        Map<String, Object> param = new HashMap<>();
        param.put("query", query);
        param.put("_source", source);
        param.put("sort", new String[] {"_doc"});
        param.put("size", scrollSize);
        // Force exact count. ES defaults to capping hits.total.value at 10000 with
        // relation="gte"; with that cap the reader's totalProcessed == totalHits
        // reconciliation falsely fires for any shard holding more than 10k matches.
        // Scroll carries this setting forward on all subsequent scroll calls, so
        // it's a one-time cost per split.
        //
        // Cost trade-off: exact counting forces ES to walk the full matching set
        // once. For match_all on a huge index this adds visible latency on the
        // first request. For VTS (a data-transport tool) the totalProcessed vs
        // totalHits reconciliation is worth that cost — detecting a lost batch
        // is more important than shaving seconds off the first batch.
        param.put("track_total_hits", true);
        Request request = new Request("POST", "/" + index + "/_search");
        request.addParameter("scroll", scrollTime);
        if (preference != null) {
            request.addParameter("preference", preference);
        }
        request.setJsonEntity(JsonUtils.toJsonString(param));
        return request;
    }

    /**
     * scroll to get result call _search/scroll
     *
     * @param scrollId the scroll id of the last request
     * @param scrollTime such as:1m
     */
    public ScrollResult searchWithScrollId(String scrollId, String scrollTime) {
        Map<String, String> param = new HashMap<>();
        param.put("scroll_id", scrollId);
        param.put("scroll", scrollTime);
        Request request = new Request("POST", "/_search/scroll");
        request.setJsonEntity(JsonUtils.toJsonString(param));
        // allowRetry=false: advancing a scroll cursor is NOT idempotent. If
        // the response is lost mid-flight, ES has already moved its cursor
        // past this batch; a retry would silently return the NEXT batch,
        // skipping a batch of data. Let the reader's totalProcessed vs
        // totalHits reconciliation catch that failure mode by letting the
        // IOException surface and the split fail — re-scanning the whole
        // shard from a fresh scroll is the only safe recovery path. PIT +
        // search_after is idempotent and uses retry; scroll continuation
        // deliberately does not.
        return getDocsFromScrollRequest(request, /* allowRetry */ false);
    }

    private ScrollResult getDocsFromScrollRequest(Request request, boolean allowRetry) {
        String endpoint = request.getEndpoint();
        try {
            Response response =
                    allowRetry ? performRequestWithRetry(request) : restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                        "POST " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                ObjectNode responseJson = JsonUtils.parseObject(entity);

                JsonNode timedOutNode = responseJson.get("timed_out");
                if (timedOutNode != null && timedOutNode.asBoolean()) {
                    throw new ElasticsearchConnectorException(
                            ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                            String.format(
                                    "POST %s scroll search timed out, results may be incomplete",
                                    endpoint));
                }

                JsonNode shards =
                        requireField(
                                responseJson,
                                "_shards",
                                "scroll response at " + endpoint,
                                ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR);
                int totalShards = shards.get("total").intValue();
                int successful = shards.get("successful").intValue();
                if (totalShards != successful) {
                    throw new ElasticsearchConnectorException(
                            ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                            String.format(
                                    "POST %s scroll has shard failures: total(%d) != successful(%d)",
                                    endpoint, totalShards, successful));
                }

                return getDocsFromScrollResponse(responseJson, endpoint);
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                        String.format(
                                "POST %s response status code=%d,request body=%s",
                                endpoint,
                                response.getStatusLine().getStatusCode(),
                                readRequestBody(request)));
            }
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                    String.format(
                            "POST %s error,request body=%s", endpoint, readRequestBody(request)),
                    e);
        }
    }

    /**
     * Read a request's JSON body for inclusion in error messages. Entities set via
     * {@link Request#setJsonEntity(String)} are backed by repeatable in-memory buffers,
     * so this is safe to call after the request has been sent. Returns {@code ""} on
     * any failure — body is only for diagnostics, never critical.
     */
    private static String readRequestBody(Request request) {
        try {
            return request.getEntity() == null
                    ? ""
                    : EntityUtils.toString(request.getEntity());
        } catch (IOException e) {
            return "<unavailable: " + e.getMessage() + ">";
        }
    }

    private ScrollResult getDocsFromScrollResponse(ObjectNode responseJson, String endpoint) {
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setScrollId(
                requireField(
                                responseJson,
                                "_scroll_id",
                                "scroll response at " + endpoint,
                                ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR)
                        .asText());

        JsonNode hitsOuter =
                requireField(
                        responseJson,
                        "hits",
                        "scroll response at " + endpoint,
                        ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR);
        scrollResult.setTotalHits(extractTotalHits(hitsOuter));

        JsonNode hitsNode = hitsOuter.get("hits");
        List<Map<String, Object>> docs = new ArrayList<>(hitsNode.size());
        scrollResult.setDocs(docs);

        for (JsonNode jsonNode : hitsNode) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("_index", jsonNode.get("_index").textValue());
            doc.put("_id", jsonNode.get("_id").textValue());
            JsonNode source = jsonNode.get("_source");
            for (Iterator<Map.Entry<String, JsonNode>> iterator = source.fields();
                    iterator.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = iterator.next();
                String fieldName = entry.getKey();
                if (entry.getValue() instanceof TextNode) {
                    doc.put(fieldName, entry.getValue().textValue());
                } else {
                    doc.put(fieldName, entry.getValue());
                }
            }
            docs.add(doc);
        }
        return scrollResult;
    }

    /**
     * Extract {@code hits.total} across the two ES response schemas:
     * <ul>
     *   <li>ES 7+ / OpenSearch 2+: {@code "total": {"value": N, "relation": "eq"}}
     *       — object form with a {@code value} subfield.</li>
     *   <li>ES 6.x / OpenSearch 1.x: {@code "total": N} — plain number.</li>
     * </ul>
     * Returns {@code -1} if the field is missing (e.g., {@code track_total_hits=false})
     * or in any unexpected shape (defensive — we'd rather disable the reader's
     * reconciliation check than NPE on a mangled response).
     *
     * <p>Package-private for direct unit testing.
     */
    static long extractTotalHits(JsonNode hitsOuter) {
        JsonNode totalNode = hitsOuter.get("total");
        if (totalNode == null || totalNode.isMissingNode() || totalNode.isNull()) {
            return -1;
        }
        if (totalNode.isObject()) {
            JsonNode valueNode = totalNode.get("value");
            if (valueNode != null && valueNode.canConvertToLong()) {
                return valueNode.asLong();
            }
            return -1;
        }
        // ES 6.x / OpenSearch 1.x: plain number form.
        return totalNode.canConvertToLong() ? totalNode.asLong() : -1;
    }

    /**
     * Defensive check for response fields that ES's contract guarantees on a 200
     * OK but which a broken proxy / WAF / future ES version change could strip.
     * Throws a structured connector exception instead of NPE — much easier to
     * triage when the response has been mangled by infrastructure between VTS
     * and ES.
     */
    private static JsonNode requireField(
            JsonNode parent,
            String field,
            String errorContext,
            ElasticsearchConnectorErrorCode errorCode) {
        JsonNode node = parent.get(field);
        if (node == null || node.isMissingNode()) {
            throw new ElasticsearchConnectorException(
                    errorCode,
                    String.format(
                            "Response missing required field '%s' in %s", field, errorContext));
        }
        return node;
    }

    /**
     * Instead of the getIndexDocsCount method to determine if the index exists,
     *
     * <p>
     *
     * <p>getIndexDocsCount throws an exception if the index does not exist
     *
     * <p>
     *
     * @param index index
     * @return true or false
     */
    public boolean checkIndexExist(String index) {
        Request request = new Request("HEAD", "/" + index);
        try {
            Response response = restClient.performRequest(request);
            int statusCode = response.getStatusLine().getStatusCode();
            return statusCode == 200;
        } catch (Exception ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.CHECK_INDEX_FAILED, ex);
        }
    }

    public List<IndexDocsCount> getIndexDocsCount(String index) {
        String endpoint = String.format("/_cat/indices/%s?h=index,docsCount&format=json", index);
        Request request = new Request("GET", endpoint);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                return JsonUtils.toList(entity, IndexDocsCount.class);
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        String.format(
                                "GET %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED, ex);
        }
    }

    public List<String> listIndex() {
        String endpoint = "/_cat/indices?format=json";
        Request request = new Request("GET", endpoint);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.LIST_INDEX_FAILED,
                        "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                return JsonUtils.toList(entity, Map.class).stream()
                        .map(map -> map.get("index").toString())
                        .collect(Collectors.toList());
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.LIST_INDEX_FAILED,
                        String.format(
                                "GET %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.LIST_INDEX_FAILED, ex);
        }
    }

    public void createIndex(String indexName) {
        createIndex(indexName, null);
    }

    public void createIndex(String indexName, String mapping) {
        String endpoint = String.format("/%s", indexName);
        Request request = new Request("PUT", endpoint);
        if (StringUtils.isNotEmpty(mapping)) {
            request.setJsonEntity(mapping);
        }
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.CREATE_INDEX_FAILED,
                        "PUT " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.CREATE_INDEX_FAILED,
                        String.format(
                                "PUT %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.CREATE_INDEX_FAILED, ex);
        }
    }

    public void dropIndex(String tableName) {
        String endpoint = String.format("/%s", tableName);
        Request request = new Request("DELETE", endpoint);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.DROP_INDEX_FAILED,
                        "DELETE " + endpoint + " response null");
            }
            // todo: if the index doesn't exist, the response status code is 200?
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                return;
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.DROP_INDEX_FAILED,
                        String.format(
                                "DELETE %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.DROP_INDEX_FAILED, ex);
        }
    }

    public void clearIndexData(String indexName) {
        String endpoint = String.format("/%s/_delete_by_query", indexName);
        Request request = new Request("POST", endpoint);
        String jsonString = "{ \"query\": { \"match_all\": {} } }";
        request.setJsonEntity(jsonString);

        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.CLEAR_INDEX_DATA_FAILED,
                        "POST " + endpoint + " response null");
            }
            // todo: if the index doesn't exist, the response status code is 200?
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                return;
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.CLEAR_INDEX_DATA_FAILED,
                        String.format(
                                "POST %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.CLEAR_INDEX_DATA_FAILED, ex);
        }
    }

    /**
     * get es field name and type mapping realtion
     *
     * @param index index name
     * @return {key-> field name,value->es type}
     */
    public Map<String, BasicTypeDefine<EsType>> getFieldTypeMapping(
            String index, List<String> source) {
        String endpoint = String.format("/%s/_mappings", index);
        Request request = new Request("GET", endpoint);
        Map<String, BasicTypeDefine<EsType>> mapping = new HashMap<>();
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        String.format(
                                "GET %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
            String entity = EntityUtils.toString(response.getEntity());
            log.info(String.format("GET %s respnse=%s", endpoint, entity));
            ObjectNode responseJson = JsonUtils.parseObject(entity);
            for (Iterator<JsonNode> it = responseJson.elements(); it.hasNext(); ) {
                JsonNode indexProperty = it.next();
                JsonNode mappingsProperty = indexProperty.get("mappings");
                if (mappingsProperty.has("mappingsProperty")) {
                    JsonNode properties = mappingsProperty.get("properties");
                    mapping = getFieldTypeMappingFromProperties(properties, source);
                } else {
                    for (JsonNode typeNode : mappingsProperty) {
                        JsonNode properties;
                        if (typeNode.has("properties")) {
                            properties = typeNode.get("properties");
                        } else {
                            properties = typeNode;
                        }
                        mapping.putAll(getFieldTypeMappingFromProperties(properties, source));
                    }
                }
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED, ex);
        }
        return mapping;
    }

    private static Map<String, BasicTypeDefine<EsType>> getFieldTypeMappingFromProperties(
            JsonNode properties, List<String> source) {
        Map<String, BasicTypeDefine<EsType>> allElasticSearchFieldTypeInfoMap = new HashMap<>();
        properties
                .fields()
                .forEachRemaining(
                        entry -> {
                            String fieldName = entry.getKey();
                            JsonNode fieldProperty = entry.getValue();
                            if (fieldProperty.has("type")) {
                                String type = fieldProperty.get("type").asText();
                                BasicTypeDefine.BasicTypeDefineBuilder<EsType> typeDefine =
                                        BasicTypeDefine.<EsType>builder()
                                                .name(fieldName)
                                                .columnType(type)
                                                .dataType(type);
                                if (type.equalsIgnoreCase(AGGREGATE_METRIC_DOUBLE)) {
                                    ArrayNode metrics = ((ArrayNode) fieldProperty.get("metrics"));
                                    List<String> metricsList = new ArrayList<>();
                                    for (JsonNode node : metrics) {
                                        metricsList.add(node.asText());
                                    }
                                    Map<String, Object> options = new HashMap<>();
                                    options.put("metrics", metricsList);
                                    typeDefine.nativeType(new EsType(type, options));
                                } else if (type.equalsIgnoreCase(ALIAS)) {
                                    String path = fieldProperty.get("path").asText();
                                    Map<String, Object> options = new HashMap<>();
                                    options.put("path", path);
                                    typeDefine.nativeType(new EsType(type, options));
                                } else if (type.equalsIgnoreCase(DENSE_VECTOR)) {
                                    String elementType =
                                            fieldProperty.get("element_type") == null
                                                    ? "float"
                                                    : fieldProperty.get("element_type").asText();
                                    Map<String, Object> options = new HashMap<>();
                                    options.put("element_type", elementType);
                                    typeDefine.nativeType(new EsType(type, options));

                                    // es dense_vector dim
                                    typeDefine.scale(fieldProperty.get("dims").asInt());

                                } else if (type.equalsIgnoreCase(VECTOR)) {
                                    //adapt for huawei cloud elastic search
                                    String elementType =
                                            fieldProperty.get("dim_type") == null
                                                    ? "float"
                                                    : fieldProperty.get("dim_type").asText();
                                    Map<String, Object> options = new HashMap<>();
                                    options.put("element_type", elementType);
                                    typeDefine.nativeType(new EsType(type, options));

                                    // es dense_vector dim
                                    typeDefine.scale(fieldProperty.get("dimension").asInt());

                                } else if (type.equalsIgnoreCase(KNN_VECTOR)) {
                                //adapt for huawei cloud elastic search
                                String elementType =
                                        fieldProperty.get("data_type") == null
                                                ? "float"
                                                : fieldProperty.get("data_type").asText();
                                Map<String, Object> options = new HashMap<>();
                                options.put("element_type", elementType);
                                typeDefine.nativeType(new EsType(type, options));

                                // es dense_vector dim
                                typeDefine.scale(fieldProperty.get("dimension").asInt());

                                } else if (type.equalsIgnoreCase(DATE)
                                        || type.equalsIgnoreCase(DATE_NANOS)) {
                                    String format =
                                            fieldProperty.get("format") != null
                                                    ? fieldProperty.get("format").asText()
                                                    : "strict_date_optional_time_nanos||epoch_millis";
                                    Map<String, Object> options = new HashMap<>();
                                    options.put("format", format);
                                    typeDefine.nativeType(new EsType(type, options));
                                } else {
                                    typeDefine.nativeType(new EsType(type, new HashMap<>()));
                                }
                                allElasticSearchFieldTypeInfoMap.put(fieldName, typeDefine.build());
                            } else if (fieldProperty.has("properties")) {
                                // it should be object type
                                JsonNode propertiesNode = fieldProperty.get("properties");
                                List<String> fields = new ArrayList<>();
                                propertiesNode.fieldNames().forEachRemaining(fields::add);
                                Map<String, BasicTypeDefine<EsType>> subFieldTypeInfoMap =
                                        getFieldTypeMappingFromProperties(propertiesNode, fields);
                                BasicTypeDefine.BasicTypeDefineBuilder<EsType> typeDefine =
                                        BasicTypeDefine.<EsType>builder()
                                                .name(fieldName)
                                                .columnType(OBJECT)
                                                .dataType(OBJECT);
                                typeDefine.nativeType(
                                        new EsType(OBJECT, (Map) subFieldTypeInfoMap));
                                allElasticSearchFieldTypeInfoMap.put(fieldName, typeDefine.build());
                            }
                        });
        if (CollectionUtils.isEmpty(source)) {
            return allElasticSearchFieldTypeInfoMap;
        }

        allElasticSearchFieldTypeInfoMap.forEach(
                (fieldName, fieldType) -> {
                    if (fieldType.getDataType().equalsIgnoreCase(ALIAS)) {
                        BasicTypeDefine<EsType> type =
                                allElasticSearchFieldTypeInfoMap.get(
                                        fieldType.getNativeType().getOptions().get("path"));
                        if (type != null) {
                            allElasticSearchFieldTypeInfoMap.put(fieldName, type);
                        }
                    }
                });

        return source.stream()
                .collect(
                        Collectors.toMap(
                                Function.identity(),
                                fieldName -> {
                                    BasicTypeDefine<EsType> fieldType =
                                            allElasticSearchFieldTypeInfoMap.get(fieldName);
                                    if (fieldType != null) {
                                        return fieldType;
                                    }
                                    if (fieldName.equals("_id")) {
                                        return BasicTypeDefine.<EsType>builder()
                                                .name(fieldName)
                                                .columnType("string")
                                                .dataType("string")
                                                .build();
                                    }
                                    log.warn(
                                            "fail to get elasticsearch field {} mapping type,so give a default type text",
                                            fieldName);
                                    return BasicTypeDefine.<EsType>builder()
                                            .name(fieldName)
                                            .columnType("text")
                                            .dataType("text")
                                            .build();
                                }));
    }

    public boolean clearScroll(String scrollId) {
        if (StringUtils.isEmpty(scrollId)) {
            log.warn("Attempted to clear scroll with empty scroll ID");
            return false;
        }

        String endpoint = "/_search/scroll";
        Request request = new Request("DELETE", endpoint);
        Map<String, String> requestBody = new HashMap<>();
        requestBody.put("scroll_id", scrollId);
        request.setJsonEntity(JsonUtils.toJsonString(requestBody));
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                log.warn("DELETE {} response null for scroll ID: {}", endpoint, scrollId);
                return false;
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                ObjectNode responseJson = JsonUtils.parseObject(entity);
                return responseJson.get("succeeded").asBoolean();
            } else {
                log.warn(
                        "DELETE {} response status code={} for scroll ID: {}",
                        endpoint,
                        response.getStatusLine().getStatusCode(),
                        scrollId);
                return false;
            }
        } catch (Exception ex) {
            log.warn("Failed to clear scroll ID: " + scrollId, ex);
            return false;
        }
    }

    public String createPointInTime(String index, long keepAlive, String preference) {
        Request request = buildCreatePointInTimeRequest(index, keepAlive, preference);
        String endpoint = request.getEndpoint();
        try {
            Response response = performRequestWithRetry(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.CREATE_PIT_FAILED,
                        "POST " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                ObjectNode responseJson = JsonUtils.parseObject(entity);
                return responseJson.get("id").asText();
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.CREATE_PIT_FAILED,
                        String.format(
                                "POST %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.CREATE_PIT_FAILED,
                    String.format("POST %s error", endpoint),
                    e);
        }
    }

    // Package-private for unit tests: verifies endpoint/method/params assembly
    // without issuing an HTTP call.
    static Request buildCreatePointInTimeRequest(String index, long keepAlive, String preference) {
        // Don't lowercase the index name — other methods (searchByScroll, getSearchShards,
        // checkIndexExist, getFieldTypeMapping) all pass it through as-is, and ES enforces
        // lowercase itself. Silently lowercasing would hide user mistakes and, if a
        // mixed-case index somehow exists, route PIT and scroll to different indices.
        Request request = new Request("POST", "/" + index + "/_pit");
        request.addParameter("keep_alive", keepAlive + "ms");
        if (preference != null) {
            request.addParameter("preference", preference);
        }
        return request;
    }

    /**
     * Best-effort PIT cleanup — mirrors {@link #clearScroll}. On any failure
     * (network, 4xx, 5xx, parsing) logs a warning and returns {@code false};
     * never throws. PITs auto-expire on ES after their keep_alive anyway,
     * so a failed delete is a slow-path leak, not a correctness issue.
     */
    public boolean deletePointInTime(String pitId) {
        if (StringUtils.isEmpty(pitId)) {
            log.warn("Attempted to delete PIT with empty ID");
            return false;
        }
        String endpoint = "/_pit";
        Request request = new Request("DELETE", endpoint);
        Map<String, String> requestBody = new HashMap<>();
        requestBody.put("id", pitId);
        request.setJsonEntity(JsonUtils.toJsonString(requestBody));
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                log.warn("DELETE {} response null for PIT ID: {}", endpoint, pitId);
                return false;
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                ObjectNode responseJson = JsonUtils.parseObject(entity);
                return responseJson.get("succeeded").asBoolean();
            } else {
                log.warn(
                        "DELETE {} response status code={} for PIT ID: {}",
                        endpoint,
                        response.getStatusLine().getStatusCode(),
                        pitId);
                return false;
            }
        } catch (Exception ex) {
            log.warn("Failed to delete PIT with ID: " + pitId, ex);
            return false;
        }
    }

    /**
     * Execute a single PIT search. Self-contained: builds the request body from
     * the per-call arguments, sends it, parses the response.
     *
     * <p>Notes on specific fields:
     * <ul>
     *   <li>{@code trackTotalHits}: pass {@code true} on the first call of a scan so
     *       ES returns an exact {@code hits.total.value} (caller uses it for the
     *       totalProcessed reconciliation); pass {@code false} on subsequent calls
     *       to avoid the exact-count overhead.</li>
     *   <li>{@code searchAfter}: {@code null} (or empty) on the first call;
     *       the {@code searchAfter} from the previous response on subsequent calls.</li>
     *   <li>Sort key is hardcoded to {@code ["_shard_doc"]} — a PIT-specific
     *       tiebreaker ES guarantees is unique-per-doc and cheap to sort on.
     *       Do not change without also updating {@link #parsePointInTimeResponse}
     *       (it only accepts long sort values).</li>
     * </ul>
     *
     * <p>ES does NOT allow {@code preference} on {@code _search} with PIT;
     * bind the PIT to specific shard(s) via {@link #createPointInTime} instead.
     */
    public PointInTimeResult doSearchWithPointInTime(
            List<String> source,
            Map<String, Object> query,
            int batchSize,
            String pitId,
            Object[] searchAfter,
            long keepAlive,
            boolean trackTotalHits) {
        ObjectNode body = JsonUtils.createObjectNode();
        body.put("size", batchSize);
        body.set("query", JsonUtils.toJsonNode(query));
        body.set("_source", JsonUtils.toJsonNode(source));
        body.put("track_total_hits", trackTotalHits);

        ObjectNode pit = JsonUtils.createObjectNode();
        pit.put("id", pitId);
        pit.put("keep_alive", keepAlive + "ms");
        body.set("pit", pit);

        if (searchAfter != null && searchAfter.length > 0) {
            body.set("search_after", JsonUtils.toJsonNode(searchAfter));
        }

        ArrayNode sort = JsonUtils.createArrayNode();
        sort.add("_shard_doc");
        body.set("sort", sort);

        Request request = new Request("POST", "/_search");
        request.setJsonEntity(body.toString());
        try {
            Response response = performRequestWithRetry(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SEARCH_WITH_PIT_FAILED,
                        "POST /_search with PIT response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                try (InputStream stream = response.getEntity().getContent()) {
                    return parsePointInTimeResponse(stream, pitId);
                }
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SEARCH_WITH_PIT_FAILED,
                        String.format(
                                "POST /_search with PIT response status code=%d",
                                response.getStatusLine().getStatusCode()));
            }
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.SEARCH_WITH_PIT_FAILED,
                    "POST /_search with PIT error",
                    e);
        }
    }

    /**
     * Parse a PIT search response from an InputStream, avoiding
     * the full String allocation from EntityUtils.toString().
     */
    private PointInTimeResult parsePointInTimeResponse(
            InputStream inputStream, String pitId) throws IOException {
        ObjectNode responseJson = (ObjectNode) OBJECT_MAPPER.readTree(inputStream);

        JsonNode timedOutNode = responseJson.get("timed_out");
        if (timedOutNode != null && timedOutNode.asBoolean()) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.SEARCH_WITH_PIT_FAILED,
                    "PIT search timed out, results may be incomplete");
        }

        JsonNode shards =
                requireField(
                        responseJson,
                        "_shards",
                        "PIT search response",
                        ElasticsearchConnectorErrorCode.SEARCH_WITH_PIT_FAILED);
        int totalShards = shards.get("total").intValue();
        int successfulShards = shards.get("successful").intValue();
        if (totalShards != successfulShards) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.SEARCH_WITH_PIT_FAILED,
                    String.format(
                            "PIT search has shard failures: total shards(%d) != successful shards(%d)",
                            totalShards, successfulShards));
        }

        JsonNode pitIdNode = responseJson.get("pit_id");
        if (pitIdNode != null) {
            pitId = pitIdNode.asText();
        }

        JsonNode hitsOuterNode =
                requireField(
                        responseJson,
                        "hits",
                        "PIT search response",
                        ElasticsearchConnectorErrorCode.SEARCH_WITH_PIT_FAILED);
        // hits.total may be absent when track_total_hits is false.
        long totalHits = extractTotalHits(hitsOuterNode);

        // Parse hits — same shape as getDocsFromScrollResponse, inlined here
        // because PIT has a different return type (PointInTimeResult, not
        // ScrollResult) and needs the last hit's sort values for search_after.
        JsonNode hitsNode = hitsOuterNode.get("hits");
        List<Map<String, Object>> docs = new ArrayList<>(hitsNode.size());

        for (JsonNode jsonNode : hitsNode) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("_index", jsonNode.get("_index").textValue());
            doc.put("_id", jsonNode.get("_id").textValue());
            JsonNode source = jsonNode.get("_source");
            for (Iterator<Map.Entry<String, JsonNode>> iterator = source.fields();
                    iterator.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = iterator.next();
                String fieldName = entry.getKey();
                if (entry.getValue() instanceof TextNode) {
                    doc.put(fieldName, entry.getValue().textValue());
                } else {
                    doc.put(fieldName, entry.getValue());
                }
            }
            docs.add(doc);
        }

        // Extract sort values only from the last hit — ES returns hits in sort
        // order, so the last one's sort value is the cursor for the next batch.
        // Per-hit sort extraction would allocate Object[] N times per batch for
        // N-1 values that get thrown away.
        Object[] lastSortValues = null;
        if (hitsNode.size() > 0) {
            lastSortValues = extractSortValues(hitsNode.get(hitsNode.size() - 1));
        }

        return PointInTimeResult.builder()
                .pitId(pitId)
                .docs(docs)
                .totalHits(totalHits)
                .searchAfter(lastSortValues)
                .build();
    }

    /**
     * Pull {@code sort} array off a hit and convert to Object[] for
     * {@code search_after}.
     *
     * <p>{@link #doSearchWithPointInTime} hardcodes {@code sort=["_shard_doc"]},
     * which ES returns as a single long per doc. Any other type is rejected — if
     * a future change broadens the sort config, this parser must be updated to
     * match (null handling, string tiebreakers, floats, etc.). Silently coercing
     * would let pagination break subtly.
     */
    private static Object[] extractSortValues(JsonNode hit) {
        JsonNode sortNode = hit.get("sort");
        if (sortNode == null || !sortNode.isArray()) {
            return null;
        }
        Object[] out = new Object[sortNode.size()];
        for (int i = 0; i < sortNode.size(); i++) {
            JsonNode sv = sortNode.get(i);
            if (!sv.isIntegralNumber() || !sv.canConvertToLong()) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SEARCH_WITH_PIT_FAILED,
                        String.format(
                                "Unexpected sort value at index %d in PIT response: "
                                        + "type=%s, value=%s. Expected a long (from "
                                        + "sort=[\"_shard_doc\"]). If you broadened the "
                                        + "sort configuration, update the PIT response "
                                        + "parser to handle the new type.",
                                i, sv.getNodeType(), sv));
            }
            out[i] = sv.longValue();
        }
        return out;
    }
}
