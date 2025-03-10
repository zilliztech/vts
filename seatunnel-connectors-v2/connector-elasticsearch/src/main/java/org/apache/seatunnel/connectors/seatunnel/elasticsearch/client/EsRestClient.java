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

import io.github.acm19.aws.interceptor.http.AwsRequestSigningApacheInterceptor;
import org.apache.http.Header;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.message.BasicHeader;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.KNN_VECTOR;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType.VECTOR;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.EsClusterConnectionConfig.IAM;
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
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ScrollResult;
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
import org.apache.http.util.Asserts;
import org.apache.http.util.EntityUtils;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLContext;

import java.io.Closeable;
import java.io.IOException;
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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.regions.Region;

@Slf4j
public class EsRestClient implements Closeable {

    private static final int CONNECTION_REQUEST_TIMEOUT = 10 * 1000;

    private static final int SOCKET_TIMEOUT = 5 * 60 * 1000;

    private final RestClient restClient;
    private static ReadonlyConfig connConfig;

    private EsRestClient(RestClient restClient) {
        this.restClient = restClient;
    }

    public static EsRestClient createInstance(ReadonlyConfig config) {
        connConfig = config;
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
                    }else{
                        Map<String, String> iam = connConfig.get(IAM);
                        AwsCredentialsProvider awsCredentials = StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(iam.get("access_key"), iam.get("secret_key"))
                        );
                        // AWS Region & Service
                        String region = iam.get("region");  // Update to match your OpenSearch region
                        String serviceName = iam.get("service_name"); // Use "es" for Managed OpenSearch, "aoss" for Serverless

                        // OpenSearch Host (WITHOUT "https://")
                        HttpRequestInterceptor awsSigV4Interceptor = new AwsRequestSigningApacheInterceptor(
                                serviceName,
                                AwsV4HttpSigner.create(),
                                awsCredentials,
                                region
                        );
                        httpClientBuilder.addInterceptorLast(awsSigV4Interceptor);
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
                ObjectMapper objectMapper = new ObjectMapper();
                String entity = EntityUtils.toString(response.getEntity());
                JsonNode json = objectMapper.readTree(entity);
                int took = json.get("took").asInt();
                boolean errors = json.get("errors").asBoolean();
                return new BulkResponse(errors, took, entity);
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                        String.format(
                                "bulk es response status code=%d,request boy=%s",
                                response.getStatusLine().getStatusCode(), requestBody));
            }
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                    String.format("bulk es error,request boy=%s", requestBody),
                    e);
        }
    }

    public ElasticsearchClusterInfo getClusterInfo() {
        Request request = new Request("GET", "/");
        try {
            Response response = restClient.performRequest(request);
            String result = EntityUtils.toString(response.getEntity());
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(result);
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
        Map<String, Object> param = new HashMap<>();
        param.put("query", query);
        param.put("_source", source);
        param.put("sort", new String[] {"_doc"});
        param.put("size", scrollSize);
        String endpoint = "/" + index + "/_search?scroll=" + scrollTime;
        return getDocsFromScrollRequest(endpoint, JsonUtils.toJsonString(param));
    }
    public ScrollResult search(String index, List<String> source, Map<String, Object> query, List<Object> searchAfter, int pageSize) {
        Map<String, Object> param = new HashMap<>();
        param.put("query", query);
        param.put("_source", source);
        param.put("size", pageSize); // ✅ Use pageSize dynamically
        param.put("sort", List.of(
                Map.of("_id", "asc")
        )); // ✅ Stable sorting

        if (searchAfter != null && !searchAfter.isEmpty()) {
            param.put("search_after", searchAfter); // ✅ Add `search_after` only if available
        }

        String endpoint = "/" + index + "/_search";

        try {
            return getDocsFromScrollRequest(endpoint, JsonUtils.toJsonString(param));
        } catch (Exception e) {
            System.err.println("Error during search_after request: " + e.getMessage());
            return null;
        }
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
        return getDocsFromScrollRequest("/_search/scroll", JsonUtils.toJsonString(param));
    }

    private ScrollResult getDocsFromScrollRequest(String endpoint, String requestBody) {
        Request request = new Request("POST", endpoint);
        request.setJsonEntity(requestBody);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                        "POST " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                ObjectNode responseJson = JsonUtils.parseObject(entity);

                JsonNode shards = responseJson.get("_shards");
                int totalShards = shards.get("total").intValue();
                int successful = shards.get("successful").intValue();
                Asserts.check(
                        totalShards == successful,
                        String.format(
                                "POST %s,total shards(%d)!= successful shards(%d)",
                                endpoint, totalShards, successful));

                return getDocsFromScrollResponse(responseJson);
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                        String.format(
                                "POST %s response status code=%d,request boy=%s",
                                endpoint, response.getStatusLine().getStatusCode(), requestBody));
            }
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                    String.format("POST %s error,request boy=%s", endpoint, requestBody),
                    e);
        }
    }

    private ScrollResult getDocsFromScrollResponse(ObjectNode responseJson) {
        ScrollResult scrollResult = new ScrollResult();
        String scrollId = responseJson.get("_scroll_id") == null ? null : responseJson.get("_scroll_id").asText();
        scrollResult.setScrollId(scrollId);

        JsonNode hitsNode = responseJson.get("hits").get("hits");
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
        if (hitsNode.isArray() && !hitsNode.isEmpty()) {
            JsonNode lastSortNode = hitsNode.get(hitsNode.size() - 1).get("sort"); // Get last "sort"
            // Convert to List<Object>
            List<Object> lastSortList = new ArrayList<>();
            if (lastSortNode != null && lastSortNode.isArray()) {
                for (JsonNode valueNode : lastSortNode) {
                    if (valueNode.isNumber()) {
                        lastSortList.add(valueNode.numberValue()); // Add as Integer/Long/Double
                    } else if (valueNode.isTextual()) {
                        lastSortList.add(valueNode.textValue());  // Add as String
                    } else {
                        lastSortList.add(valueNode.toString());  // Fallback
                    }
                }
            }
            scrollResult.setLastSort(lastSortList);
        }
        return scrollResult;
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
}
