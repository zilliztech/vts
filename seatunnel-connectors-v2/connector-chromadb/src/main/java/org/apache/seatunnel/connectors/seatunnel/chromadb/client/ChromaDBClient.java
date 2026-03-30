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
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.chromadb.config.ChromaDBSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.chromadb.exception.ChromaDBConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.chromadb.exception.ChromaDBConnectorException;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Slf4j
public class ChromaDBClient implements AutoCloseable {

    private static final MediaType JSON_MEDIA_TYPE =
            MediaType.get("application/json; charset=utf-8");
    private static final Gson GSON = new GsonBuilder().create();

    private static final String PATH_COLLECTIONS =
            "/api/v2/tenants/%s/databases/%s/collections";
    private static final String PATH_COLLECTION =
            "/api/v2/tenants/%s/databases/%s/collections/%s";
    private static final String PATH_RECORDS_GET =
            "/api/v2/tenants/%s/databases/%s/collections/%s/get";

    private final String baseUrl;
    private final String tenant;
    private final String database;
    private final OkHttpClient httpClient;
    private final String authHeaderName;
    private final String authHeaderValue;
    private int maxRetries = 3;
    private long retryBaseDelayMs = 1000;

    public ChromaDBClient(ReadonlyConfig config) {
        String url = config.get(ChromaDBSourceConfig.URL);
        this.baseUrl = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
        this.tenant = config.get(ChromaDBSourceConfig.TENANT);
        this.database = config.get(ChromaDBSourceConfig.DATABASE);

        String token = config.get(ChromaDBSourceConfig.TOKEN);
        String username = config.get(ChromaDBSourceConfig.USERNAME);
        String password = config.get(ChromaDBSourceConfig.PASSWORD);
        String authType = config.get(ChromaDBSourceConfig.AUTH_TYPE);

        if ("BASIC".equalsIgnoreCase(authType)
                && password != null
                && !password.isEmpty()) {
            String user = (username != null) ? username : "";
            String credentials =
                    Base64.getEncoder()
                            .encodeToString(
                                    (user + ":" + password)
                                            .getBytes(StandardCharsets.UTF_8));
            this.authHeaderName = "Authorization";
            this.authHeaderValue = "Basic " + credentials;
        } else if ("BASIC".equalsIgnoreCase(authType)
                && (password == null || password.isEmpty())) {
            log.warn(
                    "auth_type is BASIC but password is empty — "
                            + "falling back to token-based auth. "
                            + "Set 'password' for BASIC auth.");
            if (token != null && !token.isEmpty()) {
                this.authHeaderName = "Authorization";
                this.authHeaderValue = "Bearer " + token;
            } else {
                this.authHeaderName = null;
                this.authHeaderValue = null;
            }
        } else if (token != null && !token.isEmpty()) {
            if ("X_CHROMA_TOKEN".equalsIgnoreCase(authType)) {
                this.authHeaderName = "X-Chroma-Token";
                this.authHeaderValue = token;
            } else {
                this.authHeaderName = "Authorization";
                this.authHeaderValue = "Bearer " + token;
            }
        } else {
            this.authHeaderName = null;
            this.authHeaderValue = null;
        }
        int connectTimeout = config.get(ChromaDBSourceConfig.CONNECT_TIMEOUT);
        int readTimeout = config.get(ChromaDBSourceConfig.READ_TIMEOUT);
        this.httpClient =
                new OkHttpClient.Builder()
                        .connectTimeout(connectTimeout, TimeUnit.SECONDS)
                        .readTimeout(readTimeout, TimeUnit.SECONDS)
                        .writeTimeout(readTimeout, TimeUnit.SECONDS)
                        .build();
    }

    /** Package-visible for testing — override retry behavior without exposing it in the API. */
    void setRetryPolicy(int maxRetries, long retryBaseDelayMs) {
        this.maxRetries = maxRetries;
        this.retryBaseDelayMs = retryBaseDelayMs;
    }

    // ---- Collection operations ----

    public List<CollectionInfo> listCollections() {
        String path =
                String.format(PATH_COLLECTIONS, encode(tenant), encode(database));
        return doGet(path, new com.google.gson.reflect.TypeToken<List<CollectionInfo>>() {}.getType());
    }

    public CollectionInfo getCollection(String collectionName) {
        return doGet(buildCollectionPath(PATH_COLLECTION, collectionName), CollectionInfo.class);
    }

    // ---- Record operations ----

    public GetRecordsResult getRecords(String collectionId, GetRecordsRequest request) {
        return doPost(buildCollectionPath(PATH_RECORDS_GET, collectionId), request, GetRecordsResult.class);
    }

    // ---- HTTP primitives ----

    private <T> T doGet(String path, Type responseType) {
        Request request = newRequestBuilder(path).get().build();
        return execute(request, responseType);
    }

    private <T> T doPost(String path, Object body, Type responseType) {
        String json = GSON.toJson(body);
        RequestBody requestBody = RequestBody.create(json, JSON_MEDIA_TYPE);
        Request request = newRequestBuilder(path).post(requestBody).build();
        return execute(request, responseType);
    }

    @SuppressWarnings("unchecked")
    private <T> T execute(Request request, Type responseType) {
        return executeWithRetry(
                () -> {
                    try (Response response = httpClient.newCall(request).execute()) {
                        ResponseBody body = response.body();
                        String bodyStr = body != null ? body.string() : "";

                        if (!response.isSuccessful()) {
                            if (response.code() == 404) {
                                throw new ChromaDBConnectorException(
                                        ChromaDBConnectorErrorCode.COLLECTION_NOT_FOUND,
                                        "Resource not found: "
                                                + request.url()
                                                + ", response: "
                                                + bodyStr);
                            }
                            if (isRetryable(response.code())) {
                                throw new RetryableException(
                                        "HTTP " + response.code() + " from " + request.url());
                            }
                            throw new ChromaDBConnectorException(
                                    ChromaDBConnectorErrorCode.CONNECT_FAILED,
                                    "HTTP "
                                            + response.code()
                                            + " from "
                                            + request.url()
                                            + ", response: "
                                            + bodyStr);
                        }

                        return (T) GSON.fromJson(bodyStr, responseType);
                    } catch (IOException e) {
                        throw new RetryableException(
                                "Failed to connect to ChromaDB at " + baseUrl, e);
                    }
                });
    }

    private <T> T executeWithRetry(Supplier<T> action) {
        Exception lastException = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                return action.get();
            } catch (RetryableException e) {
                lastException = e;
                if (attempt < maxRetries) {
                    long delay = retryBaseDelayMs * (1L << attempt);
                    delay += ThreadLocalRandom.current().nextLong(Math.max(1, delay / 4));
                    log.warn(
                            "Request failed (attempt {}/{}), retrying in {}ms: {}",
                            attempt + 1,
                            maxRetries + 1,
                            delay,
                            e.getMessage());
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        throw new ChromaDBConnectorException(
                ChromaDBConnectorErrorCode.CONNECT_FAILED,
                "Request failed after " + (maxRetries + 1) + " attempts",
                lastException);
    }

    private static boolean isRetryable(int httpCode) {
        return httpCode == 429 || httpCode == 502 || httpCode == 503 || httpCode == 504;
    }

    private static class RetryableException extends RuntimeException {
        RetryableException(String message) {
            super(message);
        }

        RetryableException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private Request.Builder newRequestBuilder(String path) {
        Request.Builder builder = new Request.Builder().url(baseUrl + path);
        if (authHeaderName != null) {
            builder.addHeader(authHeaderName, authHeaderValue);
        }
        return builder;
    }

    private String buildCollectionPath(String template, String collectionNameOrId) {
        return String.format(
                template, encode(tenant), encode(database), encode(collectionNameOrId));
    }

    private static String encode(String segment) {
        try {
            return URLEncoder.encode(segment, StandardCharsets.UTF_8.name()).replace("+", "%20");
        } catch (java.io.UnsupportedEncodingException e) {
            throw new RuntimeException(e); // UTF-8 is always supported
        }
    }

    @Override
    public void close() {
        httpClient.dispatcher().executorService().shutdown();
        try {
            if (!httpClient.dispatcher().executorService().awaitTermination(5, TimeUnit.SECONDS)) {
                httpClient.dispatcher().executorService().shutdownNow();
            }
        } catch (InterruptedException e) {
            httpClient.dispatcher().executorService().shutdownNow();
            Thread.currentThread().interrupt();
        }
        httpClient.connectionPool().evictAll();
    }

    // ---- DTOs ----

    @Getter
    public static class CollectionInfo implements Serializable {
        private String id;
        private String name;
        private Map<String, Object> metadata;
        private Integer dimension;

        @SerializedName("configuration_json")
        private Map<String, Object> configurationJson;
    }

    public static class GetRecordsRequest implements Serializable {
        private List<String> ids;
        private List<String> include;
        private Integer limit;
        private Integer offset;

        public GetRecordsRequest() {}

        public GetRecordsRequest setIds(List<String> ids) {
            this.ids = ids;
            return this;
        }

        public GetRecordsRequest setInclude(List<String> include) {
            this.include = include;
            return this;
        }

        public GetRecordsRequest setLimit(Integer limit) {
            this.limit = limit;
            return this;
        }

        public GetRecordsRequest setOffset(Integer offset) {
            this.offset = offset;
            return this;
        }
    }

    @Getter
    public static class GetRecordsResult implements Serializable {
        private List<String> ids;
        private List<List<Float>> embeddings;
        private List<String> documents;
        private List<String> uris;
        private List<Map<String, Object>> metadatas;
    }
}
