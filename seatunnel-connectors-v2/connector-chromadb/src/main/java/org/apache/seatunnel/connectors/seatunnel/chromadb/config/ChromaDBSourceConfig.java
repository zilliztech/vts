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

package org.apache.seatunnel.connectors.seatunnel.chromadb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.ArrayList;
import java.util.List;

public class ChromaDBSourceConfig {

    // ── Connector identity ──

    public static final String CONNECTOR_IDENTITY = "ChromaDB";

    // ── ChromaDB data model field names ──

    public static final String FIELD_ID = "id";
    public static final String FIELD_EMBEDDING = "embedding";
    public static final String FIELD_DOCUMENT = "document";
    public static final String FIELD_URI = "uri";

    // ── ChromaDB API constants ──

    public static final String INCLUDE_EMBEDDINGS = "embeddings";
    public static final String INCLUDE_DOCUMENTS = "documents";
    public static final String INCLUDE_METADATAS = "metadatas";
    public static final String INCLUDE_URIS = "uris";

    // ── Connection options ──

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "ChromaDB server URL, e.g. http://localhost:8000 or https://my-chroma:8000");

    public static final Option<String> TENANT =
            Options.key("tenant")
                    .stringType()
                    .defaultValue("default_tenant")
                    .withDescription("ChromaDB tenant name");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .defaultValue("default_database")
                    .withDescription("ChromaDB database name");

    public static final Option<List<String>> COLLECTIONS =
            Options.key("collections")
                    .listType()
                    .noDefaultValue()
                    .withDescription("ChromaDB collection names to read.");

    // ── Authentication options ──

    public static final Option<String> AUTH_TYPE =
            Options.key("auth_type")
                    .stringType()
                    .defaultValue("BEARER")
                    .withDescription(
                            "Authentication type: BEARER (Authorization: Bearer <token>), "
                                    + "X_CHROMA_TOKEN (X-Chroma-Token: <token>), "
                                    + "or BASIC (Authorization: Basic base64(username:password))");

    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Authentication token for BEARER or X_CHROMA_TOKEN auth type.");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Username for BASIC auth.");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Password for BASIC auth.");

    // ── HTTP tuning options ──

    public static final Option<Integer> CONNECT_TIMEOUT =
            Options.key("connect_timeout")
                    .intType()
                    .defaultValue(30)
                    .withDescription("HTTP connect timeout in seconds. Default 30.");

    public static final Option<Integer> READ_TIMEOUT =
            Options.key("read_timeout")
                    .intType()
                    .defaultValue(600)
                    .withDescription(
                            "HTTP read timeout in seconds. Default 600 (10 minutes). "
                                    + "Increase for very large collections.");

    // ── Read tuning options ──

    public static final Option<Integer> ID_SPLIT_SIZE =
            Options.key("id_split_size")
                    .intType()
                    .defaultValue(10000000)
                    .withDescription(
                            "Number of IDs to fetch per split when scanning the collection. "
                                    + "Default 10000000 (10M), large enough to scan all IDs in one pass "
                                    + "for most collections. Reduce if memory is constrained.");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Batch size for fetching full records by ID list");
}
