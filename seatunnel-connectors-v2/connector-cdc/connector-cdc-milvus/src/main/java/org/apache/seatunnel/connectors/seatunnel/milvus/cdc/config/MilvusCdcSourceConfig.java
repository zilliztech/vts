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

package org.apache.seatunnel.connectors.seatunnel.milvus.cdc.config;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MilvusCdcSourceConfig {

    public static final String CONNECTOR_IDENTITY = "Milvus-CDC";

    public static final Option<String> URL =
            Options.key("url").stringType().noDefaultValue().withDescription("Milvus endpoint");

    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Milvus token for authentication");

    public static final Option<Map<String, List<String>>> DATABASE_COLLECTIONS =
            Options.key("database_collections")
                    .type(new TypeReference<Map<String, List<String>>>() {})
                    .noDefaultValue()
                    .withDescription("Milvus CDC source database to source collections mapping");

    public static final Option<List<String>> MESSAGE_TYPES =
            Options.key("message_types")
                    .listType()
                    .defaultValue(Arrays.asList("insert", "delete"))
                    .withDescription(
                            "Milvus CDC message types to process. Currently supports insert and delete only.");

    public static final Option<List<Map<String, Object>>> CHANNEL_POSITIONS =
            Options.key("channel_positions")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription("Per-pchannel CDC start positions");

    public static final Option<MilvusCdcStartupMode> STARTUP_MODE =
            Options.key("startup_mode")
                    .enumType(MilvusCdcStartupMode.class)
                    .defaultValue(MilvusCdcStartupMode.CDC)
                    .withDescription("Milvus CDC startup mode");

    public static final Option<Integer> QUEUE_CAPACITY =
            Options.key("queue_capacity")
                    .intType()
                    .defaultValue(16)
                    .withDescription("Buffered Milvus CDC WAL messages per reader");

    public static final Option<String> CLIENT_PEM_PATH =
            Options.key("client_pem_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Path to the PEM file for client certificate");

    public static final Option<String> CLIENT_KEY_PATH =
            Options.key("client_key_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Path to the KEY file for client certificate");

    public static final Option<String> CA_PEM_PATH =
            Options.key("ca_pem_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Path to the PEM file for CA certificate");

    public static final Option<String> SERVER_NAME =
            Options.key("server_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Server name for TLS verification");
}
