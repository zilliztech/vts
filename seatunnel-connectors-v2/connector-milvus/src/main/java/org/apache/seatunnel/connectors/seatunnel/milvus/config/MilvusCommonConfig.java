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

package org.apache.seatunnel.connectors.seatunnel.milvus.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public abstract class MilvusCommonConfig {

    public static final String CONNECTOR_IDENTITY = "Milvus";

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Milvus public endpoint");

    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Milvus token for authentication");
    public static final Option<String> SERVER_PEM_PATH =
            Options.key("server_pem_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Path to the PEM file for server certificate");
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
    public static final Option<String> JOB_ID =
            Options.key("job_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("related vts job id");
}
