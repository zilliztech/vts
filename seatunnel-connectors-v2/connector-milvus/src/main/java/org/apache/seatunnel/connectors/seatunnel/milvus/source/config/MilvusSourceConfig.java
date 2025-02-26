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

package org.apache.seatunnel.connectors.seatunnel.milvus.source.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusCommonConfig;

import java.util.ArrayList;
import java.util.List;

public class MilvusSourceConfig extends MilvusCommonConfig {

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .defaultValue("default")
                    .withDescription("database");

    public static final Option<List<String>> COLLECTION =
            Options.key("collections")
                    .listType()
                    .defaultValue(new ArrayList<>())
                    .withDescription("Milvus collections to read");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("writer batch size");

    public static final Option<Integer> PARALLELISM =
            Options.key("parallelism")
            .intType()
            .defaultValue(1)
            .withDescription("parallelism");
}
