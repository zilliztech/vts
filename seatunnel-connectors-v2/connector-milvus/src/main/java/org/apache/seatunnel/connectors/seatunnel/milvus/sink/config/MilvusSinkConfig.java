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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink.config;

import io.milvus.v2.common.ConsistencyLevel;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.sink.DataSaveMode;
import static org.apache.seatunnel.api.sink.DataSaveMode.APPEND_DATA;
import static org.apache.seatunnel.api.sink.DataSaveMode.DROP_DATA;
import static org.apache.seatunnel.api.sink.DataSaveMode.ERROR_WHEN_DATA_EXISTS;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusCommonConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MilvusSinkConfig extends MilvusCommonConfig {

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("database");
    public static final Option<Map<String, String>> COLLECTION_DESCRIPTION =
            Options.key("collection_description")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription("collection description");
    public static final Option<String> PARTITION_KEY =
            Options.key("partition_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Milvus partition key field");

    public static final Option<SchemaSaveMode> SCHEMA_SAVE_MODE =
            Options.key("schema_save_mode")
                    .enumType(SchemaSaveMode.class)
                    .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                    .withDescription("schema_save_mode");

    public static final Option<DataSaveMode> DATA_SAVE_MODE =
            Options.key("data_save_mode")
                    .singleChoice(
                            DataSaveMode.class,
                            Arrays.asList(DROP_DATA, APPEND_DATA, ERROR_WHEN_DATA_EXISTS))
                    .defaultValue(APPEND_DATA)
                    .withDescription("data_save_mode");

    public static final Option<Boolean> ENABLE_AUTO_ID =
            Options.key("enable_auto_id")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Enable Auto Id");

    public static final Option<Boolean> ENABLE_DYNAMIC_FIELD =
            Options.key("enable_dynamic_field")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Enable dynamic field");

    public static final Option<ConsistencyLevel> CONSISTENCY_LEVEL =
            Options.key("consistency_level")
                    .singleChoice(
                            ConsistencyLevel.class,
                            Arrays.asList(ConsistencyLevel.STRONG, ConsistencyLevel.SESSION,
                                    ConsistencyLevel.EVENTUALLY, ConsistencyLevel.BOUNDED))
                    .noDefaultValue()
                    .withDescription("consistency level");

    public static final Option<Integer> SHARDS_NUM =
            Options.key("consistency_level")
                    .intType()
                    .noDefaultValue()
                    .withDescription("consistency level");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("writer batch size");
    public static final Option<Boolean> CREATE_INDEX =
            Options.key("create_index")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("create index");

    public static final Option<Map<String, String>> BULK_WRITER_CONFIG =
            Options.key("bulk_writer_config")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription("bulk writer config");
    public static final Option<Integer> WRITER_CACHE =
            Options.key("writer_cache")
                    .intType()
                    .defaultValue(500000)
                    .withDescription("max cache allowed");

}
