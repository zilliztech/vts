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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MilvusSinkConfig extends MilvusCommonConfig {

        public static final Option<SchemaSaveMode> SCHEMA_SAVE_MODE = Options.key("schema_save_mode")
                        .enumType(SchemaSaveMode.class)
                        .defaultValue(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST)
                        .withDescription("schema_save_mode");

        public static final Option<DataSaveMode> DATA_SAVE_MODE = Options.key("data_save_mode")
                        .singleChoice(
                                        DataSaveMode.class,
                                        Arrays.asList(DROP_DATA, APPEND_DATA, ERROR_WHEN_DATA_EXISTS))
                        .defaultValue(APPEND_DATA)
                        .withDescription("data_save_mode");

        public static final Option<String> DATABASE = Options.key("database")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("database");
        public static final Option<Map<String, String>> COLLECTION_RENAME = Options.key("collection_rename")
                        .mapType()
                        .defaultValue(new HashMap<>())
                        .withDescription("collection rename");
        public static final Option<Map<String, String>> COLLECTION_DESCRIPTION = Options.key("collection_description")
                        .mapType()
                        .defaultValue(new HashMap<>())
                        .withDescription("collection description");

        /**
         * Unified field schema configuration for defining the target collection schema.
         *
         * IMPORTANT: When field_schema is supplied, ONLY the fields defined in
         * field_schema will be used.
         * The schema is built entirely from the configuration - source schema is not
         * referenced.
         *
         * If field_schema is NOT supplied (empty), the full source schema will be used
         * by default.
         *
         * Each field object MUST contain:
         *
         * Required fields:
         * - field_name: Name of the field in the target collection (String, required)
         * OR
         * - source_field_name: For dynamic field extraction, the path in source dynamic
         * field (String)
         * - If both are provided, field_name is used as target name
         * - If only source_field_name is provided, it's used as both source path and
         * target name
         * - data_type: Milvus data type code (Integer, REQUIRED for all fields)
         * Common types: Int64=5, VarChar=21, FloatVector=101, etc.
         *
         * Optional type-specific fields:
         * - element_type: Element type for Array fields (Integer, required for Array
         * type)
         * - max_capacity: Max capacity for Array fields (Integer, optional, default:
         * 4096)
         * - max_length: Max length for VarChar/String fields (Integer, optional,
         * default: 65535)
         * - dimension: Dimension for vector fields (Integer, required for vector types)
         *
         * Optional field properties:
         * - is_nullable: Whether the field is nullable (Boolean, optional, default:
         * true)
         * - default_value: Default value for the field (Object, optional)
         * - is_primary_key: Mark field as primary key (Boolean, optional, default:
         * false)
         * - auto_id: Enable auto ID for primary key field (Boolean, optional)
         * - is_partition_key: Mark field as partition key (Boolean, optional, default:
         * false)
         * - enable_analyzer: Enable analyzer for full-text search (Boolean, optional,
         * default: false)
         * - analyzer_params: Analyzer parameters (Map<String, Object>, optional)
         * - enable_match: Enable match for text matching (Boolean, optional, default:
         * false)
         *
         * Example - Define complete schema:
         * field_schema = [
         * {field_name = "id", data_type = 5, is_primary_key = true, auto_id = true}
         * {field_name = "vector", data_type = 101, dimension = 768}
         * {field_name = "category", data_type = 21, max_length = 100, is_partition_key
         * = true}
         * ]
         *
         * Example - Extract from dynamic fields:
         * field_schema = [
         * {source_field_name = "metadata.title", field_name = "title", data_type = 21,
         * max_length = 200}
         * {source_field_name = "metadata.tags", field_name = "tags", data_type = 22,
         * element_type = 21}
         * ]
         */
        public static final Option<List<Object>> FIELD_SCHEMA = Options.key("field_schema")
                        .listType(Object.class)
                        .defaultValue(new ArrayList<>())
                        .withDescription(
                                        "Field schema configuration. When supplied, ONLY fields defined here will be used (built entirely from config). If empty, uses full source schema.");
        /**
         * List of Milvus functions to be added to the collection schema.
         * Each function object should contain:
         * - name: Function name (String)
         * - description: Function description (String)
         * - functionType: Type of function, e.g., "BM25", "TEXTEMBEDDING" (String)
         * - inputFieldNames: List of input field names (List<String>)
         * - outputFieldNames: List of output field names (List<String>)
         * - params: Additional parameters for the function (Map<String, String>)
         *
         * Functions from config will be merged with functions from source metadata.
         */
        public static final Option<List<Object>> functionList = Options.key("function_list")
                        .listType(Object.class)
                        .defaultValue(new ArrayList<>())
                        .withDescription(
                                        "List of Milvus functions. Each function should contain: name, description, functionType, inputFieldNames, outputFieldNames, and params");

        /**
         * Struct fields configuration for Array[Struct] type support.
         *
         * This is a simple list of struct field names to include from the source
         * schema.
         * Struct field definitions (nested fields, max_capacity, etc.) are
         * automatically
         * extracted from the source metadata.
         *
         * If empty (default), all struct fields from source will be included.
         * If specified, only the listed struct field names will be included.
         *
         * Example:
         * struct_fields = ["clips", "videos", "segments"]
         *
         * This acts as a filter to control which Array[Struct] fields are migrated
         * from source to sink collection.
         */
        public static final Option<List<String>> structFieldsList = Options.key("struct_fields")
                        .listType(String.class)
                        .defaultValue(new ArrayList<>())
                        .withDescription("List of struct field names to include from source. Empty means include all.");

        public static final Option<Boolean> ENABLE_DYNAMIC_FIELD = Options.key("enable_dynamic_field")
                        .booleanType()
                        .noDefaultValue()
                        .withDescription("Enable dynamic field");

        public static final Option<ConsistencyLevel> CONSISTENCY_LEVEL = Options.key("consistency_level")
                        .singleChoice(
                                        ConsistencyLevel.class,
                                        Arrays.asList(ConsistencyLevel.STRONG, ConsistencyLevel.SESSION,
                                                        ConsistencyLevel.EVENTUALLY, ConsistencyLevel.BOUNDED))
                        .noDefaultValue()
                        .withDescription("consistency level");

        public static final Option<Integer> SHARDS_NUM = Options.key("shard_num")
                        .intType()
                        .noDefaultValue()
                        .withDescription("shard num");

        public static final Option<String> TIMEZONE = Options.key("timezone")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("timezone");

        public static final Option<Integer> BATCH_SIZE = Options.key("batch_size")
                        .intType()
                        .defaultValue(1000)
                        .withDescription("writer batch size");

        public static final Option<Map<String, String>> BULK_WRITER_CONFIG = Options.key("bulk_writer_config")
                        .mapType()
                        .defaultValue(new HashMap<>())
                        .withDescription("bulk writer config");
}
