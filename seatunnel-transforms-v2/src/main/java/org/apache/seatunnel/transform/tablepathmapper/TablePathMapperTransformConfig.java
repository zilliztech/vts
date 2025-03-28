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

package org.apache.seatunnel.transform.tablepathmapper;

import lombok.Getter;
import lombok.Setter;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 全部形式如下，目录同步到Milvus应该只需要用到table_mapper部分 TablePathMapper { table_mapper = { test-vector2 =
 * test_vector2 } database_mapper = {
 *
 * <p>} schema_mapper = {
 *
 * <p>} }
 */
@Getter
@Setter
public class TablePathMapperTransformConfig implements Serializable {
    public static final Option<Map<String, String>> TABLE_MAPPER =
            Options.key("table_mapper")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the table name mapping relationship between input and output");
    public static final Option<Map<String, String>> DATABASE_MAPPER =
            Options.key("database_mapper")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the database name mapping relationship between input and output");
    public static final Option<Map<String, String>> SCHEMA_MAPPER =
            Options.key("schema_mapper")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the schema name mapping relationship between input and output");

    private Map<String, String> tableMapper = new LinkedHashMap<>();
    private Map<String, String> databaseMapper = new LinkedHashMap<>();
    private Map<String, String> schemaMapper = new LinkedHashMap<>();

    public static TablePathMapperTransformConfig of(ReadonlyConfig config) {
        TablePathMapperTransformConfig tablePathMapperTransformConfig =
                new TablePathMapperTransformConfig();
        tablePathMapperTransformConfig.setTableMapper(config.get(TABLE_MAPPER));
        tablePathMapperTransformConfig.setDatabaseMapper(config.get(DATABASE_MAPPER));
        tablePathMapperTransformConfig.setSchemaMapper(config.get(SCHEMA_MAPPER));
        return tablePathMapperTransformConfig;
    }
}
