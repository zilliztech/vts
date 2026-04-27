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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.config;

import com.google.common.collect.ImmutableMap;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.catalog.CatalogTable;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class SourceConfig implements Serializable {

    public static final Option<List<Map<String, Object>>> INDEX_LIST =
            Options.key("index_list")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription("index_list for multiTable sync");

    public static final Option<String> INDEX =
            Options.key("index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Elasticsearch index name, support * fuzzy matching");

    public static final Option<List<String>> SOURCE =
            Options.key("source")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "The fields of index. You can get the document id by specifying the field _id.If sink _id to other index,you need specify an alias for _id due to the Elasticsearch limit");

    public static final Option<Map<String, String>> ARRAY_COLUMN =
            Options.key("array_column")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription(
                            "Because there is no array type in es,so need specify array Type.");

    public static final Option<String> SCROLL_TIME =
            Options.key("scroll_time")
                    .stringType()
                    .defaultValue("1m")
                    .withDescription(
                            "Amount of time Elasticsearch will keep the search context alive for scroll requests");

    public static final Option<Integer> SCROLL_SIZE =
            Options.key("scroll_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Maximum number of hits to be returned with each Elasticsearch scroll request");

    public static final Option<Map<String, Object>> QUERY =
            Options.key("query")
                    .type(new TypeReference<Map<String, Object>>() {})
                    .defaultValue(
                            Collections.singletonMap("match_all", new HashMap<String, String>()))
                    .withDescription(
                            "Elasticsearch query language. You can control the range of data read");
    public static final Option<Map> PK =
            Options.key("pk")
                    .objectType(Map.class)
                    .defaultValue(ImmutableMap.of("name", "_id", "type", "string", "length", 512))
                    .withDescription("The PK of es index, will as pk field of sink table.");

    public static final Option<SearchApiTypeEnum> SEARCH_API_TYPE =
            Options.key("search_api_type")
                    .enumType(SearchApiTypeEnum.class)
                    .defaultValue(SearchApiTypeEnum.SCROLL)
                    .withDescription(
                            "Choose API type for pagination: SCROLL or PIT (Point in Time).");

    public static final Option<Long> PIT_KEEP_ALIVE =
            Options.key("pit_keep_alive")
                    .longType()
                    .defaultValue(3600000L)
                    .withDescription(
                            "The amount of time (in milliseconds) for which the PIT should be kept alive. Default is 1 hour.");

    public static final Option<Integer> PIT_BATCH_SIZE =
            Options.key("pit_batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Maximum number of hits to be returned with each PIT search request. Similar to scroll_size but for PIT API.");

    private String index;
    private List<String> source;
    private Map<String, Object> query;
    private String scrollTime;
    private int scrollSize;

    private SearchApiTypeEnum searchApiType = SearchApiTypeEnum.SCROLL;
    private long pitKeepAlive = 3600000L;
    private int pitBatchSize = 100;

    private CatalogTable catalogTable;
}
