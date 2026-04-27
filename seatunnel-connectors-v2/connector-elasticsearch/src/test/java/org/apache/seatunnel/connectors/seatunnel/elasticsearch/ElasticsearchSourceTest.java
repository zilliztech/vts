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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SearchApiTypeEnum;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.source.ElasticsearchSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchSourceTest {
    @Test
    @SuppressWarnings("unchecked")
    public void testIndexListInheritsTopLevelOptionsForEveryIndex() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Collections.singletonList("http://localhost:9200"));
        config.put(
                "index_list",
                java.util.Arrays.asList(indexConfig("idx-a"), indexConfig("idx-b")));
        config.put("search_api_type", "PIT");
        config.put("pit_keep_alive", 12345L);
        config.put("pit_batch_size", 321);
        config.put("scroll_time", "7m");
        config.put("scroll_size", 456);

        ElasticsearchSource source = new ElasticsearchSource(ReadonlyConfig.fromMap(config));

        Field field = ElasticsearchSource.class.getDeclaredField("sourceConfigList");
        field.setAccessible(true);
        List<SourceConfig> sourceConfigs = (List<SourceConfig>) field.get(source);
        Assertions.assertEquals(2, sourceConfigs.size());
        Assertions.assertEquals("idx-a", sourceConfigs.get(0).getIndex());
        Assertions.assertEquals("idx-b", sourceConfigs.get(1).getIndex());
        for (SourceConfig sourceConfig : sourceConfigs) {
            Assertions.assertEquals(SearchApiTypeEnum.PIT, sourceConfig.getSearchApiType());
            Assertions.assertEquals(12345L, sourceConfig.getPitKeepAlive());
            Assertions.assertEquals(321, sourceConfig.getPitBatchSize());
            Assertions.assertEquals("7m", sourceConfig.getScrollTime());
            Assertions.assertEquals(456, sourceConfig.getScrollSize());
        }
    }

    private static Map<String, Object> indexConfig(String index) {
        Map<String, Object> schema = new HashMap<>();
        schema.put("columns", Collections.singletonList(column("id", "string")));

        Map<String, Object> indexConfig = new HashMap<>();
        indexConfig.put("index", index);
        indexConfig.put("source", Collections.singletonList("id"));
        indexConfig.put("schema", schema);
        return indexConfig;
    }

    private static Map<String, Object> column(String name, String type) {
        Map<String, Object> column = new HashMap<>();
        column.put("name", name);
        column.put("type", type);
        return column;
    }

    @Test
    public void testPrepareWithEmptySource() throws PrepareFailException {
        BasicTypeDefine.BasicTypeDefineBuilder<EsType> typeDefine =
                BasicTypeDefine.<EsType>builder()
                        .name("field1")
                        .columnType("text")
                        .dataType("text");
        Map<String, BasicTypeDefine<EsType>> esFieldType = new HashMap<>();
        esFieldType.put("field1", typeDefine.build());
        SeaTunnelDataType[] seaTunnelDataTypes =
                ElasticsearchSource.getSeaTunnelDataType(
                        esFieldType, new ArrayList<>(esFieldType.keySet()));
        Assertions.assertNotNull(seaTunnelDataTypes);
        Assertions.assertEquals(seaTunnelDataTypes[0].getTypeClass(), String.class);
    }
}
