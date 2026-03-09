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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class PartitionRouterTest {

    @Test
    void emptyRoutingTablePreservesPartition() {
        PartitionRouter router = new PartitionRouter(Collections.emptyMap());
        assertEquals("part1", router.resolve("colA", "part1"));
        assertEquals("_default", router.resolve("colA", "_default"));
    }

    @Test
    void nullOrEmptyPartitionReturnsDefault() {
        PartitionRouter router = new PartitionRouter(Collections.emptyMap());
        assertEquals("_default", router.resolve("colA", null));
        assertEquals("_default", router.resolve("colA", ""));
    }

    @Test
    void exactMatchTakesPriority() {
        Map<String, Map<String, String>> table = new HashMap<>();
        Map<String, String> colRules = new HashMap<>();
        colRules.put("part1", "target_p1");
        colRules.put("*", "fallback");
        table.put("colA", colRules);

        PartitionRouter router = new PartitionRouter(table);
        assertEquals("target_p1", router.resolve("colA", "part1"));
    }

    @Test
    void tableWildcardFallback() {
        Map<String, Map<String, String>> table = new HashMap<>();
        Map<String, String> colRules = new HashMap<>();
        colRules.put("part1", "target_p1");
        colRules.put("*", "other");
        table.put("colA", colRules);

        PartitionRouter router = new PartitionRouter(table);
        assertEquals("other", router.resolve("colA", "part2"));
        assertEquals("other", router.resolve("colA", "_default"));
    }

    @Test
    void globalWildcardFallback() {
        Map<String, Map<String, String>> table = new HashMap<>();
        Map<String, String> globalRules = new HashMap<>();
        globalRules.put("*", "global_partition");
        table.put("*", globalRules);

        PartitionRouter router = new PartitionRouter(table);
        assertEquals("global_partition", router.resolve("unknownCol", "anyPart"));
    }

    @Test
    void tableRuleTakesPriorityOverGlobal() {
        Map<String, Map<String, String>> table = new HashMap<>();
        Map<String, String> colRules = new HashMap<>();
        colRules.put("*", "from_colA");
        table.put("colA", colRules);
        Map<String, String> globalRules = new HashMap<>();
        globalRules.put("*", "global");
        table.put("*", globalRules);

        PartitionRouter router = new PartitionRouter(table);
        assertEquals("from_colA", router.resolve("colA", "part1"));
        assertEquals("global", router.resolve("colB", "part1"));
    }

    @Test
    void noWildcardPreservesUnmapped() {
        Map<String, Map<String, String>> table = new HashMap<>();
        Map<String, String> colRules = new HashMap<>();
        colRules.put("part1", "renamed");
        table.put("colA", colRules);

        PartitionRouter router = new PartitionRouter(table);
        assertEquals("renamed", router.resolve("colA", "part1"));
        assertEquals("part2", router.resolve("colA", "part2"));
    }

    @Test
    void hyphenInPartitionNameIsReplaced() {
        PartitionRouter router = new PartitionRouter(Collections.emptyMap());
        assertEquals("my_part", router.resolve("colA", "my-part"));
    }

    @Test
    void staticPartitionForAllData() {
        Map<String, Map<String, String>> table = new HashMap<>();
        Map<String, String> globalRules = new HashMap<>();
        globalRules.put("*", "partition_A");
        table.put("*", globalRules);

        PartitionRouter router = new PartitionRouter(table);
        assertEquals("partition_A", router.resolve("col1", "_default"));
        assertEquals("partition_A", router.resolve("col2", "part1"));
    }

    @Test
    void multiTableMergeScenario() {
        Map<String, Map<String, String>> table = new HashMap<>();
        table.put("colA", Collections.singletonMap("*", "from_A"));
        table.put("colB", Collections.singletonMap("*", "from_B"));
        table.put("colC", Collections.singletonMap("*", "from_C"));

        PartitionRouter router = new PartitionRouter(table);
        assertEquals("from_A", router.resolve("colA", "_default"));
        assertEquals("from_B", router.resolve("colB", "part1"));
        assertEquals("from_C", router.resolve("colC", "whatever"));
        assertEquals("part1", router.resolve("colD", "part1"));
    }

    @Test
    void flatConfigParsing() {
        // Simulates buildPartitionRouter parsing from flat HOCON map
        Map<String, String> flatMap = new HashMap<>();
        flatMap.put("colA.*", "from_A");
        flatMap.put("colB.hot", "active");
        flatMap.put("colB.*", "other");
        flatMap.put("*.*", "global");

        Map<String, Map<String, String>> routingTable = new HashMap<>();
        for (Map.Entry<String, String> entry : flatMap.entrySet()) {
            String key = entry.getKey();
            int dotIndex = key.lastIndexOf(".");
            String collection = key.substring(0, dotIndex);
            String partition = key.substring(dotIndex + 1);
            routingTable
                    .computeIfAbsent(collection, k -> new HashMap<>())
                    .put(partition, entry.getValue());
        }

        PartitionRouter router = new PartitionRouter(routingTable);
        assertEquals("from_A", router.resolve("colA", "_default"));
        assertEquals("active", router.resolve("colB", "hot"));
        assertEquals("other", router.resolve("colB", "cold"));
        assertEquals("global", router.resolve("colX", "any"));
    }
}
