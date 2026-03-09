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

import static org.apache.seatunnel.connectors.seatunnel.milvus.common.MilvusConstants.DEFAULT_PARTITION;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Map;

/**
 * Resolves target partition names using a two-level routing table with wildcard support.
 *
 * <p>Routing table structure: Map&lt;sourceCollection, Map&lt;sourcePartition, targetPartition&gt;&gt;
 *
 * <p>Resolution priority:
 * <ol>
 *   <li>Exact match: routingTable[sourceCollection][sourcePartition]</li>
 *   <li>Table wildcard: routingTable[sourceCollection]["*"]</li>
 *   <li>Global wildcard: routingTable["*"]["*"]</li>
 *   <li>No match: preserve original partition name</li>
 * </ol>
 */
public class PartitionRouter {

    private static final String WILDCARD = "*";

    private final Map<String, Map<String, String>> routingTable;

    public PartitionRouter(Map<String, Map<String, String>> routingTable) {
        this.routingTable = routingTable != null ? routingTable : Collections.emptyMap();
    }

    public String resolve(String sourceCollection, String sourcePartition) {
        if (StringUtils.isEmpty(sourcePartition)) {
            sourcePartition = DEFAULT_PARTITION;
        }
        sourcePartition = sourcePartition.replace("-", "_");

        // 1. Collection-specific rules
        Map<String, String> collectionRules = routingTable.get(sourceCollection);
        if (collectionRules != null) {
            String target = collectionRules.get(sourcePartition);
            if (target != null) {
                return target;
            }
            target = collectionRules.get(WILDCARD);
            if (target != null) {
                return target;
            }
        }

        // 2. Global wildcard rules
        Map<String, String> globalRules = routingTable.get(WILDCARD);
        if (globalRules != null) {
            String target = globalRules.get(sourcePartition);
            if (target != null) {
                return target;
            }
            target = globalRules.get(WILDCARD);
            if (target != null) {
                return target;
            }
        }

        // 3. No match - preserve original
        return sourcePartition;
    }
}
