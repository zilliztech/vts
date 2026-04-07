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

package org.apache.seatunnel.connectors.seatunnel.chromadb.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.connectors.seatunnel.chromadb.config.ChromaDBSourceConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ChromaDBSourceFactoryTest {

    private final ChromaDBSourceFactory factory = new ChromaDBSourceFactory();

    @Test
    public void testFactoryIdentifier() {
        Assertions.assertEquals("ChromaDB", factory.factoryIdentifier());
    }

    @Test
    public void testSourceClass() {
        Assertions.assertEquals(ChromaDBSource.class, factory.getSourceClass());
    }

    @Test
    public void testOptionRuleHasRequiredOptions() {
        OptionRule rule = factory.optionRule();
        Assertions.assertNotNull(rule);

        // url and collections should be required
        Assertions.assertTrue(
                rule.getRequiredOptions().stream()
                        .flatMap(o -> o.getOptions().stream())
                        .anyMatch(o -> o.key().equals("url")),
                "url should be a required option");
        Assertions.assertTrue(
                rule.getRequiredOptions().stream()
                        .flatMap(o -> o.getOptions().stream())
                        .anyMatch(o -> o.key().equals("collections")),
                "collections should be a required option");
    }

    @Test
    public void testOptionRuleHasOptionalOptions() {
        OptionRule rule = factory.optionRule();

        Assertions.assertTrue(
                rule.getOptionalOptions().stream()
                        .anyMatch(o -> o.key().equals("token")),
                "token should be optional");
        Assertions.assertTrue(
                rule.getOptionalOptions().stream()
                        .anyMatch(o -> o.key().equals("tenant")),
                "tenant should be optional");
        Assertions.assertTrue(
                rule.getOptionalOptions().stream()
                        .anyMatch(o -> o.key().equals("database")),
                "database should be optional");
        Assertions.assertTrue(
                rule.getOptionalOptions().stream()
                        .anyMatch(o -> o.key().equals("id_split_size")),
                "id_split_size should be optional");
        Assertions.assertTrue(
                rule.getOptionalOptions().stream()
                        .anyMatch(o -> o.key().equals("batch_size")),
                "batch_size should be optional");
    }

    @Test
    public void testConfigDefaults() {
        Assertions.assertEquals("", ChromaDBSourceConfig.TOKEN.defaultValue());
        Assertions.assertEquals("default_tenant", ChromaDBSourceConfig.TENANT.defaultValue());
        Assertions.assertEquals("default_database", ChromaDBSourceConfig.DATABASE.defaultValue());
        Assertions.assertEquals(1000, ChromaDBSourceConfig.BATCH_SIZE.defaultValue());
    }
}
