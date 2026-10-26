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

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ChromaDBSourceSplitTest {

    @Test
    void testSplitIdEncodesCollectionAndSequence() {
        ChromaDBSourceSplit split =
                new ChromaDBSourceSplit(
                        "uuid-123",
                        TablePath.of("db", "my_col"),
                        0L,
                        Arrays.asList("a", "b"));
        Assertions.assertEquals("uuid-123#0", split.splitId());
        Assertions.assertEquals("uuid-123", split.getCollectionId());
        Assertions.assertEquals(0L, split.getSplitSeq());
    }

    @Test
    void testSplitIdsAreUniquePerSequence() {
        ChromaDBSourceSplit a =
                new ChromaDBSourceSplit(
                        "col", TablePath.of("db", "col"), 0L, Collections.singletonList("x"));
        ChromaDBSourceSplit b =
                new ChromaDBSourceSplit(
                        "col", TablePath.of("db", "col"), 1L, Collections.singletonList("y"));
        Assertions.assertNotEquals(a.splitId(), b.splitId());
        Assertions.assertEquals("col#0", a.splitId());
        Assertions.assertEquals("col#1", b.splitId());
    }

    @Test
    void testTablePathPreserved() {
        TablePath path = TablePath.of("default_database", "test_collection");
        ChromaDBSourceSplit split =
                new ChromaDBSourceSplit(
                        "col-id", path, 5L, Collections.emptyList());
        Assertions.assertEquals(path, split.getTablePath());
        Assertions.assertEquals("test_collection", split.getTablePath().getTableName());
    }

    @Test
    void testIdsAreDefensivelyCopied() {
        java.util.ArrayList<String> mutable = new java.util.ArrayList<>();
        mutable.add("a");
        mutable.add("b");
        ChromaDBSourceSplit split =
                new ChromaDBSourceSplit("col", TablePath.of("db", "col"), 0L, mutable);
        mutable.add("c");
        Assertions.assertEquals(2, split.getIds().size());
        Assertions.assertEquals("a", split.getIds().get(0));
        Assertions.assertEquals("b", split.getIds().get(1));
    }

    @Test
    void testNullIdsBecomeEmptyList() {
        ChromaDBSourceSplit split =
                new ChromaDBSourceSplit("col", TablePath.of("db", "col"), 0L, null);
        Assertions.assertNotNull(split.getIds());
        Assertions.assertTrue(split.getIds().isEmpty());
    }

    @Test
    void testEqualityWithSameFields() {
        TablePath path = TablePath.of("db", "col");
        List<String> ids = Arrays.asList("a", "b");
        ChromaDBSourceSplit a = new ChromaDBSourceSplit("col", path, 0L, ids);
        ChromaDBSourceSplit b = new ChromaDBSourceSplit("col", path, 0L, ids);
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void testInequalityOnDifferentSequence() {
        TablePath path = TablePath.of("db", "col");
        ChromaDBSourceSplit a =
                new ChromaDBSourceSplit("col", path, 0L, Collections.singletonList("a"));
        ChromaDBSourceSplit b =
                new ChromaDBSourceSplit("col", path, 1L, Collections.singletonList("a"));
        Assertions.assertNotEquals(a, b);
    }
}
