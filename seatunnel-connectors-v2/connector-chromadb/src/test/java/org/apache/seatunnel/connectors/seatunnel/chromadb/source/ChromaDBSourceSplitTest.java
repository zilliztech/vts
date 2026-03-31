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

public class ChromaDBSourceSplitTest {

    @Test
    void testSplitIdReturnsCollectionId() {
        ChromaDBSourceSplit split =
                new ChromaDBSourceSplit("uuid-123", TablePath.of("db", "my_col"));
        Assertions.assertEquals("uuid-123", split.splitId());
        Assertions.assertEquals("uuid-123", split.getCollectionId());
    }

    @Test
    void testTablePath() {
        TablePath path = TablePath.of("default_database", "test_collection");
        ChromaDBSourceSplit split = new ChromaDBSourceSplit("col-id", path);
        Assertions.assertEquals(path, split.getTablePath());
        Assertions.assertEquals("test_collection", split.getTablePath().getTableName());
    }

    @Test
    void testEquality() {
        TablePath path = TablePath.of("db", "col");
        ChromaDBSourceSplit a = new ChromaDBSourceSplit("id-1", path);
        ChromaDBSourceSplit b = new ChromaDBSourceSplit("id-1", path);
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void testInequality() {
        ChromaDBSourceSplit a =
                new ChromaDBSourceSplit("id-1", TablePath.of("db", "col_a"));
        ChromaDBSourceSplit b =
                new ChromaDBSourceSplit("id-2", TablePath.of("db", "col_b"));
        Assertions.assertNotEquals(a, b);
    }
}
