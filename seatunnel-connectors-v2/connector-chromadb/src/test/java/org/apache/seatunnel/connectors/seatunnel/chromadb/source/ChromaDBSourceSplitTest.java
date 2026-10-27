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

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChromaDBSourceSplitTest {
    private final TablePath path = TablePath.of("db", "col");

    @Test
    void rangeIdentityDoesNotChangeWithProgress() {
        ChromaDBSourceSplit split = new ChromaDBSourceSplit("col-id", path, 100, 105);
        ChromaDBSourceSplit progressed =
                split.withPendingIds(Arrays.asList("a", "b", "c", "d", "e")).advance(2);
        assertEquals("col-id:100:105", progressed.splitId());
        assertEquals(split.splitId(), progressed.splitId());
        assertNotEquals(
                split.splitId(), new ChromaDBSourceSplit("col-id", path, 105, 110).splitId());
        assertEquals(102, progressed.getNextOffset());
        assertEquals(100, split.getNextOffset());
        assertEquals(path, progressed.getTablePath());
    }

    @Test
    void checkpointRetainsOnlyUnreadIdsAndRoundTrips() throws Exception {
        ChromaDBSourceSplit split =
                new ChromaDBSourceSplit("col-id", path, 100, 105)
                        .withPendingIds(Arrays.asList("a", "b", "c", "d", "e"))
                        .advance(2);
        ChromaDBSourceSplit snapshot = split.copy();
        assertEquals(Arrays.asList("c", "d", "e"), snapshot.getPendingIds());
        assertEquals(0, snapshot.getPendingIdIndex());
        DefaultSerializer<ChromaDBSourceSplit> serializer = new DefaultSerializer<>();
        assertEquals(snapshot, serializer.deserialize(serializer.serialize(snapshot)));
        ChromaDBSourceSplit completed = split.advance(3);
        assertTrue(completed.getPendingIds().isEmpty());
        assertEquals(105, completed.getNextOffset());
        assertEquals(102, snapshot.getNextOffset());
        assertEquals(Arrays.asList("c", "d", "e"), snapshot.getPendingIds());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.getPendingIds().clear());
    }

    @Test
    void invalidRangesAndProgressAreRejected() {
        assertThrows(
                IllegalArgumentException.class, () -> new ChromaDBSourceSplit("id", path, -1, 3));
        assertThrows(
                IllegalArgumentException.class, () -> new ChromaDBSourceSplit("id", path, 3, 3));
        ChromaDBSourceSplit split = new ChromaDBSourceSplit("id", path, 1, 2);
        assertThrows(
                IllegalArgumentException.class,
                () -> split.withPendingIds(Arrays.asList("a", "b")));
        assertThrows(
                IllegalArgumentException.class,
                () -> split.withPendingIds(Collections.singletonList("a")).advance(2));
    }
}
