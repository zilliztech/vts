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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.TablePath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** A fixed offset range and its unconsumed ID page. Progress instances are immutable. */
@Getter
@EqualsAndHashCode
public class ChromaDBSourceSplit implements SourceSplit {
    private static final long serialVersionUID = 1L;

    private final String collectionId;
    private final TablePath tablePath;
    private final long startOffset;
    private final long endOffset;
    private final long nextOffset;
    private final List<String> pendingIds;
    private final int pendingIdIndex;

    public ChromaDBSourceSplit(
            String collectionId, TablePath tablePath, long startOffset, long endOffset) {
        this(
                collectionId,
                tablePath,
                startOffset,
                endOffset,
                startOffset,
                Collections.emptyList(),
                0);
    }

    private ChromaDBSourceSplit(
            String collectionId,
            TablePath tablePath,
            long startOffset,
            long endOffset,
            long nextOffset,
            List<String> pendingIds,
            int pendingIdIndex) {
        if (startOffset < 0
                || endOffset <= startOffset
                || nextOffset < startOffset
                || nextOffset > endOffset
                || pendingIdIndex < 0
                || pendingIdIndex > pendingIds.size()
                || pendingIds.size() - pendingIdIndex > endOffset - nextOffset) {
            throw new IllegalArgumentException("Invalid ChromaDB range or read progress");
        }
        this.collectionId = collectionId;
        this.tablePath = tablePath;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.nextOffset = nextOffset;
        this.pendingIds = pendingIds;
        this.pendingIdIndex = pendingIdIndex;
    }

    ChromaDBSourceSplit withPendingIds(List<String> ids) {
        return new ChromaDBSourceSplit(
                collectionId,
                tablePath,
                startOffset,
                endOffset,
                nextOffset,
                Collections.unmodifiableList(new ArrayList<>(ids)),
                0);
    }

    ChromaDBSourceSplit advance(int count) {
        if (count <= 0 || count > pendingIds.size() - pendingIdIndex) {
            throw new IllegalArgumentException("Invalid number of consumed IDs: " + count);
        }
        int nextIndex = pendingIdIndex + count;
        boolean pageComplete = nextIndex == pendingIds.size();
        return new ChromaDBSourceSplit(
                collectionId,
                tablePath,
                startOffset,
                endOffset,
                nextOffset + count,
                pageComplete ? Collections.emptyList() : pendingIds,
                pageComplete ? 0 : nextIndex);
    }

    /** Checkpoints retain only IDs that have not been emitted. */
    ChromaDBSourceSplit copy() {
        return new ChromaDBSourceSplit(
                collectionId,
                tablePath,
                startOffset,
                endOffset,
                nextOffset,
                Collections.unmodifiableList(
                        new ArrayList<>(pendingIds.subList(pendingIdIndex, pendingIds.size()))),
                0);
    }

    @Override
    public String splitId() {
        return collectionId + ":" + startOffset + ":" + endOffset;
    }
}
