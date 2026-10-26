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

import lombok.Data;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.TablePath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A split represents one shard of Phase-2 work for a single collection.
 *
 * <p>The enumerator owns Phase 1 (scanning IDs via offset/limit) and partitions each batch of
 * scanned IDs into N sub-splits (N = reader parallelism). Each sub-split carries its concrete
 * ID list; readers only execute Phase 2 (fetching full records by ID list).
 *
 * <p>The {@code splitId} is made unique across sub-splits via {@code collectionId + "#" + splitSeq}
 * so that the framework's tracking (logging, checkpoint bookkeeping) stays consistent when many
 * sub-splits belong to the same collection.
 */
@Data
public class ChromaDBSourceSplit implements SourceSplit {

    private final String collectionId;
    private final TablePath tablePath;
    /** Monotonic sequence number per collection, used to derive a unique splitId. */
    private final long splitSeq;
    /** The ID shard this split is responsible for. May be empty for degenerate partitions. */
    private final List<String> ids;

    public ChromaDBSourceSplit(
            String collectionId, TablePath tablePath, long splitSeq, List<String> ids) {
        this.collectionId = collectionId;
        this.tablePath = tablePath;
        this.splitSeq = splitSeq;
        // Defensive copy: the caller may pass a subList view which is not Serializable.
        this.ids = (ids == null) ? Collections.emptyList() : new ArrayList<>(ids);
    }

    @Override
    public String splitId() {
        return collectionId + "#" + splitSeq;
    }
}
