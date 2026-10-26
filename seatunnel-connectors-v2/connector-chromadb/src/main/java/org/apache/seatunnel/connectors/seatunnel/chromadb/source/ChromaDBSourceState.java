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

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Enumerator state for checkpoint/restore.
 *
 * <p>Granularity is per-collection: if a job fails mid-collection, the whole collection is
 * re-read on restart. This assumes the sink is idempotent (upsert-by-ID), which is the case for
 * Milvus and most vector stores. Finer-grained recovery can be added later if needed.
 */
@Data
@AllArgsConstructor
public class ChromaDBSourceState implements Serializable {
    /** Collections that have been fully processed — skipped on restart. */
    private final Set<String> completedCollectionIds;

    /** Splits generated but not yet successfully assigned (rare; kept for completeness). */
    private final List<ChromaDBSourceSplit> pendingSplits;

    /** Splits currently assigned to readers; re-queued on reader failure. */
    private final Map<Integer, List<ChromaDBSourceSplit>> assignedSplits;
}
