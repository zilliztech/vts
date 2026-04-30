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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.source;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SourceConfig;

import lombok.Getter;
import lombok.ToString;

/**
 * One split per ES shard. Holds:
 * <ul>
 *   <li>a shared reference to the source's {@link SourceConfig} — user-facing
 *       configuration, common to every split from the same source;</li>
 *   <li>split-specific runtime metadata resolved by the enumerator
 *       ({@code concreteIndex}, {@code shardId}, {@code nodeId},
 *       {@code dataNodeAddress}).</li>
 * </ul>
 * Split-specific fields do NOT belong on {@code SourceConfig} because they
 * are not user-configurable and vary per-split; keeping them here lets
 * multiple splits share a single config instance (effectively read-only
 * after enumeration completes).
 */
@ToString
public class ElasticsearchSourceSplit implements SourceSplit {

    // First explicit version (was -1L in master). Fields expanded for
    // shard-based splitting; old-format checkpoints cannot deserialize into
    // this class — restart the job from scratch after upgrading.
    private static final long serialVersionUID = 1L;

    @Getter private final SourceConfig sourceConfig;

    /** Concrete index this split queries — resolved from {@code SourceConfig.index}
     *  (which may be a wildcard) via {@code _cat/indices}. */
    @Getter private final String concreteIndex;

    /** ES shard id (0-based). */
    @Getter private final int shardId;

    /** ES node id that hosts the primary of this shard. Used to pin search execution
     *  via {@code _prefer_nodes} so ES's adaptive replica selection doesn't route to
     *  a replica on another node, which would undo the direct-connect optimization.
     *  {@code null} when {@code _search_shards} didn't report one. */
    @Getter private final String nodeId;

    /** Resolved data-node HTTP address (host:port). {@code null} when {@code wan_only=true}
     *  or the {@code _nodes/http} lookup failed; reader falls back to the default client. */
    @Getter private final String dataNodeAddress;

    // Computed in the constructor from (concreteIndex, shardId) — not serialized
    // as a free-standing field for clarity, but stored to avoid per-call concat.
    // No @Getter — SourceSplit interface exposes splitId() (no get- prefix),
    // which is overridden explicitly below.
    private final String splitId;

    /** {@code preference=} query param: pins the shard AND soft-prefers the primary
     *  node when known. Soft (not hard) so ES can still fall back to a replica copy
     *  if the primary is briefly unavailable. */
    @Getter private final String preference;

    public ElasticsearchSourceSplit(
            SourceConfig sourceConfig,
            String concreteIndex,
            int shardId,
            String nodeId,
            String dataNodeAddress) {
        this.sourceConfig = sourceConfig;
        this.concreteIndex = concreteIndex;
        this.shardId = shardId;
        // Normalize missing nodeId: Jackson's MissingNode.asText() returns ""
        // (not null) when _search_shards omits the node field (unassigned primary).
        // An empty suffix in "_prefer_nodes:" would make ES reject the preference
        // with HTTP 400.
        this.nodeId = (nodeId == null || nodeId.isEmpty()) ? null : nodeId;
        this.dataNodeAddress = dataNodeAddress;
        this.splitId = concreteIndex + "-shard" + shardId;
        this.preference =
                this.nodeId != null
                        ? "_shards:" + shardId + "|_prefer_nodes:" + this.nodeId
                        : "_shards:" + shardId;
    }

    public SeaTunnelRowType getSeaTunnelRowType() {
        return sourceConfig.getCatalogTable().getSeaTunnelRowType();
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
