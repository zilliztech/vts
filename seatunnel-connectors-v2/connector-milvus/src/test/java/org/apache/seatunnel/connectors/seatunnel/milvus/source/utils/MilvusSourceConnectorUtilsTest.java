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

package org.apache.seatunnel.connectors.seatunnel.milvus.source.utils;

import io.milvus.v2.common.IndexParam.IndexType;
import io.milvus.v2.common.IndexParam.MetricType;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import org.apache.seatunnel.connectors.seatunnel.milvus.common.MilvusConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MilvusSourceConnectorUtilsTest {

    private DescribeIndexResp.IndexDesc buildIndexDesc(
            IndexType indexType, MetricType metricType, Map<String, String> extraParams) {
        return DescribeIndexResp.IndexDesc.builder()
                .indexName("idx")
                .fieldName("field")
                .indexType(indexType)
                .metricType(metricType)
                .extraParams(extraParams)
                .build();
    }

    @Test
    void testPutIndexType_knownTypePassedThrough() {
        Map<String, String> indexInfo = new HashMap<>();
        MilvusSourceConnectorUtils.putIndexType(indexInfo,
                buildIndexDesc(IndexType.IVF_FLAT, MetricType.COSINE,
                        Collections.singletonMap("nlist", "1024")));
        Assertions.assertEquals("IVF_FLAT", indexInfo.get(MilvusConstants.INDEX_TYPE));
    }

    @Test
    void testPutIndexType_noneWithEmptyParamsOmitted() {
        // Index created without index_type (empty params): omit the type so the sink
        // lets the Milvus Java SDK apply its default (AUTOINDEX)
        Map<String, String> indexInfo = new HashMap<>();
        MilvusSourceConnectorUtils.putIndexType(indexInfo,
                buildIndexDesc(IndexType.None, MetricType.INVALID, Collections.emptyMap()));
        Assertions.assertFalse(indexInfo.containsKey(MilvusConstants.INDEX_TYPE));
    }

    @Test
    void testPutIndexType_noneWithNullExtraParamsOmitted() {
        Map<String, String> indexInfo = new HashMap<>();
        MilvusSourceConnectorUtils.putIndexType(indexInfo,
                buildIndexDesc(IndexType.None, MetricType.INVALID, null));
        Assertions.assertFalse(indexInfo.containsKey(MilvusConstants.INDEX_TYPE));
    }

    @Test
    void testPutIndexType_nullIndexTypeOmitted() {
        Map<String, String> indexInfo = new HashMap<>();
        MilvusSourceConnectorUtils.putIndexType(indexInfo,
                buildIndexDesc(null, MetricType.INVALID, Collections.emptyMap()));
        Assertions.assertFalse(indexInfo.containsKey(MilvusConstants.INDEX_TYPE));
    }

    @Test
    void testPutIndexType_noneWithMetricTypeKeptForLoudFailure() {
        // SDK mapped the type to None but a metric type survived: the source index type
        // is unrecognized by this SDK. Keep the None sentinel so the sink fails loudly
        // instead of silently downgrading the index to AUTOINDEX.
        Map<String, String> indexInfo = new HashMap<>();
        MilvusSourceConnectorUtils.putIndexType(indexInfo,
                buildIndexDesc(IndexType.None, MetricType.COSINE, Collections.emptyMap()));
        Assertions.assertEquals("None", indexInfo.get(MilvusConstants.INDEX_TYPE));
    }

    @Test
    void testPutIndexType_noneWithExtraParamsKeptForLoudFailure() {
        Map<String, String> indexInfo = new HashMap<>();
        MilvusSourceConnectorUtils.putIndexType(indexInfo,
                buildIndexDesc(IndexType.None, MetricType.INVALID,
                        Collections.singletonMap("nlist", "1024")));
        Assertions.assertEquals("None", indexInfo.get(MilvusConstants.INDEX_TYPE));
    }
}
