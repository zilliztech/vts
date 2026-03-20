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

package org.apache.seatunnel.connectors.seatunnel.milvus.source;

import io.milvus.orm.iterator.QueryIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MilvusBufferReaderTest {

    private MilvusClientV2 mockClient;
    private QueryIterator mockIterator;
    private Collector<SeaTunnelRow> mockCollector;
    private TableSchema tableSchema;
    private MilvusSourceSplit split;

    @BeforeEach
    void setUp() {
        mockClient = mock(MilvusClientV2.class);
        mockIterator = mock(QueryIterator.class);
        mockCollector = mock(Collector.class);

        List<Column> columns = Arrays.asList(
                PhysicalColumn.builder()
                        .name("id")
                        .dataType(BasicType.LONG_TYPE)
                        .build(),
                PhysicalColumn.builder()
                        .name("embedding")
                        .dataType(VectorType.VECTOR_FLOAT_TYPE)
                        .build());

        tableSchema = TableSchema.builder().columns(columns).build();

        split = MilvusSourceSplit.builder()
                .tablePath(TablePath.of("default", "test_collection"))
                .splitId("test-split-1")
                .collectionName("test_collection")
                .build();

        when(mockClient.getLoadState(any())).thenReturn(true);
        when(mockClient.queryIterator(any(QueryIteratorReq.class))).thenReturn(mockIterator);
    }

    private MilvusBufferReader createReader() {
        MilvusBufferReader reader = new MilvusBufferReader(split, mockCollector, mockClient, tableSchema);
        reader.setRateLimitRetryIntervalMs(0);
        return reader;
    }

    private QueryResultsWrapper.RowRecord createRecord(long id) {
        QueryResultsWrapper.RowRecord record = new QueryResultsWrapper.RowRecord();
        record.put("id", id);
        record.put("embedding", Arrays.asList(0.1f, 0.2f, 0.3f));
        return record;
    }

    @Test
    void testNormalReadAllData() {
        QueryResultsWrapper.RowRecord record1 = createRecord(1L);
        QueryResultsWrapper.RowRecord record2 = createRecord(2L);

        when(mockIterator.next())
                .thenReturn(Arrays.asList(record1, record2))
                .thenReturn(Collections.emptyList());

        MilvusBufferReader reader = createReader();
        reader.pollData(100);

        verify(mockCollector, times(2)).collect(any(SeaTunnelRow.class));
    }

    @Test
    void testIntermittentRateLimitThenRecover() {
        QueryResultsWrapper.RowRecord record1 = createRecord(1L);
        QueryResultsWrapper.RowRecord record2 = createRecord(2L);

        when(mockIterator.next())
                .thenReturn(Collections.singletonList(record1))
                .thenThrow(new RuntimeException("rate limit exceeded"))
                .thenReturn(Collections.singletonList(record2))
                .thenReturn(Collections.emptyList());

        MilvusBufferReader reader = createReader();
        reader.pollData(100);

        verify(mockCollector, times(2)).collect(any(SeaTunnelRow.class));
    }

    @Test
    void testConsecutiveRateLimitExhaustsRetries() {
        QueryResultsWrapper.RowRecord record1 = createRecord(1L);

        when(mockIterator.next())
                .thenReturn(Collections.singletonList(record1))
                .thenThrow(new RuntimeException("rate limit exceeded"))
                .thenThrow(new RuntimeException("rate limit exceeded"))
                .thenThrow(new RuntimeException("rate limit exceeded"));

        MilvusBufferReader reader = createReader();

        assertThrows(MilvusConnectorException.class, () -> reader.pollData(100));

        verify(mockCollector, times(1)).collect(any(SeaTunnelRow.class));
    }

    @Test
    void testNonRateLimitExceptionThrowsImmediately() {
        when(mockIterator.next())
                .thenThrow(new RuntimeException("connection refused"));

        MilvusBufferReader reader = createReader();

        assertThrows(MilvusConnectorException.class, () -> reader.pollData(100));

        verify(mockCollector, times(0)).collect(any(SeaTunnelRow.class));
    }

    @Test
    void testRetryCounterResetsAfterSuccess() {
        QueryResultsWrapper.RowRecord record = createRecord(1L);

        when(mockIterator.next())
                .thenThrow(new RuntimeException("rate limit exceeded"))
                .thenThrow(new RuntimeException("rate limit exceeded"))
                .thenReturn(Collections.singletonList(record))
                .thenThrow(new RuntimeException("rate limit exceeded"))
                .thenThrow(new RuntimeException("rate limit exceeded"))
                .thenReturn(Collections.singletonList(record))
                .thenReturn(Collections.emptyList());

        MilvusBufferReader reader = createReader();
        reader.pollData(100);

        verify(mockCollector, times(2)).collect(any(SeaTunnelRow.class));
    }
}
