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

import com.google.gson.Gson;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.seatunnel.chromadb.client.ChromaDBClient;
import org.apache.seatunnel.connectors.seatunnel.chromadb.exception.ChromaDBConnectorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the Phase-2-only {@link ChromaDBSourceReader}.
 *
 * <p>Splits now arrive pre-populated with their ID shard from the enumerator; the reader's only
 * job is to batch them by {@code batch_size}, fetch full records, and emit. Phase 1 logic no
 * longer lives in the reader.
 */
@ExtendWith(MockitoExtension.class)
public class ChromaDBSourceReaderMockTest {

    private static final Gson GSON = new Gson();
    private static final String COLLECTION_ID = "test-col-id";

    @Mock private ChromaDBClient mockClient;
    @Mock private Collector<SeaTunnelRow> mockCollector;
    @Mock private SourceReader.Context mockContext;

    private CatalogTable catalogTable;
    private final Object checkpointLock = new Object();

    @BeforeEach
    void setup() {
        TableSchema schema =
                TableSchema.builder()
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id")))
                        .columns(
                                Arrays.<Column>asList(
                                        PhysicalColumn.builder()
                                                .name("id")
                                                .dataType(BasicType.STRING_TYPE)
                                                .build(),
                                        PhysicalColumn.builder()
                                                .name("embedding")
                                                .dataType(VectorType.VECTOR_FLOAT_TYPE)
                                                .scale(3)
                                                .build(),
                                        PhysicalColumn.builder()
                                                .name("document")
                                                .dataType(BasicType.STRING_TYPE)
                                                .build(),
                                        PhysicalColumn.builder()
                                                .name(CommonOptions.METADATA.getName())
                                                .dataType(BasicType.STRING_TYPE)
                                                .build()))
                        .build();
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("chromadb", TablePath.of("db", "col")),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");
    }

    private ChromaDBSourceReader createReaderWithMockClient(
            ReadonlyConfig config, List<String> splitIds) throws Exception {
        when(mockCollector.getCheckpointLock()).thenReturn(checkpointLock);

        LinkedHashMap<String, CatalogTable> tables = new LinkedHashMap<>();
        tables.put(COLLECTION_ID, catalogTable);

        ChromaDBSourceReader reader = new ChromaDBSourceReader(config, mockContext, tables);
        Field clientField = ChromaDBSourceReader.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(reader, mockClient);

        reader.addSplits(
                Collections.singletonList(
                        new ChromaDBSourceSplit(
                                COLLECTION_ID, catalogTable.getTablePath(), 0L, splitIds)));
        return reader;
    }

    private ReadonlyConfig makeConfig(int batchSize) {
        Map<String, Object> map = new HashMap<>();
        map.put("url", "http://localhost:8000");
        map.put("batch_size", batchSize);
        return ReadonlyConfig.fromMap(map);
    }

    private ChromaDBClient.GetRecordsResult result(String json) {
        return GSON.fromJson(json, ChromaDBClient.GetRecordsResult.class);
    }

    // --- Lifecycle ---

    @Test
    void testCloseReleasesClient() throws Exception {
        LinkedHashMap<String, CatalogTable> tables = new LinkedHashMap<>();
        tables.put(COLLECTION_ID, catalogTable);
        ChromaDBSourceReader reader =
                new ChromaDBSourceReader(makeConfig(100), mockContext, tables);
        Field clientField = ChromaDBSourceReader.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(reader, mockClient);
        reader.close();
        verify(mockClient).close();
    }

    // --- Reading with splits ---

    @Test
    void testEmptySplitIdsEmitsNothing() throws Exception {
        when(mockContext.getBoundedness()).thenReturn(Boundedness.BOUNDED);

        ChromaDBSourceReader reader =
                createReaderWithMockClient(makeConfig(100), Collections.emptyList());
        reader.handleNoMoreSplits();
        reader.pollNext(mockCollector);
        // Second poll should drain and signal no more element
        reader.pollNext(mockCollector);

        verify(mockContext, atLeastOnce()).signalNoMoreElement();
        verify(mockCollector, never()).collect(any(SeaTunnelRow.class));
        // Empty split should not trigger any client call
        verify(mockClient, never()).getRecords(eq(COLLECTION_ID), any());
    }

    @Test
    void testSingleBatchEmitsAllRows() throws Exception {
        when(mockClient.getRecords(eq(COLLECTION_ID), any()))
                .thenReturn(
                        result(
                                "{\"ids\":[\"a\",\"b\",\"c\"],\"embeddings\":[[1,2,3],[4,5,6],[7,8,9]],"
                                        + "\"documents\":[\"d1\",\"d2\",\"d3\"],\"metadatas\":[{},{},{}]}"));

        ChromaDBSourceReader reader =
                createReaderWithMockClient(makeConfig(100), Arrays.asList("a", "b", "c"));
        reader.handleNoMoreSplits();
        reader.pollNext(mockCollector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(mockCollector, times(3)).collect(captor.capture());
        Assertions.assertEquals("a", captor.getAllValues().get(0).getField(0));
        Assertions.assertEquals("c", captor.getAllValues().get(2).getField(0));
    }

    @Test
    void testMultipleBatchesForLargeSplit() throws Exception {
        // batch_size=2, split has 5 IDs → 3 client calls: [a,b], [c,d], [e]
        when(mockClient.getRecords(eq(COLLECTION_ID), any()))
                .thenReturn(
                        result(
                                "{\"ids\":[\"a\",\"b\"],\"embeddings\":[[1,2,3],[4,5,6]],"
                                        + "\"documents\":[\"d1\",\"d2\"],\"metadatas\":[{},{}]}"))
                .thenReturn(
                        result(
                                "{\"ids\":[\"c\",\"d\"],\"embeddings\":[[7,8,9],[10,11,12]],"
                                        + "\"documents\":[\"d3\",\"d4\"],\"metadatas\":[{},{}]}"))
                .thenReturn(
                        result(
                                "{\"ids\":[\"e\"],\"embeddings\":[[13,14,15]],"
                                        + "\"documents\":[\"d5\"],\"metadatas\":[{}]}"));

        ChromaDBSourceReader reader =
                createReaderWithMockClient(
                        makeConfig(2), Arrays.asList("a", "b", "c", "d", "e"));
        reader.handleNoMoreSplits();
        reader.pollNext(mockCollector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(mockCollector, times(5)).collect(captor.capture());
        Assertions.assertEquals("a", captor.getAllValues().get(0).getField(0));
        Assertions.assertEquals("e", captor.getAllValues().get(4).getField(0));
    }

    @Test
    void testNullResponseThrows() throws Exception {
        when(mockClient.getRecords(eq(COLLECTION_ID), any())).thenReturn(null);

        ChromaDBSourceReader reader =
                createReaderWithMockClient(makeConfig(100), Arrays.asList("a"));
        reader.handleNoMoreSplits();

        Assertions.assertThrows(
                ChromaDBConnectorException.class, () -> reader.pollNext(mockCollector));
    }

    @Test
    void testEmitRowsWithNullOptionalFields() throws Exception {
        // Phase 2 response only has ids, no embeddings/docs/metadatas
        when(mockClient.getRecords(eq(COLLECTION_ID), any()))
                .thenReturn(result("{\"ids\":[\"a\",\"b\"]}"));

        ChromaDBSourceReader reader =
                createReaderWithMockClient(makeConfig(100), Arrays.asList("a", "b"));
        reader.handleNoMoreSplits();
        reader.pollNext(mockCollector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(mockCollector, times(2)).collect(captor.capture());

        SeaTunnelRow row = captor.getAllValues().get(0);
        Assertions.assertEquals("a", row.getField(0));
        Assertions.assertNull(row.getField(1));
        Assertions.assertNull(row.getField(2));
    }

    @Test
    void testEmptyQueueSendsSplitRequest() throws Exception {
        LinkedHashMap<String, CatalogTable> tables = new LinkedHashMap<>();
        tables.put(COLLECTION_ID, catalogTable);
        ChromaDBSourceReader reader =
                new ChromaDBSourceReader(makeConfig(100), mockContext, tables);
        Field clientField = ChromaDBSourceReader.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(reader, mockClient);

        when(mockCollector.getCheckpointLock()).thenReturn(checkpointLock);

        // No splits, noMoreSplit not yet signaled → reader should ask for more
        reader.pollNext(mockCollector);

        verify(mockContext).sendSplitRequest();
    }

    @Test
    void testAddSplitsResetsSplitRequestOutstanding() throws Exception {
        LinkedHashMap<String, CatalogTable> tables = new LinkedHashMap<>();
        tables.put(COLLECTION_ID, catalogTable);
        ChromaDBSourceReader reader =
                new ChromaDBSourceReader(makeConfig(100), mockContext, tables);
        Field clientField = ChromaDBSourceReader.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(reader, mockClient);

        when(mockCollector.getCheckpointLock()).thenReturn(checkpointLock);
        when(mockClient.getRecords(eq(COLLECTION_ID), any()))
                .thenReturn(result("{\"ids\":[\"a\"]}"));

        // First poll: empty queue → sendSplitRequest
        reader.pollNext(mockCollector);
        verify(mockContext, times(1)).sendSplitRequest();

        // addSplits should reset the outstanding flag and process the split
        reader.addSplits(
                Collections.singletonList(
                        new ChromaDBSourceSplit(
                                COLLECTION_ID,
                                catalogTable.getTablePath(),
                                0L,
                                Collections.singletonList("a"))));
        reader.pollNext(mockCollector);

        // After processing, queue is empty again → one more sendSplitRequest
        reader.pollNext(mockCollector);
        verify(mockContext, times(2)).sendSplitRequest();
    }

    // --- Config validation ---

    @Test
    void testZeroBatchSizeThrows() {
        LinkedHashMap<String, CatalogTable> tables = new LinkedHashMap<>();
        tables.put(COLLECTION_ID, catalogTable);
        ChromaDBSourceReader reader =
                new ChromaDBSourceReader(makeConfig(0), mockContext, tables);

        Assertions.assertThrows(ChromaDBConnectorException.class, reader::open);
    }

    @Test
    void testNegativeIdSplitSizeThrows() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", "http://localhost:8000");
        map.put("id_split_size", -1);
        map.put("batch_size", 100);
        ReadonlyConfig config = ReadonlyConfig.fromMap(map);

        LinkedHashMap<String, CatalogTable> tables = new LinkedHashMap<>();
        tables.put(COLLECTION_ID, catalogTable);
        ChromaDBSourceReader reader = new ChromaDBSourceReader(config, mockContext, tables);

        Assertions.assertThrows(ChromaDBConnectorException.class, reader::open);
    }
}
