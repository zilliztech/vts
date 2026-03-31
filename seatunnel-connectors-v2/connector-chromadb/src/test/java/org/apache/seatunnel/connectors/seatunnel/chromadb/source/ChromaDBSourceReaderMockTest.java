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

    private ChromaDBSourceReader createReaderWithMockClient(ReadonlyConfig config) throws Exception {
        when(mockCollector.getCheckpointLock()).thenReturn(checkpointLock);

        LinkedHashMap<String, CatalogTable> tables = new LinkedHashMap<>();
        tables.put(COLLECTION_ID, catalogTable);

        ChromaDBSourceReader reader = new ChromaDBSourceReader(config, mockContext, tables);
        Field clientField = ChromaDBSourceReader.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(reader, mockClient);

        reader.addSplits(
                Collections.singletonList(
                        new ChromaDBSourceSplit(COLLECTION_ID, catalogTable.getTablePath())));
        return reader;
    }

    private ReadonlyConfig makeConfig(int idSplitSize, int batchSize) {
        Map<String, Object> map = new HashMap<>();
        map.put("url", "http://localhost:8000");
        map.put("id_split_size", idSplitSize);
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
        ChromaDBSourceReader reader = new ChromaDBSourceReader(makeConfig(1000, 100), mockContext, tables);
        Field clientField = ChromaDBSourceReader.class.getDeclaredField("client");
        clientField.setAccessible(true);
        clientField.set(reader, mockClient);
        reader.close();
        verify(mockClient).close();
    }

    // --- Reading with splits ---

    @Test
    void testEmptyCollectionSignalsComplete() throws Exception {
        when(mockContext.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        when(mockClient.getRecords(eq(COLLECTION_ID), any()))
                .thenReturn(result("{\"ids\":[]}"));

        ChromaDBSourceReader reader = createReaderWithMockClient(makeConfig(1000, 100));
        reader.handleNoMoreSplits();
        reader.pollNext(mockCollector);
        reader.pollNext(mockCollector);

        verify(mockContext, atLeastOnce()).signalNoMoreElement();
        verify(mockCollector, never()).collect(any(SeaTunnelRow.class));
    }

    @Test
    void testSingleSplitMultipleBatches() throws Exception {
        when(mockContext.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        when(mockClient.getRecords(eq(COLLECTION_ID), any()))
                .thenReturn(result("{\"ids\":[\"a\",\"b\",\"c\",\"d\",\"e\"]}"))
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

        ChromaDBSourceReader reader = createReaderWithMockClient(makeConfig(1000, 2));
        reader.handleNoMoreSplits();
        reader.pollNext(mockCollector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(mockCollector, times(5)).collect(captor.capture());
        Assertions.assertEquals("a", captor.getAllValues().get(0).getField(0));
        Assertions.assertEquals("e", captor.getAllValues().get(4).getField(0));
    }

    @Test
    void testSplitTerminatesOnEmpty() throws Exception {
        when(mockContext.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        when(mockClient.getRecords(eq(COLLECTION_ID), any()))
                .thenReturn(result("{\"ids\":[\"a\"]}"))
                .thenReturn(
                        result(
                                "{\"ids\":[\"a\"],\"embeddings\":[[1,2,3]],"
                                        + "\"documents\":[\"d1\"],\"metadatas\":[{}]}"));

        ChromaDBSourceReader reader = createReaderWithMockClient(makeConfig(10, 100));
        reader.handleNoMoreSplits();
        reader.pollNext(mockCollector);

        verify(mockCollector, times(1)).collect(any(SeaTunnelRow.class));
    }

    @Test
    void testNullIdsTerminates() throws Exception {
        when(mockContext.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        when(mockClient.getRecords(eq(COLLECTION_ID), any())).thenReturn(result("{}"));

        ChromaDBSourceReader reader = createReaderWithMockClient(makeConfig(10, 100));
        reader.handleNoMoreSplits();
        reader.pollNext(mockCollector);

        verify(mockCollector, never()).collect(any(SeaTunnelRow.class));
    }

    @Test
    void testEmitRowsWithNullOptionalFields() throws Exception {
        when(mockContext.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        when(mockClient.getRecords(eq(COLLECTION_ID), any()))
                .thenReturn(result("{\"ids\":[\"a\",\"b\"]}"))
                .thenReturn(result("{\"ids\":[\"a\",\"b\"]}"));

        ChromaDBSourceReader reader = createReaderWithMockClient(makeConfig(1000, 100));
        reader.handleNoMoreSplits();
        reader.pollNext(mockCollector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(mockCollector, times(2)).collect(captor.capture());

        SeaTunnelRow row = captor.getAllValues().get(0);
        Assertions.assertEquals("a", row.getField(0));
        Assertions.assertNull(row.getField(1));
        Assertions.assertNull(row.getField(2));
    }

    // --- Multi id-split: first split is exactly id_split_size, triggers second split ---

    @Test
    void testMultipleIdSplits() throws Exception {
        when(mockContext.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        // id_split_size=3: first split returns exactly 3 IDs → triggers second split
        when(mockClient.getRecords(eq(COLLECTION_ID), any()))
                // Phase 1, split 1: scan IDs (offset=0, limit=3) → exactly 3
                .thenReturn(result("{\"ids\":[\"a\",\"b\",\"c\"]}"))
                // Phase 2, split 1: batch fetch (ids=[a,b,c])
                .thenReturn(
                        result(
                                "{\"ids\":[\"a\",\"b\",\"c\"],\"embeddings\":[[1,2,3],[4,5,6],[7,8,9]],"
                                        + "\"documents\":[\"d1\",\"d2\",\"d3\"],\"metadatas\":[{},{},{}]}"))
                // Phase 1, split 2: scan IDs (offset=3, limit=3) → 2 (less than 3 → last split)
                .thenReturn(result("{\"ids\":[\"d\",\"e\"]}"))
                // Phase 2, split 2: batch fetch (ids=[d,e])
                .thenReturn(
                        result(
                                "{\"ids\":[\"d\",\"e\"],\"embeddings\":[[10,11,12],[13,14,15]],"
                                        + "\"documents\":[\"d4\",\"d5\"],\"metadatas\":[{},{}]}"));

        ChromaDBSourceReader reader = createReaderWithMockClient(makeConfig(3, 100));
        reader.handleNoMoreSplits();
        reader.pollNext(mockCollector);

        ArgumentCaptor<SeaTunnelRow> captor = ArgumentCaptor.forClass(SeaTunnelRow.class);
        verify(mockCollector, times(5)).collect(captor.capture());
        Assertions.assertEquals("a", captor.getAllValues().get(0).getField(0));
        Assertions.assertEquals("e", captor.getAllValues().get(4).getField(0));
    }

    // --- Config validation: zero/negative values ---

    @Test
    void testZeroBatchSizeThrows() {
        LinkedHashMap<String, CatalogTable> tables = new LinkedHashMap<>();
        tables.put(COLLECTION_ID, catalogTable);
        ChromaDBSourceReader reader =
                new ChromaDBSourceReader(makeConfig(1000, 0), mockContext, tables);

        Assertions.assertThrows(
                ChromaDBConnectorException.class,
                () -> reader.open());
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

        Assertions.assertThrows(
                ChromaDBConnectorException.class,
                () -> reader.open());
    }
}
