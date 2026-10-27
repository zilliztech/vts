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
import com.google.gson.JsonObject;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.chromadb.client.ChromaDBClient;
import org.apache.seatunnel.connectors.seatunnel.chromadb.exception.ChromaDBConnectorException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ChromaDBSourceReaderMockTest {
    private static final Gson GSON = new Gson();
    private static final String COLLECTION_ID = "col-id";
    private static final TablePath PATH = TablePath.of("db", "col");
    private final Object checkpointLock = new Object();
    private final List<JsonObject> requests = new ArrayList<>();
    private final List<String> emitted = new ArrayList<>();
    private ChromaDBClient client;
    private Collector<SeaTunnelRow> collector;
    private SourceReader.Context context;
    private CatalogTable table;

    @BeforeEach
    void setup() {
        client = mock(ChromaDBClient.class);
        collector = mock(Collector.class);
        context = mock(SourceReader.Context.class);
        when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        when(collector.getCheckpointLock()).thenReturn(checkpointLock);
        doAnswer(
                        call -> {
                            assertTrue(Thread.holdsLock(checkpointLock));
                            SeaTunnelRow row = call.getArgument(0);
                            emitted.add((String) row.getField(0));
                            return null;
                        })
                .when(collector)
                .collect(any(SeaTunnelRow.class));
        TableSchema schema =
                TableSchema.builder()
                        .columns(
                                Collections.singletonList(
                                        PhysicalColumn.builder()
                                                .name("id")
                                                .dataType(BasicType.STRING_TYPE)
                                                .build()))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of("chromadb", PATH),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");
    }

    private ChromaDBSourceReader reader(int idSize, int batchSize, ChromaDBSourceSplit... splits)
            throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("url", "http://localhost:8000");
        config.put("id_split_size", idSize);
        config.put("batch_size", batchSize);
        ChromaDBSourceReader reader =
                new ChromaDBSourceReader(
                        ReadonlyConfig.fromMap(config),
                        context,
                        Collections.singletonMap(COLLECTION_ID, table));
        Field field = ChromaDBSourceReader.class.getDeclaredField("client");
        field.setAccessible(true);
        field.set(reader, client);
        reader.addSplits(Arrays.asList(splits));
        return reader;
    }

    private ChromaDBSourceSplit split(long start, long end) {
        return new ChromaDBSourceSplit(COLLECTION_ID, PATH, start, end);
    }

    private ChromaDBClient.GetRecordsResult result(List<String> ids) {
        return GSON.fromJson(
                GSON.toJson(Collections.singletonMap("ids", ids)),
                ChromaDBClient.GetRecordsResult.class);
    }

    private void stubDataset() {
        when(client.getRecords(eq(COLLECTION_ID), any()))
                .thenAnswer(
                        call -> {
                            assertFalse(
                                    Thread.holdsLock(checkpointLock),
                                    "Network request must not hold the checkpoint lock");
                            JsonObject request =
                                    GSON.toJsonTree(call.getArgument(1)).getAsJsonObject();
                            requests.add(request);
                            List<String> ids = new ArrayList<>();
                            if (request.has("ids")) {
                                request.getAsJsonArray("ids")
                                        .forEach(id -> ids.add(id.getAsString()));
                            } else {
                                assertEquals(0, request.getAsJsonArray("include").size());
                                long offset = request.get("offset").getAsLong();
                                int limit = request.get("limit").getAsInt();
                                ids.addAll(
                                        LongStream.range(offset, offset + limit)
                                                .mapToObj(Long::toString)
                                                .collect(Collectors.toList()));
                            }
                            return result(ids);
                        });
    }

    private void drain(ChromaDBSourceReader reader) throws Exception {
        reader.handleNoMoreSplits();
        for (int i = 0; i < 100 && !reader.snapshotState(1).isEmpty(); i++) {
            reader.pollNext(collector);
        }
        assertTrue(reader.snapshotState(1).isEmpty(), "Reader should exhaust its assigned ranges");
        reader.pollNext(collector);
    }

    private List<JsonObject> scans() {
        return requests.stream().filter(r -> r.has("offset")).collect(Collectors.toList());
    }

    @Test
    void largeIdPageUsesOneScanAndMultipleDataBatches() throws Exception {
        stubDataset();
        ChromaDBSourceReader reader = reader(10000000, 2, split(0, 5));
        drain(reader);
        assertEquals(Arrays.asList("0", "1", "2", "3", "4"), emitted);
        assertEquals(1, scans().size());
        assertEquals(5, scans().get(0).get("limit").getAsInt());
        assertEquals(4, requests.size());
        verify(context, times(1)).signalNoMoreElement();
        verify(context, never()).sendSplitRequest();
    }

    @Test
    void scansRespectNonzeroRangeAndTruncateTheFinalPage() throws Exception {
        stubDataset();
        drain(reader(5, 2, split(6, 13)));
        assertEquals(
                LongStream.range(6, 13).mapToObj(Long::toString).collect(Collectors.toList()),
                emitted);
        assertEquals(2, scans().size());
        assertEquals(6, scans().get(0).get("offset").getAsLong());
        assertEquals(5, scans().get(0).get("limit").getAsInt());
        assertEquals(11, scans().get(1).get("offset").getAsLong());
        assertEquals(2, scans().get(1).get("limit").getAsInt());
    }

    @Test
    void offsetsAboveIntegerMaxRemainExact() throws Exception {
        stubDataset();
        long start = (long) Integer.MAX_VALUE + 1;
        drain(reader(300, 300, split(start, start + 2)));
        assertEquals(start, scans().get(0).get("offset").getAsLong());
        assertEquals(Arrays.asList(Long.toString(start), Long.toString(start + 1)), emitted);
    }

    @Test
    void checkpointInMiddleOfPageRestoresUnreadIdsWithoutRescanning() throws Exception {
        stubDataset();
        ChromaDBSourceReader first = reader(5, 2, split(0, 8));
        first.pollNext(collector);
        ChromaDBSourceSplit checkpoint = first.snapshotState(1).get(0);
        assertEquals(2, checkpoint.getNextOffset());
        assertEquals(Arrays.asList("2", "3", "4"), checkpoint.getPendingIds());
        DefaultSerializer<ChromaDBSourceSplit> serializer = new DefaultSerializer<>();
        ChromaDBSourceSplit recovered = serializer.deserialize(serializer.serialize(checkpoint));
        ChromaDBSourceReader restored = reader(5, 2, recovered);
        drain(restored);
        assertEquals(
                LongStream.range(0, 8).mapToObj(Long::toString).collect(Collectors.toList()),
                emitted);
        assertEquals(
                Arrays.asList(0L, 5L),
                scans().stream()
                        .map(r -> r.get("offset").getAsLong())
                        .collect(Collectors.toList()));
        assertEquals(2, checkpoint.getNextOffset());
        assertEquals(Arrays.asList("2", "3", "4"), checkpoint.getPendingIds());
    }

    @Test
    void incompleteIdPageFailsInsteadOfFinishingSilently() throws Exception {
        for (List<String> ids :
                Arrays.asList(Collections.<String>emptyList(), Collections.singletonList("0"))) {
            when(client.getRecords(eq(COLLECTION_ID), any())).thenReturn(result(ids));
            ChromaDBSourceReader reader = reader(300, 300, split(0, 2));
            reader.handleNoMoreSplits();
            assertThrows(ChromaDBConnectorException.class, () -> reader.pollNext(collector));
            assertEquals(0, reader.snapshotState(1).get(0).getNextOffset());
        }
        assertTrue(emitted.isEmpty());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void nullIdResponseFailsWithoutAdvancing() throws Exception {
        ChromaDBSourceReader reader = reader(300, 300, split(0, 2));
        assertThrows(ChromaDBConnectorException.class, () -> reader.pollNext(collector));
        assertEquals(0, reader.snapshotState(1).get(0).getNextOffset());
    }

    @Test
    void missingOrUnexpectedFullRecordsFailBeforeEmitting() throws Exception {
        for (List<String> returned :
                Arrays.asList(
                        Collections.singletonList("0"),
                        Arrays.asList("0", "other"),
                        Arrays.asList("0", "0"))) {
            when(client.getRecords(eq(COLLECTION_ID), any()))
                    .thenReturn(result(Arrays.asList("0", "1")))
                    .thenReturn(result(returned));
            ChromaDBSourceReader reader = reader(300, 300, split(0, 2));
            assertThrows(ChromaDBConnectorException.class, () -> reader.pollNext(collector));
            assertEquals(0, reader.snapshotState(1).get(0).getNextOffset());
        }
        assertTrue(emitted.isEmpty());
    }

    @Test
    void fullRecordsMayBeReturnedInADifferentOrder() throws Exception {
        when(client.getRecords(eq(COLLECTION_ID), any()))
                .thenReturn(result(Arrays.asList("0", "1")))
                .thenReturn(result(Arrays.asList("1", "0")));
        drain(reader(300, 300, split(0, 2)));
        assertEquals(Arrays.asList("1", "0"), emitted);
    }

    @Test
    void failedOutputDoesNotAdvanceBatchProgress() throws Exception {
        stubDataset();
        doThrow(new IllegalStateException("sink rejected row"))
                .when(collector)
                .collect(any(SeaTunnelRow.class));
        ChromaDBSourceReader reader = reader(5, 2, split(0, 5));
        assertThrows(ChromaDBConnectorException.class, () -> reader.pollNext(collector));
        ChromaDBSourceSplit checkpoint = reader.snapshotState(1).get(0);
        assertEquals(0, checkpoint.getNextOffset());
        assertEquals(Arrays.asList("0", "1", "2", "3", "4"), checkpoint.getPendingIds());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void failureFetchingFullRecordsRetainsTheIdPageForRetry() throws Exception {
        when(client.getRecords(eq(COLLECTION_ID), any()))
                .thenReturn(result(Arrays.asList("0", "1")))
                .thenThrow(new IllegalStateException("network failure"))
                .thenReturn(result(Arrays.asList("0", "1")));
        ChromaDBSourceReader reader = reader(300, 300, split(0, 2));
        assertThrows(IllegalStateException.class, () -> reader.pollNext(collector));
        assertEquals(Arrays.asList("0", "1"), reader.snapshotState(1).get(0).getPendingIds());
        drain(reader);
        assertEquals(Arrays.asList("0", "1"), emitted);
        verify(client, times(3)).getRecords(eq(COLLECTION_ID), any());
    }

    @Test
    void checkpointIsNotBlockedByIdScan() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(client.getRecords(eq(COLLECTION_ID), any()))
                .thenAnswer(
                        call -> {
                            JsonObject request =
                                    GSON.toJsonTree(call.getArgument(1)).getAsJsonObject();
                            if (request.has("offset")) {
                                entered.countDown();
                                assertTrue(release.await(5, TimeUnit.SECONDS));
                            }
                            return result(Collections.singletonList("0"));
                        });
        ChromaDBSourceReader reader = reader(300, 300, split(0, 1));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> reading =
                    executor.submit(
                            () -> {
                                reader.pollNext(collector);
                                return null;
                            });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertTimeoutPreemptively(
                    Duration.ofSeconds(2),
                    () -> {
                        synchronized (checkpointLock) {
                            assertEquals(0, reader.snapshotState(1).get(0).getNextOffset());
                        }
                    });
            release.countDown();
            reading.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void idlePollBeforeAssignmentDoesNotFinishAnUnreadSplit() throws Exception {
        stubDataset();
        ChromaDBSourceReader reader = reader(300, 300);
        reader.pollNext(collector);
        verify(context, never()).signalNoMoreElement();
        reader.addSplits(Collections.singletonList(split(0, 1)));
        reader.handleNoMoreSplits();
        reader.pollNext(collector);
        assertEquals(Collections.singletonList("0"), emitted);
        verify(context, times(1)).signalNoMoreElement();
    }

    @Test
    void readerFinishesOnlyAfterEveryAssignedRange() throws Exception {
        stubDataset();
        ChromaDBSourceReader reader = reader(300, 300, split(0, 1), split(5, 6));
        reader.handleNoMoreSplits();
        reader.pollNext(collector);
        verify(context, never()).signalNoMoreElement();
        reader.pollNext(collector);
        assertEquals(Arrays.asList("0", "5"), emitted);
        verify(context, times(1)).signalNoMoreElement();
    }

    @Test
    void emptyReaderFinishesWithoutReadingOrRequestingSplits() throws Exception {
        ChromaDBSourceReader reader = reader(300, 300);
        drain(reader);
        verifyNoInteractions(client);
        verify(context, times(1)).signalNoMoreElement();
        verify(context, never()).sendSplitRequest();
    }

    @Test
    void nonpositiveBatchParametersFailOnOpen() throws Exception {
        ChromaDBSourceReader zeroBatch = reader(300, 0);
        ChromaDBSourceReader negativePage = reader(-1, 300);
        assertThrows(ChromaDBConnectorException.class, zeroBatch::open);
        assertThrows(ChromaDBConnectorException.class, negativePage::open);
    }

    @Test
    void closeReleasesClient() throws Exception {
        reader(300, 300).close();
        verify(client).close();
    }
}
