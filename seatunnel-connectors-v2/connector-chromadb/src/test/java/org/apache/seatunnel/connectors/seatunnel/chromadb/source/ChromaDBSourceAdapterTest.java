package org.apache.seatunnel.connectors.seatunnel.chromadb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.source.BaseSourceFunction;
import org.apache.seatunnel.translation.source.CoordinatedSource;
import org.apache.seatunnel.translation.source.ParallelSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ChromaDBSourceAdapterTest {
    private static final Gson GSON = new Gson();
    private static final int RECORDS = 8;
    private final AtomicInteger counts = new AtomicInteger();
    private MockWebServer server;
    private ChromaDBSource source;

    @BeforeEach
    void setup() throws Exception {
        server = new MockWebServer();
        server.setDispatcher(
                new Dispatcher() {
                    @Override
                    public MockResponse dispatch(RecordedRequest request) {
                        if (request.getPath().endsWith("/collections/source")) {
                            return new MockResponse()
                                    .setBody(
                                            "{\"id\":\"col-id\",\"name\":\"source\",\"dimension\":1}");
                        }
                        if (request.getPath().endsWith("/count")) {
                            counts.incrementAndGet();
                            return new MockResponse().setBody(Integer.toString(RECORDS));
                        }
                        JsonObject body =
                                GSON.fromJson(request.getBody().readUtf8(), JsonObject.class);
                        List<String> ids = new ArrayList<>();
                        if (body.has("ids")) {
                            body.getAsJsonArray("ids").forEach(id -> ids.add(id.getAsString()));
                        } else {
                            int start = body.get("offset").getAsInt();
                            int end = Math.min(RECORDS, start + body.get("limit").getAsInt());
                            for (int i = start; i < end; i++) {
                                ids.add(Integer.toString(i));
                            }
                        }
                        Map<String, Object> result = new HashMap<>();
                        result.put("ids", ids);
                        result.put(
                                "embeddings",
                                ids.stream()
                                        .map(id -> new float[] {Float.parseFloat(id)})
                                        .collect(Collectors.toList()));
                        return new MockResponse().setBody(GSON.toJson(result));
                    }
                });
        server.start();
        Map<String, Object> config = new HashMap<>();
        config.put("url", server.url("/").toString());
        config.put("collections", Collections.singletonList("source"));
        config.put("id_split_size", 3);
        config.put("batch_size", 2);
        source = new ChromaDBSource(ReadonlyConfig.fromMap(config));
    }

    @AfterEach
    void teardown() throws Exception {
        server.close();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4})
    void sparkAdapterSelectionReadsEveryRowAndFinishes(int parallelism) throws Exception {
        // Spark selects the translation adapter using this marker, rather than sharing
        // an enumerator between independent ParallelSource instances.
        BaseSourceFunction<SeaTunnelRow> adapter =
                source instanceof SupportCoordinate
                        ? new CoordinatedSource<>(source, null, parallelism, "chroma-adapter-test")
                        : new ParallelSource<>(source, null, parallelism, "chroma-adapter-test", 0);
        ConcurrentLinkedQueue<String> ids = new ConcurrentLinkedQueue<>();
        Map<Integer, List<byte[]>> state = runToCompletion(adapter, ids);
        assertComplete(ids);
        assertEquals(1, counts.get());
        assertTrue(
                source.getEnumeratorStateSerializer()
                        .deserialize(state.get(-1).get(0))
                        .getPendingSplits()
                        .isEmpty());
        for (int reader = 0; reader < parallelism; reader++) {
            assertTrue(state.get(reader).isEmpty());
        }
    }

    @Test
    void coordinatedAdapterRestoresOnlyUnconsumedReaderState() throws Exception {
        ChromaDBSourceSplit pending =
                new ChromaDBSourceSplit(
                                "col-id",
                                source.getProducedCatalogTables().get(0).getTablePath(),
                                4,
                                RECORDS)
                        .withPendingIds(Arrays.asList("4", "5", "6"))
                        .advance(2)
                        .copy();
        Map<Integer, List<byte[]>> checkpoint = new HashMap<>();
        checkpoint.put(
                -1,
                Collections.singletonList(
                        source.getEnumeratorStateSerializer()
                                .serialize(
                                        new ChromaDBSourceState(
                                                Collections.singleton("col-id"),
                                                Collections.emptyList()))));
        checkpoint.put(0, Collections.emptyList());
        checkpoint.put(
                1, Collections.singletonList(source.getSplitSerializer().serialize(pending)));
        ConcurrentLinkedQueue<String> ids = new ConcurrentLinkedQueue<>();
        Map<Integer, List<byte[]>> finished =
                runToCompletion(
                        new CoordinatedSource<>(source, checkpoint, 2, "chroma-restore-test"), ids);
        assertEquals(Arrays.asList("6", "7"), new ArrayList<>(ids));
        assertEquals(0, counts.get(), "A restore must not recount or repartition the collection");
        assertTrue(finished.get(0).isEmpty());
        assertTrue(finished.get(1).isEmpty());
    }

    private Map<Integer, List<byte[]>> runToCompletion(
            BaseSourceFunction<SeaTunnelRow> adapter, ConcurrentLinkedQueue<String> ids)
            throws Exception {
        Collector<SeaTunnelRow> output = collector(ids);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> task = null;
        try {
            adapter.open();
            task =
                    executor.submit(
                            () -> {
                                adapter.run(output);
                                return null;
                            });
            task.get(5, TimeUnit.SECONDS);
            return adapter.snapshotState(1);
        } finally {
            try {
                adapter.close();
            } finally {
                if (task != null) {
                    task.cancel(true);
                }
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("unchecked")
    void failedReaderResumesAfterPeerFinished(boolean finishedPeerRemainsRegistered)
            throws Exception {
        SourceSplitEnumerator.Context<ChromaDBSourceSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(2);
        Set<Integer> registered = new HashSet<>(Arrays.asList(0, 1));
        when(context.registeredReaders()).thenAnswer(call -> new HashSet<>(registered));
        List<SourceReader<SeaTunnelRow, ChromaDBSourceSplit>> readers = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            SourceReader.Context readerContext = mock(SourceReader.Context.class);
            when(readerContext.getBoundedness()).thenReturn(Boundedness.BOUNDED);
            SourceReader<SeaTunnelRow, ChromaDBSourceSplit> reader =
                    source.createReader(readerContext);
            reader.open();
            readers.add(reader);
        }
        doAnswer(
                        call -> {
                            readers.get((Integer) call.getArgument(0))
                                    .addSplits(call.getArgument(1));
                            return null;
                        })
                .when(context)
                .assignSplit(anyInt(), anyList());
        doAnswer(
                        call -> {
                            readers.get((Integer) call.getArgument(0)).handleNoMoreSplits();
                            return null;
                        })
                .when(context)
                .signalNoMoreSplits(anyInt());
        ConcurrentLinkedQueue<String> ids = new ConcurrentLinkedQueue<>();
        Collector<SeaTunnelRow> output = collector(ids);
        try (SourceSplitEnumerator<ChromaDBSourceSplit, ChromaDBSourceState> enumerator =
                source.createEnumerator(context)) {
            enumerator.open();
            enumerator.run();
            for (int i = 0; i < RECORDS; i++) {
                readers.get(0).pollNext(output);
            }
            readers.get(1).pollNext(output);
            List<ChromaDBSourceSplit> checkpoint = readers.get(1).snapshotState(1);
            assertEquals(6, ids.size());
            // Flink unregisters the failed subtask before returning its splits, then
            // registers the replacement reader. The completed peer may remain registered.
            registered.remove(1);
            if (!finishedPeerRemainsRegistered) {
                registered.remove(0);
            }
            enumerator.addSplitsBack(checkpoint, 1);
            enumerator.handleSplitRequest(0);
            assertEquals(1, enumerator.currentUnassignedSplitSize());
            readers.get(1).close();
            SourceReader.Context recoveredContext = mock(SourceReader.Context.class);
            when(recoveredContext.getBoundedness()).thenReturn(Boundedness.BOUNDED);
            AtomicInteger completions = new AtomicInteger();
            doAnswer(
                            call -> {
                                completions.incrementAndGet();
                                return null;
                            })
                    .when(recoveredContext)
                    .signalNoMoreElement();
            SourceReader<SeaTunnelRow, ChromaDBSourceSplit> recovered =
                    source.createReader(recoveredContext);
            recovered.open();
            readers.set(1, recovered);
            registered.add(1);
            enumerator.registerReader(1);
            for (int i = 0; i < RECORDS; i++) {
                recovered.pollNext(output);
            }
            assertComplete(ids);
            assertEquals(1, completions.get());
            assertEquals(0, enumerator.currentUnassignedSplitSize());
            assertEquals(1, counts.get());
        } finally {
            for (SourceReader<SeaTunnelRow, ChromaDBSourceSplit> reader : readers) {
                reader.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Collector<SeaTunnelRow> collector(ConcurrentLinkedQueue<String> ids) {
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(new Object());
        when(collector.isEmptyThisPollNext()).thenReturn(true);
        doAnswer(
                        call -> {
                            ids.add((String) ((SeaTunnelRow) call.getArgument(0)).getField(0));
                            return null;
                        })
                .when(collector)
                .collect(any(SeaTunnelRow.class));
        return collector;
    }

    private void assertComplete(ConcurrentLinkedQueue<String> ids) {
        assertEquals(RECORDS, ids.size());
        assertEquals(
                IntStream.range(0, RECORDS).mapToObj(Integer::toString).collect(Collectors.toSet()),
                new HashSet<>(ids));
    }
}
