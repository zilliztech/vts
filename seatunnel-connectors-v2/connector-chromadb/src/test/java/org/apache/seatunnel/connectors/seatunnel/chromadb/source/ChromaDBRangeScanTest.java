package org.apache.seatunnel.connectors.seatunnel.chromadb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ChromaDBRangeScanTest {
    private static final Gson GSON = new Gson();
    private static final int RECORDS = 2407;
    private static final int BATCH_SIZE = 128;

    @Test
    void testConcurrentRangesWithSmallPages() throws Exception {
        verifyScan(4, 300);
    }

    @Test
    void testSingleReaderWithOneIdPage() throws Exception {
        verifyScan(1, 10000000);
    }

    @SuppressWarnings("unchecked")
    private void verifyScan(int parallelism, int idPageSize) throws Exception {
        AtomicInteger counts = new AtomicInteger();
        AtomicInteger fetches = new AtomicInteger();
        AtomicInteger assignments = new AtomicInteger();
        CountDownLatch concurrentFetches = new CountDownLatch(parallelism);
        ConcurrentLinkedQueue<JsonObject> scans = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<SeaTunnelRow> rows = new ConcurrentLinkedQueue<>();
        List<ChromaDBSourceSplit> assigned = new ArrayList<>();
        List<SourceReader<SeaTunnelRow, ChromaDBSourceSplit>> readers = new ArrayList<>();
        List<Collector<SeaTunnelRow>> outputs = new ArrayList<>();
        List<AtomicBoolean> finished = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(parallelism);
        SourceSplitEnumerator<ChromaDBSourceSplit, ChromaDBSourceState> enumerator = null;
        try (MockWebServer server = new MockWebServer()) {
            server.setDispatcher(
                    new Dispatcher() {
                        @Override
                        public MockResponse dispatch(RecordedRequest request)
                                throws InterruptedException {
                            String path = request.getPath();
                            if (path.endsWith("/collections/source")) {
                                return new MockResponse()
                                        .setBody(
                                                "{\"id\":\"col-id\",\"name\":\"source\",\"dimension\":3}");
                            }
                            if (path.endsWith("/col-id/count")) {
                                counts.incrementAndGet();
                                return new MockResponse().setBody(Integer.toString(RECORDS));
                            }
                            if (!path.endsWith("/col-id/get")) {
                                return new MockResponse().setResponseCode(404);
                            }
                            JsonObject body =
                                    GSON.fromJson(request.getBody().readUtf8(), JsonObject.class);
                            List<String> ids = new ArrayList<>();
                            Map<String, Object> response = new HashMap<>();
                            if (body.has("offset")) {
                                scans.add(body);
                                long offset = body.get("offset").getAsLong();
                                int limit = body.get("limit").getAsInt();
                                if (limit > idPageSize
                                        || offset + limit > RECORDS
                                        || body.getAsJsonArray("include").size() != 0) {
                                    violations.add("Invalid ID scan: " + body);
                                }
                                ids.addAll(
                                        LongStream.range(offset, offset + limit)
                                                .mapToObj(Long::toString)
                                                .collect(Collectors.toList()));
                            } else {
                                body.getAsJsonArray("ids").forEach(id -> ids.add(id.getAsString()));
                                if (ids.size() > BATCH_SIZE) {
                                    violations.add("Oversized data fetch: " + body);
                                }
                                if (fetches.incrementAndGet() <= parallelism) {
                                    concurrentFetches.countDown();
                                    if (!concurrentFetches.await(5, TimeUnit.SECONDS)) {
                                        violations.add("Readers did not fetch concurrently");
                                    }
                                }
                                response.put(
                                        "embeddings",
                                        ids.stream()
                                                .map(
                                                        id ->
                                                                new float[] {
                                                                    Float.parseFloat(id),
                                                                    1.0f,
                                                                    -1.0f
                                                                })
                                                .collect(Collectors.toList()));
                                response.put(
                                        "documents",
                                        ids.stream()
                                                .map(id -> "doc-" + id)
                                                .collect(Collectors.toList()));
                                response.put(
                                        "uris",
                                        ids.stream()
                                                .map(id -> "uri-" + id)
                                                .collect(Collectors.toList()));
                                response.put(
                                        "metadatas",
                                        ids.stream()
                                                .map(
                                                        id ->
                                                                Collections.singletonMap(
                                                                        "index",
                                                                        Integer.parseInt(id)))
                                                .collect(Collectors.toList()));
                            }
                            response.put("ids", ids);
                            return new MockResponse().setBody(GSON.toJson(response));
                        }
                    });
            server.start();
            Map<String, Object> options = new HashMap<>();
            options.put("url", server.url("/").toString());
            options.put("collections", Collections.singletonList("source"));
            options.put("id_split_size", idPageSize);
            options.put("batch_size", BATCH_SIZE);
            options.put("read_timeout", 10);
            ChromaDBSource source = new ChromaDBSource(ReadonlyConfig.fromMap(options));
            for (int i = 0; i < parallelism; i++) {
                SourceReader.Context readerContext = mock(SourceReader.Context.class);
                when(readerContext.getBoundedness()).thenReturn(Boundedness.BOUNDED);
                when(readerContext.getIndexOfSubtask()).thenReturn(i);
                AtomicBoolean done = new AtomicBoolean();
                finished.add(done);
                doAnswer(
                                call -> {
                                    assertTrue(
                                            done.compareAndSet(false, true),
                                            "Completion must only be signalled once");
                                    return null;
                                })
                        .when(readerContext)
                        .signalNoMoreElement();
                SourceReader<SeaTunnelRow, ChromaDBSourceSplit> reader =
                        source.createReader(readerContext);
                readers.add(reader);
                reader.open();
                Object checkpointLock = new Object();
                Collector<SeaTunnelRow> output = mock(Collector.class);
                when(output.getCheckpointLock()).thenReturn(checkpointLock);
                doAnswer(
                                call -> {
                                    assertTrue(Thread.holdsLock(checkpointLock));
                                    rows.add(call.getArgument(0));
                                    return null;
                                })
                        .when(output)
                        .collect(any(SeaTunnelRow.class));
                outputs.add(output);
            }
            SourceSplitEnumerator.Context<ChromaDBSourceSplit> context =
                    mock(SourceSplitEnumerator.Context.class);
            when(context.currentParallelism()).thenReturn(parallelism);
            Set<Integer> registered =
                    IntStream.range(0, parallelism).boxed().collect(Collectors.toSet());
            when(context.registeredReaders()).thenReturn(registered);
            doAnswer(
                            call -> {
                                int reader = call.getArgument(0);
                                List<ChromaDBSourceSplit> splits = call.getArgument(1);
                                assignments.incrementAndGet();
                                assigned.addAll(splits);
                                readers.get(reader).addSplits(splits);
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
            enumerator = source.createEnumerator(context);
            enumerator.open();
            SourceSplitEnumerator<ChromaDBSourceSplit, ChromaDBSourceState> runningEnumerator =
                    enumerator;
            assertTimeoutPreemptively(Duration.ofSeconds(3), runningEnumerator::run);
            assertEquals(0, fetches.get(), "Enumeration must finish before readers start");
            assertTrue(enumerator.snapshotState(1).getPendingSplits().isEmpty());
            List<Future<?>> tasks = new ArrayList<>();
            for (int i = 0; i < parallelism; i++) {
                final int reader = i;
                tasks.add(
                        pool.submit(
                                () -> {
                                    for (int attempt = 0;
                                            attempt < RECORDS && !finished.get(reader).get();
                                            attempt++) {
                                        readers.get(reader).pollNext(outputs.get(reader));
                                    }
                                    assertTrue(
                                            finished.get(reader).get(),
                                            "Reader did not finish its range");
                                    return null;
                                }));
            }
            for (Future<?> task : tasks) {
                task.get(15, TimeUnit.SECONDS);
            }
            assertEquals(1, counts.get());
            assertEquals(
                    parallelism, assignments.get(), "Pages must not create new split assignments");
            assertEquals(parallelism, assigned.size());
            assertEquals(RECORDS, rows.size());
            Set<String> ids = new HashSet<>();
            String tableId = source.getProducedCatalogTables().get(0).getTablePath().toString();
            for (SeaTunnelRow row : rows) {
                String id = (String) row.getField(0);
                assertTrue(ids.add(id), "Duplicate ID: " + id);
                ByteBuffer vector = (ByteBuffer) row.getField(1);
                assertEquals(Float.parseFloat(id), vector.getFloat());
                assertEquals(1.0f, vector.getFloat());
                assertEquals(-1.0f, vector.getFloat());
                assertEquals("doc-" + id, row.getField(2));
                assertEquals("uri-" + id, row.getField(3));
                assertEquals(
                        Long.parseLong(id),
                        GSON.fromJson((String) row.getField(4), JsonObject.class)
                                .get("index")
                                .getAsLong());
                assertEquals(tableId, row.getTableId());
            }
            assertEquals(
                    IntStream.range(0, RECORDS)
                            .mapToObj(Integer::toString)
                            .collect(Collectors.toSet()),
                    ids);
            assertFalse(scans.isEmpty());
            long expectedScans = 0;
            for (ChromaDBSourceSplit split : assigned) {
                expectedScans +=
                        (split.getEndOffset() - split.getStartOffset() + idPageSize - 1)
                                / idPageSize;
            }
            assertEquals(expectedScans, scans.size());
            for (JsonObject scan : scans) {
                long offset = scan.get("offset").getAsLong();
                int limit = scan.get("limit").getAsInt();
                assertTrue(
                        assigned.stream()
                                .anyMatch(
                                        split ->
                                                offset >= split.getStartOffset()
                                                        && offset < split.getEndOffset()
                                                        && limit
                                                                == Math.min(
                                                                        idPageSize,
                                                                        split.getEndOffset()
                                                                                - offset)),
                        "Scan escaped its assigned range: " + scan);
            }
            assertTrue(violations.isEmpty(), violations.toString());
        } finally {
            pool.shutdownNow();
            for (SourceReader<SeaTunnelRow, ChromaDBSourceSplit> reader : readers) {
                reader.close();
            }
            if (enumerator != null) {
                enumerator.close();
            }
        }
    }
}
