package org.apache.seatunnel.connectors.seatunnel.chromadb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.chromadb.exception.ChromaDBConnectorException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChromaDBDirectBatchReadTest {
    private static final Gson GSON = new Gson();
    private static final int BATCH_SIZE = 300;
    private final ConcurrentLinkedQueue<JsonObject> requests = new ConcurrentLinkedQueue<>();
    private final List<SeaTunnelRow> rows = new ArrayList<>();
    private final Object checkpointLock = new Object();
    private int recordCount;
    private int nullResponseOffset = -1;
    private MockWebServer server;
    private SourceReader.Context context;
    private Collector<SeaTunnelRow> output;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() throws Exception {
        context = mock(SourceReader.Context.class);
        when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        output = mock(Collector.class);
        when(output.getCheckpointLock()).thenReturn(checkpointLock);
        doAnswer(
                        call -> {
                            assertTrue(Thread.holdsLock(checkpointLock));
                            rows.add(call.getArgument(0));
                            return null;
                        })
                .when(output)
                .collect(any(SeaTunnelRow.class));
        server = new MockWebServer();
        server.setDispatcher(
                new Dispatcher() {
                    @Override
                    public MockResponse dispatch(RecordedRequest request) {
                        if (request.getPath().endsWith("/collections/source")) {
                            return new MockResponse()
                                    .setBody(
                                            "{\"id\":\"col-id\",\"name\":\"source\",\"dimension\":3}");
                        }
                        if (!request.getPath().endsWith("/get")) {
                            return new MockResponse().setResponseCode(404);
                        }
                        JsonObject body =
                                GSON.fromJson(request.getBody().readUtf8(), JsonObject.class);
                        requests.add(body);
                        if (body.has("offset")
                                && body.get("offset").getAsInt() == nullResponseOffset) {
                            return new MockResponse().setBody("null");
                        }
                        List<String> ids = new ArrayList<>();
                        if (body.has("ids")) {
                            body.getAsJsonArray("ids").forEach(id -> ids.add(id.getAsString()));
                        } else {
                            int offset = body.get("offset").getAsInt();
                            int limit = body.get("limit").getAsInt();
                            for (int i = offset; i < Math.min(recordCount, offset + limit); i++) {
                                ids.add(Integer.toString(i));
                            }
                        }
                        Map<String, Object> result = new HashMap<>();
                        result.put("ids", ids);
                        if (body.getAsJsonArray("include").size() > 0) {
                            result.put(
                                    "embeddings",
                                    ids.stream()
                                            .map(id -> new float[] {Float.parseFloat(id), 1, -1})
                                            .collect(Collectors.toList()));
                            result.put(
                                    "documents",
                                    ids.stream()
                                            .map(id -> "doc-" + id)
                                            .collect(Collectors.toList()));
                            result.put(
                                    "uris",
                                    ids.stream()
                                            .map(id -> "uri-" + id)
                                            .collect(Collectors.toList()));
                            result.put(
                                    "metadatas",
                                    ids.stream()
                                            .map(
                                                    id ->
                                                            Collections.singletonMap(
                                                                    "index", Integer.parseInt(id)))
                                            .collect(Collectors.toList()));
                        }
                        return new MockResponse().setBody(GSON.toJson(result));
                    }
                });
        server.start();
    }

    @AfterEach
    void teardown() throws Exception {
        server.close();
    }

    private ChromaDBSource source(int idSplitSize, int batchSize) {
        Map<String, Object> config = new HashMap<>();
        config.put("url", server.url("/").toString());
        config.put("collections", Collections.singletonList("source"));
        config.put("id_split_size", idSplitSize);
        config.put("batch_size", batchSize);
        return new ChromaDBSource(ReadonlyConfig.fromMap(config));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 600, 601})
    void equalSizesFetchFullPagesWithoutIdRequests(int count) throws Exception {
        recordCount = count;
        ChromaDBSource source = source(BATCH_SIZE, BATCH_SIZE);
        try (SourceReader<SeaTunnelRow, ChromaDBSourceSplit> reader =
                source.createReader(context)) {
            reader.open();
            reader.addSplits(
                    Collections.singletonList(
                            new ChromaDBSourceSplit(
                                    "col-id",
                                    source.getProducedCatalogTables().get(0).getTablePath())));
            reader.handleNoMoreSplits();
            assertEquals(1, reader.snapshotState(1).size());
            reader.pollNext(output);
            assertTrue(reader.snapshotState(2).isEmpty());
        }
        verify(context).signalNoMoreElement();
        assertRows(source);
        // A full final page needs one empty request to detect the end, as on master.
        assertEquals(count / BATCH_SIZE + 1, requests.size());
        int offset = 0;
        for (JsonObject request : requests) {
            assertFalse(request.has("ids"));
            assertEquals(offset, request.get("offset").getAsInt());
            assertEquals(BATCH_SIZE, request.get("limit").getAsInt());
            assertEquals(
                    GSON.toJsonTree(Arrays.asList("embeddings", "documents", "uris", "metadatas")),
                    request.get("include"));
            offset += BATCH_SIZE;
        }
    }

    @ParameterizedTest
    @CsvSource({"600,300", "300,600"})
    void unequalSizesKeepIdScanAndExplicitIdFetch(int idSplitSize, int batchSize) throws Exception {
        recordCount = 601;
        ChromaDBSource source = source(idSplitSize, batchSize);
        try (SourceReader<SeaTunnelRow, ChromaDBSourceSplit> reader =
                source.createReader(context)) {
            reader.open();
            reader.addSplits(
                    Collections.singletonList(
                            new ChromaDBSourceSplit(
                                    "col-id",
                                    source.getProducedCatalogTables().get(0).getTablePath())));
            reader.handleNoMoreSplits();
            reader.pollNext(output);
        }
        assertRows(source);
        int scans = 0;
        int fetches = 0;
        for (JsonObject request : requests) {
            if (request.has("offset")) {
                assertEquals(scans++ * idSplitSize, request.get("offset").getAsInt());
                assertEquals(idSplitSize, request.get("limit").getAsInt());
                assertEquals(0, request.getAsJsonArray("include").size());
            } else {
                assertTrue(request.has("ids"));
                assertTrue(request.getAsJsonArray("ids").size() <= batchSize);
                assertEquals(4, request.getAsJsonArray("include").size());
                fetches++;
            }
        }
        assertEquals(recordCount / idSplitSize + 1, scans);
        assertEquals(3, fetches);
        verify(context).signalNoMoreElement();
    }

    @Test
    void nullDirectPageFailsWithoutSignallingCompletion() throws Exception {
        recordCount = 601;
        nullResponseOffset = BATCH_SIZE;
        ChromaDBSource source = source(BATCH_SIZE, BATCH_SIZE);
        try (SourceReader<SeaTunnelRow, ChromaDBSourceSplit> reader =
                source.createReader(context)) {
            reader.open();
            reader.addSplits(
                    Collections.singletonList(
                            new ChromaDBSourceSplit(
                                    "col-id",
                                    source.getProducedCatalogTables().get(0).getTablePath())));
            reader.handleNoMoreSplits();
            assertThrows(ChromaDBConnectorException.class, () -> reader.pollNext(output));
        }
        assertEquals(BATCH_SIZE, rows.size());
        assertEquals(2, requests.size());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void restoredCollectionSplitUsesDirectPages() throws Exception {
        recordCount = 601;
        ChromaDBSource source = source(BATCH_SIZE, BATCH_SIZE);
        byte[] checkpoint;
        try (SourceReader<SeaTunnelRow, ChromaDBSourceSplit> original =
                source.createReader(context)) {
            original.addSplits(
                    Collections.singletonList(
                            new ChromaDBSourceSplit(
                                    "col-id",
                                    source.getProducedCatalogTables().get(0).getTablePath())));
            checkpoint = source.getSplitSerializer().serialize(original.snapshotState(1).get(0));
        }
        try (SourceReader<SeaTunnelRow, ChromaDBSourceSplit> recovered =
                source.createReader(context)) {
            recovered.open();
            recovered.addSplits(
                    Collections.singletonList(source.getSplitSerializer().deserialize(checkpoint)));
            recovered.handleNoMoreSplits();
            recovered.pollNext(output);
        }
        assertRows(source);
        assertEquals(3, requests.size());
        assertTrue(requests.stream().noneMatch(request -> request.has("ids")));
    }

    private void assertRows(ChromaDBSource source) {
        assertEquals(recordCount, rows.size());
        assertEquals(
                IntStream.range(0, recordCount)
                        .mapToObj(Integer::toString)
                        .collect(Collectors.toSet()),
                rows.stream().map(row -> (String) row.getField(0)).collect(Collectors.toSet()));
        String tableId = source.getProducedCatalogTables().get(0).getTablePath().toString();
        for (SeaTunnelRow row : rows) {
            String id = (String) row.getField(0);
            assertEquals(tableId, row.getTableId());
            ByteBuffer vector = ((ByteBuffer) row.getField(1)).duplicate();
            vector.rewind();
            assertEquals(Float.parseFloat(id), vector.getFloat());
            assertEquals(1f, vector.getFloat());
            assertEquals(-1f, vector.getFloat());
            assertEquals("doc-" + id, row.getField(2));
            assertEquals("uri-" + id, row.getField(3));
            assertEquals(
                    Integer.parseInt(id),
                    GSON.fromJson((String) row.getField(4), JsonObject.class)
                            .get("index")
                            .getAsInt());
        }
    }
}
