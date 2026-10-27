package org.apache.seatunnel.translation.spark.source.partition.batch;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.translation.source.BaseSourceFunction;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.serialization.InternalRowCollector;
import org.apache.seatunnel.translation.spark.serialization.InternalRowConverter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Timeout(15)
class CoordinatedBatchPartitionReaderTest {
    private final CountDownLatch sourceStopped = new CountDownLatch(1);
    private final CountDownLatch releasePeer = new CountDownLatch(1);
    private CoordinatedBatchPartitionReader adapter;

    @AfterEach
    void close() throws Exception {
        releasePeer.countDown();
        if (adapter != null) {
            adapter.close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4})
    void readsAllRowsBeforeEndOfInput(int parallelism) throws Exception {
        adapter =
                createAdapter(
                        parallelism,
                        (context, collector) -> {
                            int firstId = context.getIndexOfSubtask() * 2;
                            collector.collect(new SeaTunnelRow(new Object[] {firstId}));
                            collector.collect(new SeaTunnelRow(new Object[] {firstId + 1}));
                            context.signalNoMoreElement();
                        });
        Set<Integer> ids = new HashSet<>();
        int count = 0;
        while (adapter.next()) {
            ids.add(adapter.get().getInt(2));
            count++;
        }
        assertEquals(parallelism * 2, count);
        assertEquals(IntStream.range(0, count).boxed().collect(Collectors.toSet()), ids);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void propagatesWorkerFailureWithoutHanging(boolean fatal) throws Exception {
        Throwable failure =
                fatal ? new AssertionError("reader failed") : new IOException("reader failed");
        adapter =
                createAdapter(
                        1,
                        (context, collector) -> {
                            throw failure;
                        });
        assertTimeoutPreemptively(
                Duration.ofSeconds(5),
                () -> {
                    assertSame(failure, assertThrows(failure.getClass(), adapter::next));
                });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void peerCompletionCannotHideFailure(boolean bufferRows) throws Exception {
        IOException failure = new IOException("reader 0 failed");
        CountDownLatch peerStarted = new CountDownLatch(1);
        CountDownLatch peerCompleted = new CountDownLatch(1);
        adapter =
                createAdapter(
                        2,
                        (context, collector) -> {
                            if (context.getIndexOfSubtask() == 0) {
                                assertTrue(peerStarted.await(5, TimeUnit.SECONDS));
                                throw failure;
                            }
                            if (bufferRows) {
                                collector.collect(new SeaTunnelRow(new Object[] {2}));
                                collector.collect(new SeaTunnelRow(new Object[] {3}));
                            }
                            peerStarted.countDown();
                            assertTrue(releasePeer.await(5, TimeUnit.SECONDS));
                            context.signalNoMoreElement();
                            peerCompleted.countDown();
                        });
        adapter.prepare();
        // Let the failing worker stop the source before its peer signals completion.
        assertTrue(sourceStopped.await(5, TimeUnit.SECONDS));
        releasePeer.countDown();
        assertTrue(peerCompleted.await(5, TimeUnit.SECONDS));

        assertSame(failure, assertThrows(IOException.class, adapter::next));
        assertSame(failure, assertThrows(RuntimeException.class, adapter::get).getCause());
        assertSame(failure, assertThrows(IOException.class, adapter::next));
    }

    @SuppressWarnings("unchecked")
    private CoordinatedBatchPartitionReader createAdapter(int parallelism, ReaderAction action)
            throws Exception {
        SeaTunnelSource<SeaTunnelRow, SourceSplit, Serializable> testSource =
                mock(SeaTunnelSource.class);
        when(testSource.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        when(testSource.createEnumerator(any())).thenReturn(mock(SourceSplitEnumerator.class));
        when(testSource.createReader(any()))
                .thenAnswer(
                        invocation -> {
                            SourceReader.Context context = invocation.getArgument(0);
                            SourceReader<SeaTunnelRow, SourceSplit> reader =
                                    mock(SourceReader.class);
                            doAnswer(
                                            call -> {
                                                action.poll(context, call.getArgument(0));
                                                return null;
                                            })
                                    .when(reader)
                                    .pollNext(any());
                            return reader;
                        });
        MultiTableManager manager = mock(MultiTableManager.class);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});
        when(manager.getInternalRowCollector(any(), any(), any()))
                .thenAnswer(
                        invocation ->
                                new InternalRowCollector(
                                        invocation.getArgument(0),
                                        invocation.getArgument(1),
                                        new InternalRowConverter(rowType),
                                        emptyMap()));
        return new CoordinatedBatchPartitionReader(
                testSource, parallelism, "spark-reader-test", 0, emptyMap(), manager) {
            @Override
            protected BaseSourceFunction<SeaTunnelRow> createInternalSource() {
                return new InternalCoordinatedSource<SourceSplit, Serializable>(
                        testSource, null, parallelism, jobId) {
                    @Override
                    public void run(Collector<SeaTunnelRow> collector) throws Exception {
                        try {
                            super.run(collector);
                        } finally {
                            sourceStopped.countDown();
                        }
                    }
                };
            }
        };
    }

    @FunctionalInterface
    private interface ReaderAction {
        void poll(SourceReader.Context context, Collector<SeaTunnelRow> collector) throws Throwable;
    }
}
