package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultiTableSinkWriterCheckpointTest {

    @Test
    void prepareCommitWaitsForDequeuedRow() throws Exception {
        assertCheckpointWaitsForDequeuedRow(true, false);
    }

    @Test
    void snapshotStateWaitsForDequeuedRow() throws Exception {
        assertCheckpointWaitsForDequeuedRow(false, false);
    }

    @Test
    void prepareCommitPropagatesDequeuedWriteFailure() throws Exception {
        assertCheckpointWaitsForDequeuedRow(true, true);
    }

    @Test
    void interruptedEnqueueRollsBackPendingRowAndPreservesInterrupt() throws Exception {
        assertFailedEnqueueRollsBackPendingRow(true);
    }

    @Test
    void failedEnqueueRollsBackPendingRow() throws Exception {
        assertFailedEnqueueRollsBackPendingRow(false);
    }

    private void assertFailedEnqueueRollsBackPendingRow(boolean interrupt) throws Exception {
        MultiTableSinkWriter writer =
                new MultiTableSinkWriter(
                        Collections.singletonMap(
                                SinkIdentifier.of("review", 0),
                                new MultiTableSinkWriterTest.TestSinkWriter()),
                        1,
                        Collections.emptyMap());
        AtomicReference<MultiTableWriterRunnable> worker = new AtomicReference<>();
        LinkedBlockingQueue<SeaTunnelRow> queue =
                new LinkedBlockingQueue<SeaTunnelRow>() {
                    @Override
                    public boolean offer(SeaTunnelRow row, long timeout, TimeUnit unit)
                            throws InterruptedException {
                        assertTrue(worker.get().hasPendingRowRequests());
                        if (interrupt) {
                            throw new InterruptedException("injected enqueue interruption");
                        }
                        throw new IllegalStateException("injected enqueue failure");
                    }
                };
        worker.set(replaceQueue(writer, queue));
        try {
            if (interrupt) {
                assertThrows(
                        IOException.class, () -> writer.write(new SeaTunnelRow(new Object[] {1L})));
                assertTrue(Thread.currentThread().isInterrupted());
            } else {
                assertThrows(
                        IllegalStateException.class,
                        () -> writer.write(new SeaTunnelRow(new Object[] {1L})));
            }
            assertFalse(worker.get().hasPendingRowRequests());
            assertTrue(queue.isEmpty());
            Thread.interrupted();
            writer.prepareCommit(42);
        } finally {
            Thread.interrupted();
            writer.close();
        }
    }

    private void assertCheckpointWaitsForDequeuedRow(boolean prepare, boolean failWrite)
            throws Exception {
        AtomicInteger written = new AtomicInteger();
        AtomicInteger checkpointed = new AtomicInteger();
        SinkWriter<SeaTunnelRow, ?, ?> sink =
                new MultiTableSinkWriterTest.TestSinkWriter() {
                    @Override
                    public void write(SeaTunnelRow row) {
                        if (failWrite) {
                            throw new IllegalStateException("injected write failure");
                        }
                        written.incrementAndGet();
                    }

                    @Override
                    public Optional<MultiTableSinkWriterTest.TestSinkState> prepareCommit()
                            throws IOException {
                        checkpointed.set(written.get());
                        return super.prepareCommit();
                    }

                    @Override
                    public List<Object> snapshotState(long checkpointId) throws IOException {
                        checkpointed.set(written.get());
                        return super.snapshotState(checkpointId);
                    }
                };
        MultiTableSinkWriter writer =
                new MultiTableSinkWriter(
                        Collections.singletonMap(SinkIdentifier.of("review", 0), sink),
                        1,
                        Collections.emptyMap());
        PausedQueue queue = new PausedQueue();
        MultiTableWriterRunnable worker = replaceQueue(writer, queue);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            writer.write(new SeaTunnelRow(new Object[] {1L}));
            assertTrue(queue.polled.await(5, TimeUnit.SECONDS));
            assertTrue(queue.isEmpty());
            assertTrue(worker.hasPendingRowRequests());
            CountDownLatch checkpointStarted = new CountDownLatch(1);
            Future<?> checkpoint =
                    executor.submit(
                            () -> {
                                checkpointStarted.countDown();
                                if (prepare) {
                                    return writer.prepareCommit(42);
                                }
                                return writer.snapshotState(42);
                            });
            assertTrue(checkpointStarted.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> checkpoint.get(100, TimeUnit.MILLISECONDS));
            queue.release.countDown();
            if (failWrite) {
                ExecutionException error =
                        assertThrows(
                                ExecutionException.class,
                                () -> checkpoint.get(5, TimeUnit.SECONDS));
                assertTrue(error.getCause().getMessage().contains("sink throw error"));
                assertEquals(0, checkpointed.get());
            } else {
                checkpoint.get(5, TimeUnit.SECONDS);
                assertEquals(1, checkpointed.get());
                assertFalse(worker.hasPendingRowRequests());
            }
        } finally {
            queue.release.countDown();
            executor.shutdownNow();
            if (failWrite) {
                assertThrows(RuntimeException.class, writer::close);
            } else {
                writer.close();
            }
        }
    }

    private MultiTableWriterRunnable replaceQueue(
            MultiTableSinkWriter writer, BlockingQueue<SeaTunnelRow> queue) throws Exception {
        Field queuesField = MultiTableSinkWriter.class.getDeclaredField("blockingQueues");
        queuesField.setAccessible(true);
        List<BlockingQueue<SeaTunnelRow>> queues =
                (List<BlockingQueue<SeaTunnelRow>>) queuesField.get(writer);
        queues.set(0, queue);
        Field workersField = MultiTableSinkWriter.class.getDeclaredField("runnable");
        workersField.setAccessible(true);
        MultiTableWriterRunnable worker =
                (MultiTableWriterRunnable) ((List<?>) workersField.get(writer)).get(0);
        Field queueField = MultiTableWriterRunnable.class.getDeclaredField("queue");
        queueField.setAccessible(true);
        queueField.set(worker, queue);
        return worker;
    }

    private static class PausedQueue extends LinkedBlockingQueue<SeaTunnelRow> {
        private final CountDownLatch polled = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public SeaTunnelRow poll(long timeout, TimeUnit unit) throws InterruptedException {
            SeaTunnelRow row = super.poll(timeout, unit);
            if (row != null) {
                polled.countDown();
                release.await();
            }
            return row;
        }
    }
}
