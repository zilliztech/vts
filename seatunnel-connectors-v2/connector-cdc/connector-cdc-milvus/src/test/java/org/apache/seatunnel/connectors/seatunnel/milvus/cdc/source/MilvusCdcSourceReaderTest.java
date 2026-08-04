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

package org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.client.MilvusCdcClientFactory;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.client.MilvusCdcMessageClient;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.offset.MilvusCdcOffset;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.parser.MilvusCdcRowMetadata;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.schema.MilvusCdcCollectionSchema;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.schema.MilvusCdcCollectionSchemaRegistry;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DumpMessagesRequest;
import io.milvus.grpc.DumpMessagesResponse;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.IDs;
import io.milvus.grpc.ImmutableMessage;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.MessageID;
import io.milvus.grpc.ScalarField;
import io.milvus.grpc.WALName;
import milvus.proto.msg.Msg;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class MilvusCdcSourceReaderTest {

    @Test
    void emitRowsAndCheckpointLastEmittedOffset() throws Exception {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-1")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(insertPayload(100L))
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "100"))
                        .build();
        FakeClientFactory clientFactory =
                new FakeClientFactory(Collections.singletonList(response));
        TestContext context = new TestContext();
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(context, readerConfig(), clientFactory, schemaRegistry());

        reader.open();
        Assertions.assertEquals(1, context.splitRequests.get());

        reader.addSplits(Collections.singletonList(testSplit()));
        TestCollector collector = new TestCollector();
        for (int i = 0; i < 20 && collector.rows.isEmpty(); i++) {
            reader.pollNext(collector);
            Thread.sleep(10L);
        }

        Assertions.assertEquals(1, collector.rows.size());
        Assertions.assertEquals(1001L, collector.rows.get(0).getField(0));
        List<MilvusCdcSplit> checkpoint = reader.snapshotState(1L);
        Assertions.assertEquals(
                "message-1", checkpoint.get(0).getCurrentOffset().getConsumedMessageId());
        Assertions.assertEquals(100L, checkpoint.get(0).getCurrentOffset().getTimetick());
        reader.close();
    }

    @Test
    void unknownMessageAdvancesOffsetWithoutRows() throws Exception {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-unknown")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(ByteString.EMPTY)
                                        .putProperties("_t", "12345")
                                        .putProperties("_lc", "safe-unknown")
                                        .putProperties("timetick", "100"))
                        .build();
        FakeClientFactory clientFactory =
                new FakeClientFactory(Collections.singletonList(response));
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(), readerConfig(), clientFactory, schemaRegistry());

        try {
            reader.addSplits(Collections.singletonList(testSplit()));
            TestCollector collector = new TestCollector();
            MilvusCdcOffset checkpointOffset = null;
            for (int i = 0; i < 30; i++) {
                reader.pollNext(collector);
                checkpointOffset = reader.snapshotState(1L).get(0).getCurrentOffset();
                if (checkpointOffset != null
                        && "message-unknown".equals(checkpointOffset.getConsumedMessageId())) {
                    break;
                }
                Thread.sleep(10L);
            }

            Assertions.assertTrue(collector.rows.isEmpty());
            Assertions.assertNotNull(checkpointOffset);
            Assertions.assertEquals("message-unknown", checkpointOffset.getConsumedMessageId());
            Assertions.assertEquals("safe-unknown", checkpointOffset.getResumeMessageId());
            Assertions.assertEquals(100L, checkpointOffset.getTimetick());
        } finally {
            reader.close();
        }
    }

    @Test
    void pollNextEmitsWholeWalMessageBeforeAdvancingOffset() throws Exception {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-batch")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(
                                                insertPayload(
                                                        "default",
                                                        "collection_a",
                                                        100L,
                                                        101L,
                                                        102L))
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "102"))
                        .build();
        FakeClientFactory clientFactory =
                new FakeClientFactory(Collections.singletonList(response));
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(), readerConfig(), clientFactory, schemaRegistry());

        reader.open();
        reader.addSplits(Collections.singletonList(testSplit()));
        TestCollector collector = new TestCollector();
        for (int i = 0; i < 20 && collector.rows.isEmpty(); i++) {
            reader.pollNext(collector);
            Thread.sleep(10L);
        }

        Assertions.assertEquals(3, collector.rows.size());
        Assertions.assertEquals(1001L, collector.rows.get(0).getField(0));
        Assertions.assertEquals(1002L, collector.rows.get(1).getField(0));
        Assertions.assertEquals(1003L, collector.rows.get(2).getField(0));
        Assertions.assertNull(collector.rows.get(0).getOptions().get("MilvusCdcMessageEnd"));
        Assertions.assertNull(collector.rows.get(1).getOptions().get("MilvusCdcMessageEnd"));
        Assertions.assertEquals(
                true, collector.rows.get(2).getOptions().get("MilvusCdcMessageEnd"));
        MilvusCdcSplit checkpoint = reader.snapshotState(1L).get(0);
        Assertions.assertEquals(
                "message-batch", checkpoint.getCurrentOffset().getConsumedMessageId());
        Assertions.assertEquals(102L, checkpoint.getCurrentOffset().getTimetick());
        reader.close();
    }

    @Test
    void txnRowsAreEmittedDeleteBeforeInsertAndCheckpointCommitOffset() throws Exception {
        List<DumpMessagesResponse> responses =
                asList(
                        response("txn-begin", ByteString.EMPTY, "BeginTxn", 100L, "safe-begin"),
                        response("txn-insert", insertPayload(111L), "Insert", 111L, "safe-insert"),
                        response("txn-delete", deletePayload(112L), "Delete", 112L, "safe-delete"),
                        response("txn-commit", ByteString.EMPTY, "CommitTxn", 120L, "safe-commit"));
        FakeClientFactory clientFactory = new FakeClientFactory(responses);
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(), readerConfig(), clientFactory, schemaRegistry());

        reader.open();
        reader.addSplits(Collections.singletonList(testSplit()));
        TestCollector collector = new TestCollector();
        for (int i = 0; i < 30 && collector.rows.isEmpty(); i++) {
            reader.pollNext(collector);
            Thread.sleep(10L);
        }

        Assertions.assertEquals(2, collector.rows.size());
        Assertions.assertEquals(RowKind.DELETE, collector.rows.get(0).getRowKind());
        Assertions.assertEquals(RowKind.INSERT, collector.rows.get(1).getRowKind());
        Assertions.assertEquals(1001L, collector.rows.get(0).getField(0));
        Assertions.assertEquals(1001L, collector.rows.get(1).getField(0));
        Assertions.assertEquals(
                "txn-delete",
                collector.rows.get(0).getOptions().get(MilvusCdcRowMetadata.CURRENT_MESSAGE_ID));
        Assertions.assertEquals(
                "safe-delete",
                collector.rows.get(0).getOptions().get(MilvusCdcRowMetadata.RESUME_MESSAGE_ID));
        Assertions.assertEquals(
                112L, collector.rows.get(0).getOptions().get(MilvusCdcRowMetadata.TIMETICK));
        Assertions.assertEquals(
                112L, collector.rows.get(0).getOptions().get(MilvusCdcRowMetadata.EVENT_TIMESTAMP));
        Assertions.assertEquals(
                "txn-insert",
                collector.rows.get(1).getOptions().get(MilvusCdcRowMetadata.CURRENT_MESSAGE_ID));
        Assertions.assertEquals(
                "safe-insert",
                collector.rows.get(1).getOptions().get(MilvusCdcRowMetadata.RESUME_MESSAGE_ID));
        Assertions.assertEquals(
                111L, collector.rows.get(1).getOptions().get(MilvusCdcRowMetadata.TIMETICK));
        Assertions.assertEquals(
                111L, collector.rows.get(1).getOptions().get(MilvusCdcRowMetadata.EVENT_TIMESTAMP));
        Assertions.assertEquals(
                true, collector.rows.get(0).getOptions().get(MilvusCdcRowMetadata.MESSAGE_END));
        Assertions.assertEquals(
                true, collector.rows.get(1).getOptions().get(MilvusCdcRowMetadata.MESSAGE_END));
        MilvusCdcSplit checkpoint = reader.snapshotState(1L).get(0);
        Assertions.assertEquals("txn-commit", checkpoint.getCurrentOffset().getConsumedMessageId());
        Assertions.assertEquals("safe-commit", checkpoint.getCurrentOffset().getResumeMessageId());
        Assertions.assertEquals(120L, checkpoint.getCurrentOffset().getTimetick());
        reader.close();
    }

    @Test
    void txnKeepsOriginalDmlMessageEndBoundariesAfterTypeOrdering() throws Exception {
        List<DumpMessagesResponse> responses =
                asList(
                        response("txn-begin", ByteString.EMPTY, "BeginTxn", 200L, "safe-begin"),
                        response(
                                "txn-insert",
                                insertPayload("default", "collection_a", 211L, 212L),
                                "Insert",
                                212L,
                                "safe-insert"),
                        response(
                                "txn-delete",
                                deletePayloads(221L, 222L),
                                "Delete",
                                222L,
                                "safe-delete"),
                        response("txn-commit", ByteString.EMPTY, "CommitTxn", 230L, "safe-commit"));
        FakeClientFactory clientFactory = new FakeClientFactory(responses);
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(), readerConfig(), clientFactory, schemaRegistry());

        reader.open();
        reader.addSplits(Collections.singletonList(testSplit()));
        TestCollector collector = new TestCollector();
        for (int i = 0; i < 30 && collector.rows.isEmpty(); i++) {
            reader.pollNext(collector);
            Thread.sleep(10L);
        }

        Assertions.assertEquals(4, collector.rows.size());
        assertTxnRow(collector.rows.get(0), RowKind.DELETE, 1001L, "txn-delete", 222L, false);
        assertTxnRow(collector.rows.get(1), RowKind.DELETE, 1002L, "txn-delete", 222L, true);
        assertTxnRow(collector.rows.get(2), RowKind.INSERT, 1001L, "txn-insert", 212L, false);
        assertTxnRow(collector.rows.get(3), RowKind.INSERT, 1002L, "txn-insert", 212L, true);
        MilvusCdcSplit checkpoint = reader.snapshotState(1L).get(0);
        Assertions.assertEquals("txn-commit", checkpoint.getCurrentOffset().getConsumedMessageId());
        Assertions.assertEquals("safe-commit", checkpoint.getCurrentOffset().getResumeMessageId());
        reader.close();
    }

    @Test
    void commitTxnWithoutBeginFailsFast() throws Exception {
        assertSingleResponseFailure(
                response("txn-commit", ByteString.EMPTY, "CommitTxn", 120L, "safe-commit"),
                "commit transaction message was received without begin");
    }

    @Test
    void transactionalDmlWithoutBeginFailsFast() throws Exception {
        assertSingleResponseFailure(
                transactionalResponse(
                        "txn-insert", insertPayload(111L), "Insert", 111L, "safe-insert"),
                "transaction DML message was received without begin");
    }

    @Test
    void unsupportedTransactionControlMessagesFailFast() throws Exception {
        assertSingleResponseFailure(
                response("txn-rollback", ByteString.EMPTY, "RollbackTxn", 120L, "safe-rollback"),
                "rollback transaction message is not supported");
        assertSingleResponseFailure(
                response("txn-aggregated", ByteString.EMPTY, "Txn", 120L, "safe-txn"),
                "aggregated transaction message is not supported");
    }

    @Test
    void unknownMessageInsideTxnFailsFast() throws Exception {
        FakeClientFactory clientFactory =
                new FakeClientFactory(
                        asList(
                                response(
                                        "txn-begin",
                                        ByteString.EMPTY,
                                        "BeginTxn",
                                        100L,
                                        "safe-begin"),
                                response(
                                        "txn-unknown",
                                        ByteString.EMPTY,
                                        "Unknown",
                                        110L,
                                        "safe-unknown"),
                                response(
                                        "txn-commit",
                                        ByteString.EMPTY,
                                        "CommitTxn",
                                        120L,
                                        "safe-commit")));
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(), readerConfig(), clientFactory, schemaRegistry());
        try {
            reader.addSplits(Collections.singletonList(testSplit()));
            assertReaderFailureContains(reader, "unknown message was received inside transaction");
        } finally {
            reader.close();
        }
    }

    @Test
    void txnStreamEndsBeforeCommitFailsFast() throws Exception {
        FakeClientFactory clientFactory =
                new FakeClientFactory(
                        asList(
                                response(
                                        "txn-begin",
                                        ByteString.EMPTY,
                                        "BeginTxn",
                                        100L,
                                        "safe-begin"),
                                response(
                                        "txn-insert",
                                        insertPayload(111L),
                                        "Insert",
                                        111L,
                                        "safe-insert")),
                        true);
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(), readerConfig(), clientFactory, schemaRegistry());
        try {
            reader.addSplits(Collections.singletonList(testSplit()));
            assertReaderFailureContains(reader, "transaction stream ended before commit");
        } finally {
            reader.close();
        }
    }

    @Test
    void unboundedDumpMessagesStreamEndingFailsFast() throws Exception {
        FakeClientFactory clientFactory = new FakeClientFactory(Collections.emptyList(), true);
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(), readerConfig(), clientFactory, schemaRegistry());
        try {
            reader.addSplits(Collections.singletonList(testSplit()));
            assertReaderFailureContains(reader, "unbounded DumpMessages stream ended unexpectedly");
        } finally {
            reader.close();
        }
    }

    @Test
    void splitFailureStopsOtherProducersAndFailsBeforeDrainingQueue() throws Exception {
        BlockingProducerClientFactory clientFactory =
                new BlockingProducerClientFactory(
                        response(
                                "healthy-message",
                                insertPayload(100L),
                                "Insert",
                                100L,
                                "healthy-resume"),
                        2);
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(), readerConfig(2), clientFactory, schemaRegistry());

        try {
            reader.addSplits(
                    asList(
                            startSplit("failing-pchannel", "failing-start", 0L),
                            startSplit("healthy-pchannel", "healthy-start", 0L)));

            Assertions.assertTrue(clientFactory.awaitFailureTriggered());
            Assertions.assertEquals(3, clientFactory.healthyResponseCount.get());
            Thread.sleep(100L);

            TestCollector collector = new TestCollector();
            IllegalStateException readerFailure =
                    Assertions.assertThrows(
                            IllegalStateException.class, () -> reader.pollNext(collector));
            Assertions.assertTrue(readerFailure.getMessage().contains("Milvus CDC reader failed"));
            Assertions.assertTrue(
                    readerFailure
                            .getCause()
                            .getMessage()
                            .contains("unbounded DumpMessages stream ended unexpectedly"));
            Assertions.assertTrue(collector.rows.isEmpty());

            reader.close();
            Assertions.assertTrue(clientFactory.awaitClientsClosed());
            int responseCountAfterClose = clientFactory.healthyResponseCount.get();
            Thread.sleep(100L);
            Assertions.assertEquals(
                    responseCountAfterClose, clientFactory.healthyResponseCount.get());
        } finally {
            reader.close();
        }
    }

    @Test
    void pollNextKeepsWalMessageAndOffsetWhenCollectFailsMidMessage() throws Exception {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-batch")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(
                                                insertPayload(
                                                        "default",
                                                        "collection_a",
                                                        100L,
                                                        101L,
                                                        102L))
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "102"))
                        .build();
        FakeClientFactory clientFactory =
                new FakeClientFactory(Collections.singletonList(response));
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(), readerConfig(), clientFactory, schemaRegistry());

        reader.open();
        reader.addSplits(Collections.singletonList(testSplit()));
        FailingCollector failingCollector = new FailingCollector(2);
        RuntimeException failure = null;
        for (int i = 0; i < 20; i++) {
            try {
                reader.pollNext(failingCollector);
            } catch (RuntimeException e) {
                failure = e;
                break;
            }
            Thread.sleep(10L);
        }

        Assertions.assertNotNull(failure);
        Assertions.assertNull(reader.snapshotState(1L).get(0).getCurrentOffset());

        TestCollector retryCollector = new TestCollector();
        reader.pollNext(retryCollector);

        Assertions.assertEquals(3, retryCollector.rows.size());
        Assertions.assertEquals(
                "message-batch",
                reader.snapshotState(2L).get(0).getCurrentOffset().getConsumedMessageId());
        reader.close();
    }

    @Test
    void filteredMessagesAdvanceCheckpointWithoutOutput() throws Exception {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-delete")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(deletePayload(200L))
                                        .putProperties("_t", "3")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "200"))
                        .build();
        FakeClientFactory clientFactory =
                new FakeClientFactory(Collections.singletonList(response));
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(),
                        readerConfig(Collections.singletonList("insert")),
                        clientFactory,
                        schemaRegistry());

        reader.open();
        reader.addSplits(Collections.singletonList(testSplit()));
        TestCollector collector = new TestCollector();
        MilvusCdcSplit checkpoint = null;
        for (int i = 0; i < 20; i++) {
            reader.pollNext(collector);
            checkpoint = reader.snapshotState(1L).get(0);
            if (checkpoint.getCurrentOffset() != null) {
                break;
            }
            Thread.sleep(10L);
        }

        Assertions.assertTrue(collector.rows.isEmpty());
        Assertions.assertEquals(
                "message-delete", checkpoint.getCurrentOffset().getConsumedMessageId());
        Assertions.assertEquals(200L, checkpoint.getCurrentOffset().getTimetick());
        reader.close();
    }

    @Test
    void unconfiguredSourceTablesAdvanceCheckpointWithoutOutput() throws Exception {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-other-db")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(insertPayload("other_db", "collection_a", 300L))
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "300"))
                        .build();
        FakeClientFactory clientFactory =
                new FakeClientFactory(Collections.singletonList(response));
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(), readerConfig(), clientFactory, schemaRegistry());

        reader.open();
        reader.addSplits(Collections.singletonList(testSplit()));
        TestCollector collector = new TestCollector();
        MilvusCdcSplit checkpoint = null;
        for (int i = 0; i < 20; i++) {
            reader.pollNext(collector);
            checkpoint = reader.snapshotState(1L).get(0);
            if (checkpoint.getCurrentOffset() != null) {
                break;
            }
            Thread.sleep(10L);
        }

        Assertions.assertTrue(collector.rows.isEmpty());
        Assertions.assertEquals(
                "message-other-db", checkpoint.getCurrentOffset().getConsumedMessageId());
        Assertions.assertEquals(300L, checkpoint.getCurrentOffset().getTimetick());
        reader.close();
    }

    @Test
    void updateCommitMetricsAfterCheckpointComplete() throws Exception {
        long messageTimestampMs = System.currentTimeMillis() - 60_000L;
        long messageTimestamp = hybridTimestamp(messageTimestampMs);
        long rowTimestamp = hybridTimestamp(messageTimestampMs - 60_000L);
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-metrics")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(insertPayload(rowTimestamp))
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", Long.toString(messageTimestamp)))
                        .build();
        AbstractMetricsContext metricsContext = new AbstractMetricsContext() {};
        FakeClientFactory clientFactory =
                new FakeClientFactory(Collections.singletonList(response));
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(metricsContext),
                        readerConfig(),
                        clientFactory,
                        schemaRegistry());

        reader.open();
        reader.addSplits(Collections.singletonList(testSplit()));
        TestCollector collector = new TestCollector();
        for (int i = 0; i < 20 && collector.rows.isEmpty(); i++) {
            reader.pollNext(collector);
            Thread.sleep(10L);
        }

        SeaTunnelRow row = collector.rows.get(0);
        Assertions.assertEquals(
                messageTimestamp, row.getOptions().get(MilvusCdcRowMetadata.EVENT_TIMESTAMP));
        Assertions.assertEquals(
                messageTimestampMs, row.getOptions().get(MilvusCdcRowMetadata.EVENT_TIMESTAMP_MS));
        Assertions.assertNotEquals(
                rowTimestamp, row.getOptions().get(MilvusCdcRowMetadata.EVENT_TIMESTAMP));
        Assertions.assertFalse(row.getOptions().containsKey("MilvusCdcSourceReceiveTimestampMs"));
        Assertions.assertTrue(
                ((Long) row.getOptions().get(CommonOptions.DELAY.getName())) >= 60_000L);
        Assertions.assertTrue(
                metricsContext.counter("MilvusCdcSourceReceiveDelayMs").getCount() >= 60_000L);
        reader.snapshotState(1L);
        reader.notifyCheckpointComplete(1L);
        Assertions.assertEquals(
                0L, metricsContext.counter("MilvusCdcSourceReceiveToCommitDelayMs").getCount());
        Assertions.assertEquals(
                messageTimestampMs,
                metricsContext.counter("MilvusCdcSourceLastCommitTsAllSplitsMinMs").getCount());
        Assertions.assertEquals(
                messageTimestampMs,
                metricsContext.counter("MilvusCdcSourceLastCommitTsAllSplitsMaxMs").getCount());

        long receiveToCommitDelay =
                metricsContext.counter("MilvusCdcSourceReceiveToCommitDelayMs").getCount();
        reader.snapshotState(2L);
        reader.notifyCheckpointComplete(2L);
        Assertions.assertEquals(
                receiveToCommitDelay,
                metricsContext.counter("MilvusCdcSourceReceiveToCommitDelayMs").getCount());
        reader.close();
    }

    @Test
    void abortedCheckpointRemovesPendingOffsets() throws Exception {
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(),
                        readerConfig(),
                        new FakeClientFactory(Collections.emptyList()),
                        schemaRegistry());
        MilvusCdcSplit split = testSplit();
        split.setCurrentOffset(offset("message-aborted", System.currentTimeMillis()));

        try {
            assignedSplits(reader).put(split.splitId(), split);
            reader.snapshotState(7L);
            Assertions.assertTrue(pendingCheckpointOffsets(reader).containsKey(7L));

            reader.notifyCheckpointAborted(7L);

            Assertions.assertFalse(pendingCheckpointOffsets(reader).containsKey(7L));
        } finally {
            reader.close();
        }
    }

    @Test
    void sourceMetricsTrackCommitWatermarksAcrossSplits() throws Exception {
        AbstractMetricsContext metricsContext = new AbstractMetricsContext() {};
        Object sourceMetrics = sourceMetrics(metricsContext);
        long now = System.currentTimeMillis();
        invokeSourceMetrics(
                sourceMetrics,
                "markReceivedMessage",
                new Class<?>[] {String.class, Long.class, long.class},
                "slow-split",
                now - 9_000L,
                now);
        invokeSourceMetrics(
                sourceMetrics,
                "markReceivedMessage",
                new Class<?>[] {String.class, Long.class, long.class},
                "fast-split",
                now - 1_000L,
                now);
        Map<String, MilvusCdcOffset> checkpointOffsetsBySplit = new HashMap<>();
        checkpointOffsetsBySplit.put("slow-split", offset("slow-message", now - 9_000L));
        checkpointOffsetsBySplit.put("fast-split", offset("fast-message", now - 5_000L));

        invokeSourceMetrics(
                sourceMetrics,
                "markCompletedCheckpoint",
                new Class<?>[] {Map.class},
                checkpointOffsetsBySplit);

        Assertions.assertTrue(
                metricsContext.counter("MilvusCdcSourceReceiveDelayMs").getCount() >= 9_000L);
        Assertions.assertEquals(
                4_000L, metricsContext.counter("MilvusCdcSourceReceiveToCommitDelayMs").getCount());
        Assertions.assertEquals(
                now - 9_000L,
                metricsContext.counter("MilvusCdcSourceLastCommitTsAllSplitsMinMs").getCount());
        Assertions.assertEquals(
                now - 5_000L,
                metricsContext.counter("MilvusCdcSourceLastCommitTsAllSplitsMaxMs").getCount());

        long receiveToCommitDelay =
                metricsContext.counter("MilvusCdcSourceReceiveToCommitDelayMs").getCount();
        invokeSourceMetrics(
                sourceMetrics,
                "markCompletedCheckpoint",
                new Class<?>[] {Map.class},
                checkpointOffsetsBySplit);
        Assertions.assertEquals(
                receiveToCommitDelay,
                metricsContext.counter("MilvusCdcSourceReceiveToCommitDelayMs").getCount());
    }

    @Test
    void sourceMetricsRefreshReceiveToCommitDelayWhenReceivingNewMessage() throws Exception {
        AbstractMetricsContext metricsContext = new AbstractMetricsContext() {};
        Object sourceMetrics = sourceMetrics(metricsContext);
        long now = System.currentTimeMillis();
        long committedTimestampMs = now - 10_000L;
        long receivedTimestampMs = now - 6_000L;

        invokeSourceMetrics(
                sourceMetrics,
                "initializeCommittedOffset",
                new Class<?>[] {String.class, MilvusCdcOffset.class},
                "split-0",
                offset("committed-message", committedTimestampMs));
        invokeSourceMetrics(
                sourceMetrics,
                "markReceivedMessage",
                new Class<?>[] {String.class, Long.class, long.class},
                "split-0",
                receivedTimestampMs,
                now);

        Assertions.assertEquals(
                4_000L, metricsContext.counter("MilvusCdcSourceReceiveToCommitDelayMs").getCount());

        Map<String, MilvusCdcOffset> checkpointOffsetsBySplit = new HashMap<>();
        checkpointOffsetsBySplit.put("split-0", offset("received-message", receivedTimestampMs));
        invokeSourceMetrics(
                sourceMetrics,
                "markCompletedCheckpoint",
                new Class<?>[] {Map.class},
                checkpointOffsetsBySplit);
        Assertions.assertEquals(
                0L, metricsContext.counter("MilvusCdcSourceReceiveToCommitDelayMs").getCount());
    }

    @Test
    void sourceMetricsKeepNegativeDelays() throws Exception {
        AbstractMetricsContext metricsContext = new AbstractMetricsContext() {};
        Object sourceMetrics = sourceMetrics(metricsContext);
        long now = System.currentTimeMillis();

        invokeSourceMetrics(
                sourceMetrics,
                "markReceivedMessage",
                new Class<?>[] {String.class, Long.class, long.class},
                "split-0",
                now + 1_000L,
                now);
        Assertions.assertEquals(
                -1_000L, metricsContext.counter("MilvusCdcSourceReceiveDelayMs").getCount());

        Map<String, MilvusCdcOffset> checkpointOffsetsBySplit = new HashMap<>();
        checkpointOffsetsBySplit.put("split-0", offset("message-0", now + 2_000L));
        invokeSourceMetrics(
                sourceMetrics,
                "markCompletedCheckpoint",
                new Class<?>[] {Map.class},
                checkpointOffsetsBySplit);
        Assertions.assertEquals(
                -1_000L,
                metricsContext.counter("MilvusCdcSourceReceiveToCommitDelayMs").getCount());
    }

    @Test
    void alignStartBarrierBeforeOutput() throws Exception {
        Map<String, List<DumpMessagesResponse>> responses = new HashMap<>();
        responses.put(
                "pchannel-0",
                asList(
                        response("start-0", insertPayload(100L), "Insert", 100L, "safe-0"),
                        response("message-0", insertPayload(101L), "Insert", 101L, "safe-1")));
        responses.put(
                "pchannel-1",
                Collections.singletonList(
                        response("start-1", insertPayload(100L), "Insert", 100L, "safe-2")));
        FakeClientFactory clientFactory = new FakeClientFactory(responses);
        TestContext context = new TestContext();
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(context, readerConfig(), clientFactory, schemaRegistry());

        reader.addSplits(
                asList(
                        barrierSplit("pchannel-0", "start-0", 100L),
                        barrierSplit("pchannel-1", "start-1", 100L)));

        waitUntilRequestCount(clientFactory, 2);
        Assertions.assertTrue(
                clientFactory.requests.stream()
                        .allMatch(DumpMessagesRequest::getIncludeStartMessage));

        TestCollector collector = new TestCollector();
        for (int i = 0; i < 30 && collector.rows.isEmpty(); i++) {
            reader.pollNext(collector);
            Thread.sleep(10L);
        }

        Assertions.assertTrue(context.sourceEvents.isEmpty());
        Assertions.assertEquals(1, collector.rows.size());
        Assertions.assertEquals(1001L, collector.rows.get(0).getField(0));
        MilvusCdcSplit checkpoint =
                reader.snapshotState(1L).stream()
                        .filter(split -> "pchannel-0".equals(split.getPchannel()))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        Assertions.assertEquals("message-0", checkpoint.getCurrentOffset().getConsumedMessageId());
        Assertions.assertEquals("safe-1", checkpoint.getCurrentOffset().getResumeMessageId());
        reader.close();
    }

    @Test
    void startBarrierUsesConsumedMessageIdWhenReplayedTxnHasSameTimetick() throws Exception {
        long txnTimetick = 120L;
        Map<String, List<DumpMessagesResponse>> responses = new HashMap<>();
        responses.put(
                "pchannel-0",
                asList(
                        response(
                                "txn-begin",
                                ByteString.EMPTY,
                                "BeginTxn",
                                txnTimetick,
                                "safe-before-txn"),
                        transactionalResponse(
                                "txn-insert",
                                insertPayload(111L),
                                "Insert",
                                txnTimetick,
                                "safe-before-txn"),
                        transactionalResponse(
                                "txn-delete",
                                deletePayload(112L),
                                "Delete",
                                txnTimetick,
                                "safe-before-txn"),
                        response(
                                "txn-commit",
                                ByteString.EMPTY,
                                "CommitTxn",
                                txnTimetick,
                                "safe-before-txn"),
                        response(
                                "message-after-txn",
                                insertPayload(121L),
                                "Insert",
                                121L,
                                "safe-after-txn")));
        FakeClientFactory clientFactory = new FakeClientFactory(responses);
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(), readerConfig(), clientFactory, schemaRegistry());

        reader.addSplits(
                Collections.singletonList(
                        barrierSplitWithResume(
                                "pchannel-0", "safe-before-txn", "txn-commit", txnTimetick)));

        waitUntilRequestCount(clientFactory, 1);
        Assertions.assertTrue(clientFactory.requests.get(0).getIncludeStartMessage());
        Assertions.assertEquals(
                "safe-before-txn", clientFactory.requests.get(0).getStartMessageId().getId());

        TestCollector collector = new TestCollector();
        for (int i = 0; i < 30 && collector.rows.isEmpty(); i++) {
            reader.pollNext(collector);
            Thread.sleep(10L);
        }

        Assertions.assertEquals(1, collector.rows.size());
        Assertions.assertEquals(RowKind.INSERT, collector.rows.get(0).getRowKind());
        Assertions.assertEquals(1001L, collector.rows.get(0).getField(0));
        Assertions.assertEquals(
                "message-after-txn",
                collector.rows.get(0).getOptions().get(MilvusCdcRowMetadata.CURRENT_MESSAGE_ID));
        MilvusCdcSplit checkpoint = reader.snapshotState(1L).get(0);
        Assertions.assertEquals(
                "message-after-txn", checkpoint.getCurrentOffset().getConsumedMessageId());
        Assertions.assertEquals(
                "safe-after-txn", checkpoint.getCurrentOffset().getResumeMessageId());
        reader.close();
    }

    @Test
    void resumeOnlyStartOffsetDoesNotUseLocalBarrier() throws Exception {
        Map<String, List<DumpMessagesResponse>> responses = new HashMap<>();
        responses.put(
                "pchannel-0",
                Collections.singletonList(
                        response("message-0", insertPayload(101L), "Insert", 101L, "safe-1")));
        FakeClientFactory clientFactory = new FakeClientFactory(responses);
        TestContext context = new TestContext();
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(context, readerConfig(), clientFactory, schemaRegistry());

        reader.addSplits(Collections.singletonList(startSplit("pchannel-0", "safe-start", 100L)));

        waitUntilRequestCount(clientFactory, 1);
        Assertions.assertFalse(clientFactory.requests.get(0).getIncludeStartMessage());
        Assertions.assertEquals(
                "safe-start", clientFactory.requests.get(0).getStartMessageId().getId());

        TestCollector collector = new TestCollector();
        for (int i = 0; i < 30 && collector.rows.isEmpty(); i++) {
            reader.pollNext(collector);
            Thread.sleep(10L);
        }

        Assertions.assertTrue(context.sourceEvents.isEmpty());
        Assertions.assertEquals(1, collector.rows.size());
        Assertions.assertEquals(1001L, collector.rows.get(0).getField(0));
        MilvusCdcSplit checkpoint = reader.snapshotState(1L).get(0);
        Assertions.assertEquals("message-0", checkpoint.getCurrentOffset().getConsumedMessageId());
        Assertions.assertEquals("safe-1", checkpoint.getCurrentOffset().getResumeMessageId());
        reader.close();
    }

    @Test
    void resumeEqualsConsumedStartOffsetDoesNotUseLocalBarrier() throws Exception {
        Map<String, List<DumpMessagesResponse>> responses = new HashMap<>();
        responses.put(
                "pchannel-0",
                Collections.singletonList(
                        response("message-0", insertPayload(101L), "Insert", 101L, "safe-1")));
        FakeClientFactory clientFactory = new FakeClientFactory(responses);
        TestContext context = new TestContext();
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(context, readerConfig(), clientFactory, schemaRegistry());

        reader.addSplits(
                Collections.singletonList(
                        barrierSplitWithResume("pchannel-0", "safe-start", "safe-start", 100L)));

        waitUntilRequestCount(clientFactory, 1);
        Assertions.assertFalse(clientFactory.requests.get(0).getIncludeStartMessage());
        Assertions.assertEquals(
                "safe-start", clientFactory.requests.get(0).getStartMessageId().getId());

        TestCollector collector = new TestCollector();
        for (int i = 0; i < 30 && collector.rows.isEmpty(); i++) {
            reader.pollNext(collector);
            Thread.sleep(10L);
        }

        Assertions.assertTrue(context.sourceEvents.isEmpty());
        Assertions.assertEquals(1, collector.rows.size());
        Assertions.assertEquals(1001L, collector.rows.get(0).getField(0));
        MilvusCdcSplit checkpoint = reader.snapshotState(1L).get(0);
        Assertions.assertEquals("message-0", checkpoint.getCurrentOffset().getConsumedMessageId());
        Assertions.assertEquals("safe-1", checkpoint.getCurrentOffset().getResumeMessageId());
        reader.close();
    }

    @Test
    void failWhenStartBarrierMessageWasNotReturnedBeforeFirstVisibleMessage() throws Exception {
        FakeClientFactory clientFactory =
                new FakeClientFactory(
                        Collections.singletonMap(
                                "pchannel-0",
                                Collections.singletonList(
                                        response(
                                                "other-message",
                                                insertPayload(101L),
                                                "Insert",
                                                101L,
                                                "safe-1"))));
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(), readerConfig(), clientFactory, schemaRegistry());

        try {
            reader.addSplits(
                    Collections.singletonList(barrierSplit("pchannel-0", "start-0", 100L)));
            assertReaderFailureContains(
                    reader, "was passed before target consumed message was found");
        } finally {
            reader.close();
        }
    }

    private Object sourceMetrics(MetricsContext metricsContext) throws Exception {
        Class<?> metricsClass =
                Class.forName(MilvusCdcSourceReader.class.getName() + "$MilvusCdcSourceMetrics");
        Constructor<?> constructor = metricsClass.getDeclaredConstructor(MetricsContext.class);
        constructor.setAccessible(true);
        return constructor.newInstance(metricsContext);
    }

    @SuppressWarnings("unchecked")
    private Map<Long, Map<String, MilvusCdcOffset>> pendingCheckpointOffsets(
            MilvusCdcSourceReader reader) throws Exception {
        Field field = MilvusCdcSourceReader.class.getDeclaredField("pendingCheckpointOffsets");
        field.setAccessible(true);
        return (Map<Long, Map<String, MilvusCdcOffset>>) field.get(reader);
    }

    @SuppressWarnings("unchecked")
    private Map<String, MilvusCdcSplit> assignedSplits(MilvusCdcSourceReader reader)
            throws Exception {
        Field field = MilvusCdcSourceReader.class.getDeclaredField("assignedSplits");
        field.setAccessible(true);
        return (Map<String, MilvusCdcSplit>) field.get(reader);
    }

    private void invokeSourceMetrics(
            Object sourceMetrics, String methodName, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        Method method = sourceMetrics.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        method.invoke(sourceMetrics, args);
    }

    private ReadonlyConfig readerConfig() {
        return readerConfig(10, null);
    }

    private ReadonlyConfig readerConfig(int queueCapacity) {
        return readerConfig(queueCapacity, null);
    }

    private ReadonlyConfig readerConfig(List<String> messageTypes) {
        return readerConfig(10, messageTypes);
    }

    private ReadonlyConfig readerConfig(int queueCapacity, List<String> messageTypes) {
        Map<String, Object> config = new HashMap<>();
        config.put("queue_capacity", queueCapacity);
        config.put(
                "database_collections",
                Collections.singletonMap("default", Collections.singletonList("collection_a")));
        if (messageTypes != null) {
            config.put("message_types", messageTypes);
        }
        return ReadonlyConfig.fromMap(config);
    }

    private MilvusCdcCollectionSchemaRegistry schemaRegistry() {
        CatalogTable catalogTable = catalogTable();
        MilvusCdcCollectionSchema schema =
                MilvusCdcCollectionSchema.builder()
                        .sourceDatabase("default")
                        .sourceCollection("collection_a")
                        .catalogTable(catalogTable)
                        .rowType(catalogTable.getSeaTunnelRowType())
                        .tableId(catalogTable.getTablePath().toString())
                        .primaryKeyField("id")
                        .primaryKeyIndex(0)
                        .build();
        return new MilvusCdcCollectionSchemaRegistry(Collections.singletonList(schema));
    }

    private CatalogTable catalogTable() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                Collections.singletonList(
                                        PhysicalColumn.builder()
                                                .name("id")
                                                .dataType(BasicType.LONG_TYPE)
                                                .build()))
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id"), false))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("milvus", "default", null, "collection_a"),
                tableSchema,
                Collections.emptyMap(),
                new ArrayList<>(),
                "");
    }

    private MilvusCdcSplit testSplit() {
        return MilvusCdcSplit.builder()
                .splitId("pchannel-0")
                .pchannel("pchannel-0")
                .startOffset(MilvusCdcOffset.builder().timetick(0L).build())
                .build();
    }

    private MilvusCdcSplit startSplit(String pchannel, String resumeMessageId, long timetick) {
        return MilvusCdcSplit.builder()
                .splitId(pchannel)
                .pchannel(pchannel)
                .startOffset(
                        MilvusCdcOffset.builder()
                                .walName(WALName.Pulsar.name())
                                .resumeMessageId(resumeMessageId)
                                .timetick(timetick)
                                .build())
                .build();
    }

    private MilvusCdcSplit barrierSplit(String pchannel, String messageId, long timetick) {
        return barrierSplitWithResume(pchannel, "safe-" + messageId, messageId, timetick);
    }

    private MilvusCdcSplit barrierSplitWithResume(
            String pchannel, String resumeMessageId, String consumedMessageId, long timetick) {
        return MilvusCdcSplit.builder()
                .splitId(pchannel)
                .pchannel(pchannel)
                .startOffset(
                        MilvusCdcOffset.builder()
                                .walName(WALName.Pulsar.name())
                                .resumeMessageId(resumeMessageId)
                                .consumedMessageId(consumedMessageId)
                                .timetick(timetick)
                                .build())
                .build();
    }

    private DumpMessagesResponse response(
            String messageId,
            ByteString payload,
            String messageType,
            long timetick,
            String resumeMessageId) {
        return DumpMessagesResponse.newBuilder()
                .setMessage(
                        ImmutableMessage.newBuilder()
                                .setId(
                                        MessageID.newBuilder()
                                                .setId(messageId)
                                                .setWALName(WALName.Pulsar))
                                .setPayload(payload)
                                .putProperties("_t", milvusMessageType(messageType))
                                .putProperties("timetick", Long.toString(timetick))
                                .putProperties("_lc", resumeMessageId))
                .build();
    }

    private DumpMessagesResponse transactionalResponse(
            String messageId,
            ByteString payload,
            String messageType,
            long timetick,
            String resumeMessageId) {
        DumpMessagesResponse response =
                response(messageId, payload, messageType, timetick, resumeMessageId);
        return response.toBuilder()
                .setMessage(response.getMessage().toBuilder().putProperties("_tx", "txn-context"))
                .build();
    }

    private String milvusMessageType(String messageType) {
        switch (messageType) {
            case "Insert":
                return "2";
            case "Delete":
                return "3";
            case "BeginTxn":
                return "900";
            case "CommitTxn":
                return "901";
            case "RollbackTxn":
                return "902";
            case "Txn":
                return "999";
            case "Unknown":
                return "12345";
            default:
                throw new IllegalArgumentException("Unsupported test message type: " + messageType);
        }
    }

    private static void waitUntilRequestCount(FakeClientFactory clientFactory, int count)
            throws InterruptedException {
        for (int i = 0; i < 30 && clientFactory.requests.size() < count; i++) {
            Thread.sleep(10L);
        }
        Assertions.assertTrue(clientFactory.requests.size() >= count);
    }

    private void assertReaderFailureContains(MilvusCdcSourceReader reader, String expected)
            throws Exception {
        TestCollector collector = new TestCollector();
        IllegalStateException failure = null;
        for (int i = 0; i < 30 && failure == null; i++) {
            try {
                reader.pollNext(collector);
            } catch (IllegalStateException e) {
                failure = e;
            }
            Thread.sleep(10L);
        }
        Assertions.assertNotNull(failure);
        Assertions.assertTrue(failure.getMessage().contains("Milvus CDC reader failed"));
        Assertions.assertTrue(failure.getCause().getMessage().contains(expected));
        Assertions.assertTrue(collector.rows.isEmpty());
    }

    private void assertSingleResponseFailure(DumpMessagesResponse response, String expected)
            throws Exception {
        FakeClientFactory clientFactory =
                new FakeClientFactory(Collections.singletonList(response));
        MilvusCdcSourceReader reader =
                new MilvusCdcSourceReader(
                        new TestContext(), readerConfig(), clientFactory, schemaRegistry());
        try {
            reader.addSplits(Collections.singletonList(testSplit()));
            assertReaderFailureContains(reader, expected);
        } finally {
            reader.close();
        }
    }

    @SafeVarargs
    private static <T> List<T> asList(T... values) {
        List<T> result = new ArrayList<>();
        Collections.addAll(result, values);
        return result;
    }

    private ByteString insertPayload(long timestamp) {
        return insertPayload("default", "collection_a", timestamp);
    }

    private ByteString insertPayload(String database, String collection, long timestamp) {
        return insertPayload(database, collection, new long[] {timestamp});
    }

    private ByteString insertPayload(String database, String collection, long... timestamps) {
        LongArray.Builder rowIds = LongArray.newBuilder();
        LongArray.Builder fieldValues = LongArray.newBuilder();
        for (int i = 0; i < timestamps.length; i++) {
            long id = 1001L + i;
            rowIds.addData(id);
            fieldValues.addData(id);
        }
        Msg.InsertRequest.Builder request =
                Msg.InsertRequest.newBuilder()
                        .setDbName(database)
                        .setCollectionName(collection)
                        .setNumRows(timestamps.length)
                        .addFieldsData(
                                FieldData.newBuilder()
                                        .setFieldName("id")
                                        .setType(DataType.Int64)
                                        .setScalars(
                                                ScalarField.newBuilder().setLongData(fieldValues)));
        for (long timestamp : timestamps) {
            request.addTimestamps(timestamp);
        }
        return request.addAllRowIDs(rowIds.getDataList()).build().toByteString();
    }

    private ByteString deletePayload(long timestamp) {
        return deletePayloads(timestamp);
    }

    private ByteString deletePayloads(long... timestamps) {
        LongArray.Builder primaryKeys = LongArray.newBuilder();
        for (int i = 0; i < timestamps.length; i++) {
            primaryKeys.addData(1001L + i);
        }
        Msg.DeleteRequest.Builder request =
                Msg.DeleteRequest.newBuilder()
                        .setDbName("default")
                        .setCollectionName("collection_a")
                        .setNumRows(timestamps.length)
                        .setPrimaryKeys(IDs.newBuilder().setIntId(primaryKeys));
        for (long timestamp : timestamps) {
            request.addTimestamps(timestamp);
        }
        return request.build().toByteString();
    }

    private void assertTxnRow(
            SeaTunnelRow row,
            RowKind rowKind,
            long id,
            String currentMessageId,
            long timetick,
            boolean messageEnd) {
        Assertions.assertEquals(rowKind, row.getRowKind());
        Assertions.assertEquals(id, row.getField(0));
        Assertions.assertEquals(
                currentMessageId, row.getOptions().get(MilvusCdcRowMetadata.CURRENT_MESSAGE_ID));
        Assertions.assertEquals(timetick, row.getOptions().get(MilvusCdcRowMetadata.TIMETICK));
        Assertions.assertEquals(
                timetick, row.getOptions().get(MilvusCdcRowMetadata.EVENT_TIMESTAMP));
        if (messageEnd) {
            Assertions.assertEquals(true, row.getOptions().get(MilvusCdcRowMetadata.MESSAGE_END));
        } else {
            Assertions.assertNull(row.getOptions().get(MilvusCdcRowMetadata.MESSAGE_END));
        }
    }

    private static MilvusCdcOffset offset(String messageId, long physicalTimestampMs) {
        return MilvusCdcOffset.builder()
                .walName(WALName.Pulsar.name())
                .consumedMessageId(messageId)
                .timetick(hybridTimestamp(physicalTimestampMs))
                .build();
    }

    private static long hybridTimestamp(long physicalTimestampMs) {
        return physicalTimestampMs << 18;
    }

    private static class FakeClientFactory implements MilvusCdcClientFactory {
        private static final long serialVersionUID = 1L;

        private final List<DumpMessagesResponse> responses;
        private final Map<String, List<DumpMessagesResponse>> responsesByPchannel;
        private final List<DumpMessagesRequest> requests = new CopyOnWriteArrayList<>();
        private final boolean streamEnds;

        private FakeClientFactory(List<DumpMessagesResponse> responses) {
            this(responses, false);
        }

        private FakeClientFactory(List<DumpMessagesResponse> responses, boolean streamEnds) {
            this.responses = responses;
            this.responsesByPchannel = Collections.emptyMap();
            this.streamEnds = streamEnds;
        }

        private FakeClientFactory(Map<String, List<DumpMessagesResponse>> responsesByPchannel) {
            this.responses = Collections.emptyList();
            this.responsesByPchannel = responsesByPchannel;
            this.streamEnds = false;
        }

        @Override
        public MilvusCdcMessageClient create() {
            return new FakeClient(responses, responsesByPchannel, requests, streamEnds);
        }
    }

    private static class FakeClient implements MilvusCdcMessageClient {
        private final List<DumpMessagesResponse> responses;
        private final Map<String, List<DumpMessagesResponse>> responsesByPchannel;
        private final List<DumpMessagesRequest> requests;
        private final boolean streamEnds;
        private final AtomicBoolean closed = new AtomicBoolean();

        private FakeClient(
                List<DumpMessagesResponse> responses,
                Map<String, List<DumpMessagesResponse>> responsesByPchannel,
                List<DumpMessagesRequest> requests,
                boolean streamEnds) {
            this.responses = responses;
            this.responsesByPchannel = responsesByPchannel;
            this.requests = requests;
            this.streamEnds = streamEnds;
        }

        @Override
        public Iterator<DumpMessagesResponse> dumpMessages(DumpMessagesRequest request) {
            requests.add(request);
            Iterator<DumpMessagesResponse> iterator =
                    responsesByPchannel.getOrDefault(request.getPchannel(), responses).iterator();
            if (streamEnds) {
                return iterator;
            }
            return new Iterator<DumpMessagesResponse>() {
                @Override
                public boolean hasNext() {
                    while (!closed.get()) {
                        if (iterator.hasNext()) {
                            return true;
                        }
                        try {
                            Thread.sleep(10L);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return false;
                        }
                    }
                    return false;
                }

                @Override
                public DumpMessagesResponse next() {
                    return iterator.next();
                }
            };
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    private static class BlockingProducerClientFactory implements MilvusCdcClientFactory {
        private static final long serialVersionUID = 1L;

        private final DumpMessagesResponse healthyResponse;
        private final int queueCapacity;
        private final CountDownLatch queueSaturated = new CountDownLatch(1);
        private final CountDownLatch failureTriggered = new CountDownLatch(1);
        private final CountDownLatch clientsClosed = new CountDownLatch(2);
        private final AtomicInteger healthyResponseCount = new AtomicInteger();

        private BlockingProducerClientFactory(
                DumpMessagesResponse healthyResponse, int queueCapacity) {
            this.healthyResponse = healthyResponse;
            this.queueCapacity = queueCapacity;
        }

        @Override
        public MilvusCdcMessageClient create() {
            return new MilvusCdcMessageClient() {
                private final AtomicBoolean clientClosed = new AtomicBoolean();

                @Override
                public Iterator<DumpMessagesResponse> dumpMessages(DumpMessagesRequest request) {
                    if ("failing-pchannel".equals(request.getPchannel())) {
                        return failingIterator();
                    }
                    return healthyIterator(clientClosed);
                }

                @Override
                public void close() {
                    if (clientClosed.compareAndSet(false, true)) {
                        clientsClosed.countDown();
                    }
                }
            };
        }

        private Iterator<DumpMessagesResponse> failingIterator() {
            return new Iterator<DumpMessagesResponse>() {
                @Override
                public boolean hasNext() {
                    try {
                        if (!queueSaturated.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException(
                                    "Healthy split did not fill the queue.");
                        }
                        failureTriggered.countDown();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "Interrupted while waiting for queue fill.", e);
                    }
                    return false;
                }

                @Override
                public DumpMessagesResponse next() {
                    throw new IllegalStateException("No response is available.");
                }
            };
        }

        private Iterator<DumpMessagesResponse> healthyIterator(AtomicBoolean clientClosed) {
            return new Iterator<DumpMessagesResponse>() {
                @Override
                public boolean hasNext() {
                    return !clientClosed.get();
                }

                @Override
                public DumpMessagesResponse next() {
                    int responseCount = healthyResponseCount.incrementAndGet();
                    if (responseCount == queueCapacity + 1) {
                        queueSaturated.countDown();
                    }
                    return healthyResponse;
                }
            };
        }

        private boolean awaitClientsClosed() throws InterruptedException {
            return clientsClosed.await(5, TimeUnit.SECONDS);
        }

        private boolean awaitFailureTriggered() throws InterruptedException {
            return failureTriggered.await(5, TimeUnit.SECONDS);
        }
    }

    private static class TestCollector implements Collector<SeaTunnelRow> {
        private final Object checkpointLock = new Object();
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }

    private static class FailingCollector implements Collector<SeaTunnelRow> {
        private final Object checkpointLock = new Object();
        private final int failOnCollectCount;
        private int collectCount;

        private FailingCollector(int failOnCollectCount) {
            this.failOnCollectCount = failOnCollectCount;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            collectCount++;
            if (collectCount == failOnCollectCount) {
                throw new RuntimeException("collect failed");
            }
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }

    private static class TestContext implements SourceReader.Context {
        private final AtomicInteger splitRequests = new AtomicInteger();
        private final List<SourceEvent> sourceEvents = new CopyOnWriteArrayList<>();
        private final MetricsContext metricsContext;

        private TestContext() {
            this(null);
        }

        private TestContext(MetricsContext metricsContext) {
            this.metricsContext = metricsContext;
        }

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.UNBOUNDED;
        }

        @Override
        public void signalNoMoreElement() {}

        @Override
        public void sendSplitRequest() {
            splitRequests.incrementAndGet();
        }

        @Override
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
            sourceEvents.add(sourceEvent);
        }

        @Override
        public MetricsContext getMetricsContext() {
            return metricsContext;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }
}
