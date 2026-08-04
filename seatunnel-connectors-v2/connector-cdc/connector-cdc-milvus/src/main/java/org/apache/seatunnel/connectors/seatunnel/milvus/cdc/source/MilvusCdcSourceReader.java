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

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.config.MilvusCdcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.client.MilvusCdcClientFactory;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.client.MilvusCdcMessageClient;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.offset.MilvusCdcOffset;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.parser.MilvusCdcControlMessage;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.parser.MilvusCdcControlMessageType;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.parser.MilvusCdcDmlMessage;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.parser.MilvusCdcMessage;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.parser.MilvusCdcMessageKind;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.parser.MilvusCdcRecord;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.parser.MilvusCdcRecordParser;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.parser.MilvusCdcRowMetadata;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.schema.MilvusCdcCollectionSchemaRegistry;

import io.milvus.grpc.DumpMessagesRequest;
import io.milvus.grpc.DumpMessagesResponse;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.Status;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class MilvusCdcSourceReader implements SourceReader<SeaTunnelRow, MilvusCdcSplit> {

    private final SourceReader.Context context;
    private final MilvusCdcClientFactory clientFactory;
    private final MilvusCdcRecordParser parser;
    private final BlockingQueue<QueuedMessage> messages;
    private final Map<String, MilvusCdcSplit> assignedSplits;
    private final List<MilvusCdcMessageClient> activeClients;
    private final ExecutorService executorService;
    private final AtomicBoolean closed;
    private final AtomicReference<Throwable> failure;
    private final MilvusCdcSourceMetrics sourceMetrics;
    private final Map<Long, Map<String, MilvusCdcOffset>> pendingCheckpointOffsets;
    private final Object stateLock = new Object();

    public MilvusCdcSourceReader(
            SourceReader.Context context,
            ReadonlyConfig config,
            MilvusCdcClientFactory clientFactory,
            MilvusCdcCollectionSchemaRegistry schemaRegistry) {
        this.context = context;
        this.clientFactory = clientFactory;
        this.parser =
                new MilvusCdcRecordParser(
                        MilvusCdcSourceConfigParser.parseMessageTypes(config), schemaRegistry);
        this.messages = new LinkedBlockingQueue<>(config.get(MilvusCdcSourceConfig.QUEUE_CAPACITY));
        this.assignedSplits = new ConcurrentHashMap<>();
        this.activeClients = new CopyOnWriteArrayList<>();
        this.executorService =
                Executors.newCachedThreadPool(
                        new ThreadFactory() {
                            @Override
                            public Thread newThread(Runnable runnable) {
                                Thread thread = new Thread(runnable, "milvus-cdc-reader");
                                thread.setDaemon(true);
                                return thread;
                            }
                        });
        this.closed = new AtomicBoolean(false);
        this.failure = new AtomicReference<>();
        this.sourceMetrics = new MilvusCdcSourceMetrics(context.getMetricsContext());
        this.pendingCheckpointOffsets = new ConcurrentHashMap<>();
    }

    @Override
    public void open() {
        context.sendSplitRequest();
    }

    @Override
    public void close() throws IOException {
        closed.set(true);
        for (MilvusCdcMessageClient client : activeClients) {
            try {
                client.close();
            } catch (Throwable throwable) {
                log.warn("Failed to close Milvus CDC message client.", throwable);
            }
        }
        executorService.shutdownNow();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        Throwable error = failure.get();
        if (error != null) {
            throw new IllegalStateException("Milvus CDC reader failed.", error);
        }
        QueuedMessage queuedMessage = messages.peek();
        if (queuedMessage == null) {
            return;
        }
        Object checkpointLock = output.getCheckpointLock();
        synchronized (checkpointLock) {
            synchronized (stateLock) {
                for (MilvusCdcRecord record : queuedMessage.records) {
                    output.collect(record.getRow());
                }
                MilvusCdcSplit split = assignedSplits.get(queuedMessage.splitId);
                if (split != null && queuedMessage.offset != null) {
                    split.setCurrentOffset(queuedMessage.offset);
                }
                messages.poll();
            }
        }
        logProcessedMessage(queuedMessage);
    }

    @Override
    public List<MilvusCdcSplit> snapshotState(long checkpointId) {
        synchronized (stateLock) {
            List<MilvusCdcSplit> state = new ArrayList<>(assignedSplits.size());
            Map<String, MilvusCdcOffset> checkpointOffsets = new HashMap<>();
            for (MilvusCdcSplit split : assignedSplits.values()) {
                MilvusCdcSplit splitCopy = MilvusCdcSplitEnumerator.copySplit(split);
                state.add(splitCopy);
                MilvusCdcOffset currentOffset = splitCopy.getCurrentOffset();
                if (currentOffset != null && currentOffset.hasTimetick()) {
                    checkpointOffsets.put(splitCopy.splitId(), currentOffset);
                }
            }
            if (!checkpointOffsets.isEmpty()) {
                pendingCheckpointOffsets.put(checkpointId, checkpointOffsets);
            }
            return state;
        }
    }

    @Override
    public void addSplits(List<MilvusCdcSplit> splits) {
        for (MilvusCdcSplit split : splits) {
            MilvusCdcSplit splitCopy = MilvusCdcSplitEnumerator.copySplit(split);
            assignedSplits.put(splitCopy.splitId(), splitCopy);
            sourceMetrics.initializeCommittedOffset(
                    splitCopy.splitId(), splitCopy.getCurrentOffset());
            executorService.submit(() -> fetchSplit(splitCopy));
        }
    }

    @Override
    public void handleNoMoreSplits() {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        Map<String, MilvusCdcOffset> completedCheckpointOffsets =
                pendingCheckpointOffsets.remove(checkpointId);
        if (completedCheckpointOffsets != null) {
            sourceMetrics.markCompletedCheckpoint(completedCheckpointOffsets);
        }
        pendingCheckpointOffsets.keySet().removeIf(id -> id < checkpointId);
        log.debug(
                "Milvus CDC checkpoint {} completed with splits {}.", checkpointId, assignedSplits);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        pendingCheckpointOffsets.remove(checkpointId);
    }

    private void fetchSplit(MilvusCdcSplit split) {
        MilvusCdcMessageClient client = null;
        try {
            client = clientFactory.create();
            activeClients.add(client);
            DumpMessagesRequest request = buildRequest(split);
            Iterator<DumpMessagesResponse> iterator = client.dumpMessages(request);
            if (requiresStartCoordination(split)) {
                alignStartBarrier(split, iterator);
                log.info(
                        "Milvus CDC split {} starts fetching after local barrier.",
                        split.splitId());
            }
            MilvusCdcTxnBuffer txnBuffer = null;
            while (!closed.get() && iterator.hasNext()) {
                DumpMessagesResponse response = iterator.next();
                txnBuffer = handleResponse(split, response, txnBuffer);
            }
            if (!closed.get()) {
                if (txnBuffer != null) {
                    throw new IllegalStateException(
                            "Milvus CDC transaction stream ended before commit. split="
                                    + split.splitId()
                                    + ", beginMessageId="
                                    + txnBuffer.beginMessageId());
                }
                throw new IllegalStateException(
                        "Milvus CDC unbounded DumpMessages stream ended unexpectedly. split="
                                + split.splitId());
            }
        } catch (Throwable throwable) {
            if (!closed.get()) {
                failure.compareAndSet(null, throwable);
            }
        } finally {
            if (client != null) {
                activeClients.remove(client);
                try {
                    client.close();
                } catch (Throwable throwable) {
                    log.warn("Failed to close Milvus CDC message client.", throwable);
                }
            }
        }
    }

    private void alignStartBarrier(MilvusCdcSplit split, Iterator<DumpMessagesResponse> iterator) {
        MilvusCdcOffset barrierOffset = split.effectiveStartOffset();
        while (!closed.get() && iterator.hasNext()) {
            DumpMessagesResponse response = iterator.next();
            validateStatus(response);

            Optional<MilvusCdcOffset> messageOffset = parser.parseOffset(response);
            if (!messageOffset.isPresent()) {
                continue;
            }
            MilvusCdcOffset offset = messageOffset.get();
            if (barrierOffset.hasTimetick() && offset.hasTimetick()) {
                int compare =
                        Long.compareUnsigned(offset.getTimetick(), barrierOffset.getTimetick());
                if (compare < 0) {
                    continue;
                }
                if (compare > 0) {
                    throw new IllegalStateException(
                            String.format(
                                    "Milvus CDC start barrier for split %s was passed before target consumed message was found. targetConsumedMessageId=%s, targetTimetick=%s, currentConsumedMessageId=%s, currentTimetick=%s.",
                                    split.splitId(),
                                    barrierOffset.getConsumedMessageId(),
                                    barrierOffset.getTimetick(),
                                    offset.getConsumedMessageId(),
                                    offset.getTimetick()));
                }
            }
            if (barrierOffset.getConsumedMessageId().equals(offset.getConsumedMessageId())) {
                synchronized (stateLock) {
                    split.setCurrentOffset(offset);
                }
                sourceMetrics.initializeCommittedOffset(split.splitId(), offset);
                log.info(
                        "Milvus CDC split {} reached start barrier consumedMessageId={}, timetick={}.",
                        split.splitId(),
                        offset.getConsumedMessageId(),
                        offset.getTimetick());
                return;
            }
        }
        if (!closed.get()) {
            throw new IllegalStateException(
                    String.format(
                            "Milvus CDC start barrier for split %s was not found. targetConsumedMessageId=%s, targetTimetick=%s.",
                            split.splitId(),
                            barrierOffset.getConsumedMessageId(),
                            barrierOffset.getTimetick()));
        }
    }

    private MilvusCdcTxnBuffer handleResponse(
            MilvusCdcSplit split, DumpMessagesResponse response, MilvusCdcTxnBuffer txnBuffer)
            throws InterruptedException {
        long receiveTimestampMs = System.currentTimeMillis();
        validateStatus(response);
        MilvusCdcMessage parsedMessage = parser.parse(response);
        MilvusCdcOffset offset = parsedMessage.getOffset();
        Long messageEventTimestamp = offset == null ? null : offset.getTimetick();
        markReceivedSourceMessage(split.splitId(), messageEventTimestamp, receiveTimestampMs);

        switch (parsedMessage.getKind()) {
            case DML:
                MilvusCdcDmlMessage dmlMessage = (MilvusCdcDmlMessage) parsedMessage;
                markRecordReceiveMetadata(dmlMessage, receiveTimestampMs);
                if (txnBuffer != null) {
                    txnBuffer.add(dmlMessage);
                    return txnBuffer;
                }
                if (dmlMessage.isTransactional()) {
                    throw new IllegalStateException(
                            "Milvus CDC transaction DML message was received without begin. split="
                                    + split.splitId()
                                    + ", messageId="
                                    + (dmlMessage.getOffset() == null
                                            ? null
                                            : dmlMessage.getOffset().getConsumedMessageId()));
                }
                emitDmlMessage(split.splitId(), dmlMessage, dmlMessage.getOffset());
                return txnBuffer;
            case FILTERED:
                if (txnBuffer != null) {
                    txnBuffer.addCheckpointOnlyMessage();
                    return txnBuffer;
                }
                if (parsedMessage.shouldCheckpoint()) {
                    messages.put(QueuedMessage.forOffset(split.splitId(), parsedMessage));
                }
                return txnBuffer;
            case UNKNOWN:
                if (txnBuffer != null) {
                    throw new IllegalStateException(
                            "Milvus CDC unknown message was received inside transaction. split="
                                    + split.splitId()
                                    + ", messageId="
                                    + (parsedMessage.getOffset() == null
                                            ? null
                                            : parsedMessage.getOffset().getConsumedMessageId()));
                }
                if (parsedMessage.shouldCheckpoint()) {
                    messages.put(QueuedMessage.forOffset(split.splitId(), parsedMessage));
                }
                return txnBuffer;
            case CONTROL:
                return handleControlMessage(
                        split.splitId(), (MilvusCdcControlMessage) parsedMessage, txnBuffer);
            case EMPTY:
                return txnBuffer;
            default:
                throw new IllegalStateException(
                        "Unknown Milvus CDC message kind: " + parsedMessage.getKind());
        }
    }

    private MilvusCdcTxnBuffer handleControlMessage(
            String splitId, MilvusCdcControlMessage message, MilvusCdcTxnBuffer txnBuffer)
            throws InterruptedException {
        MilvusCdcControlMessageType controlType = message.getControlType();
        switch (controlType) {
            case BEGIN_TXN:
                if (txnBuffer != null) {
                    throw new IllegalStateException(
                            "Milvus CDC nested transaction is not supported. split=" + splitId);
                }
                return new MilvusCdcTxnBuffer(message.getOffset());
            case COMMIT_TXN:
                if (txnBuffer == null) {
                    throw new IllegalStateException(
                            "Milvus CDC commit transaction message was received without begin. split="
                                    + splitId
                                    + ", messageId="
                                    + (message.getOffset() == null
                                            ? null
                                            : message.getOffset().getConsumedMessageId()));
                }
                emitTxnMessage(splitId, txnBuffer, message.getOffset());
                return null;
            case ROLLBACK_TXN:
                throw new IllegalStateException(
                        "Milvus CDC rollback transaction message is not supported by DumpMessages. split="
                                + splitId
                                + ", messageId="
                                + (message.getOffset() == null
                                        ? null
                                        : message.getOffset().getConsumedMessageId()));
            case TXN:
                throw new IllegalStateException(
                        "Milvus CDC aggregated transaction message is not supported by DumpMessages. split="
                                + splitId
                                + ", messageId="
                                + (message.getOffset() == null
                                        ? null
                                        : message.getOffset().getConsumedMessageId()));
            default:
                throw new IllegalStateException(
                        "Unknown Milvus CDC control message type: " + controlType);
        }
    }

    private void emitDmlMessage(
            String splitId, MilvusCdcDmlMessage dmlMessage, MilvusCdcOffset outputOffset)
            throws InterruptedException {
        List<MilvusCdcRecord> records = new ArrayList<>(dmlMessage.getRecords());
        messages.put(
                QueuedMessage.forDmlRecords(
                        splitId, records, outputOffset, dmlMessage.getMessageType().name()));
    }

    private void emitTxnMessage(
            String splitId, MilvusCdcTxnBuffer txnBuffer, MilvusCdcOffset commitOffset)
            throws InterruptedException {
        List<MilvusCdcRecord> records = txnBuffer.orderedRecords();
        if (records.isEmpty()) {
            if (commitOffset != null) {
                messages.put(
                        QueuedMessage.forOffset(
                                splitId, commitOffset, MilvusCdcMessageKind.CONTROL, "TXN_COMMIT"));
            }
            return;
        }
        messages.put(
                QueuedMessage.forTxnRecords(
                        splitId, records, commitOffset, txnBuffer.messageTypeSummary()));
    }

    private void markRecordReceiveMetadata(MilvusCdcDmlMessage message, long receiveTimestampMs) {
        for (MilvusCdcRecord record : message.getRecords()) {
            Long messageEventTimestamp = MilvusCdcRowMetadata.timetick(record.getRow());
            MilvusCdcRowMetadata.setEventMetadata(
                    record.getRow(), messageEventTimestamp, receiveTimestampMs);
        }
    }

    private void markReceivedSourceMessage(
            String splitId, Long messageEventTimestamp, long receiveTimestampMs) {
        if (messageEventTimestamp == null) {
            return;
        }
        sourceMetrics.markReceivedMessage(
                splitId,
                MilvusCdcRowMetadata.toPhysicalMillis(messageEventTimestamp),
                receiveTimestampMs);
    }

    private void logProcessedMessage(QueuedMessage queuedMessage) {
        MilvusCdcOffset offset = queuedMessage.offset;
        SeaTunnelRow row =
                queuedMessage.records.isEmpty() ? null : queuedMessage.records.get(0).getRow();
        log.debug(
                "Milvus CDC source processed WAL message: split={}, table={}, kind={}, type={}, recordCount={}, currentMessageId={}, resumeMessageId={}, wal={}, timetick={}",
                queuedMessage.splitId,
                row == null ? null : row.getTableId(),
                queuedMessage.kind,
                queuedMessage.messageType,
                queuedMessage.records.size(),
                offset == null ? null : offset.getConsumedMessageId(),
                offset == null ? null : offset.getResumeMessageId(),
                offset == null ? null : offset.getWalName(),
                offset == null ? null : offset.getTimetick());
    }

    private DumpMessagesRequest buildRequest(MilvusCdcSplit split) {
        DumpMessagesRequest.Builder builder =
                DumpMessagesRequest.newBuilder().setPchannel(split.getPchannel());
        MilvusCdcOffset startOffset = split.effectiveStartOffset();
        if (startOffset != null && startOffset.hasResumeMessageId()) {
            builder.setStartMessageId(startOffset.toResumeMessageId());
        }
        if (startOffset != null && startOffset.hasTimetick()) {
            builder.setStartTimetick(startOffset.getTimetick());
        }
        if (requiresStartCoordination(split)) {
            builder.setIncludeStartMessage(true);
        }
        return builder.build();
    }

    private void validateStatus(DumpMessagesResponse response) {
        if (!response.hasStatus()) {
            return;
        }
        Status status = response.getStatus();
        if (status.getCode() != 0 || status.getErrorCode() != ErrorCode.Success) {
            throw new IllegalStateException(
                    "Milvus DumpMessages failed: "
                            + status.getErrorCode()
                            + ", "
                            + status.getReason());
        }
    }

    private boolean requiresStartCoordination(MilvusCdcSplit split) {
        MilvusCdcOffset offset = split.effectiveStartOffset();
        return offset != null
                && offset.hasResumeMessageId()
                && offset.hasConsumedMessageId()
                && !offset.getResumeMessageId().equals(offset.getConsumedMessageId());
    }

    private static class QueuedMessage {
        private final String splitId;
        private final List<MilvusCdcRecord> records;
        private final MilvusCdcOffset offset;
        private final MilvusCdcMessageKind kind;
        private final String messageType;

        private QueuedMessage(
                String splitId,
                List<MilvusCdcRecord> records,
                MilvusCdcOffset offset,
                MilvusCdcMessageKind kind,
                String messageType) {
            this.splitId = splitId;
            this.records = records == null ? Collections.emptyList() : records;
            this.offset = offset;
            this.kind = kind;
            this.messageType = messageType;
        }

        private static QueuedMessage forDmlRecords(
                String splitId,
                List<MilvusCdcRecord> records,
                MilvusCdcOffset outputOffset,
                String messageType) {
            return new QueuedMessage(
                    splitId, records, outputOffset, MilvusCdcMessageKind.DML, messageType);
        }

        private static QueuedMessage forOffset(String splitId, MilvusCdcMessage message) {
            return forOffset(splitId, message.getOffset(), message.getKind(), null);
        }

        private static QueuedMessage forOffset(
                String splitId,
                MilvusCdcOffset offset,
                MilvusCdcMessageKind kind,
                String messageType) {
            return new QueuedMessage(splitId, Collections.emptyList(), offset, kind, messageType);
        }

        private static QueuedMessage forTxnRecords(
                String splitId,
                List<MilvusCdcRecord> records,
                MilvusCdcOffset commitOffset,
                String messageType) {
            return new QueuedMessage(
                    splitId, records, commitOffset, MilvusCdcMessageKind.DML, messageType);
        }
    }

    private static class MilvusCdcTxnBuffer {
        private final MilvusCdcOffset beginOffset;
        private final List<MilvusCdcRecord> deleteRecords = new ArrayList<>();
        private final List<MilvusCdcRecord> insertRecords = new ArrayList<>();
        private int checkpointOnlyMessageCount;

        private MilvusCdcTxnBuffer(MilvusCdcOffset beginOffset) {
            this.beginOffset = beginOffset;
        }

        private void add(MilvusCdcDmlMessage message) {
            switch (message.getMessageType()) {
                case DELETE:
                    deleteRecords.addAll(message.getRecords());
                    return;
                case INSERT:
                    insertRecords.addAll(message.getRecords());
                    return;
                default:
                    throw new IllegalStateException(
                            "Unsupported Milvus CDC transaction DML type: "
                                    + message.getMessageType());
            }
        }

        private void addCheckpointOnlyMessage() {
            checkpointOnlyMessageCount++;
        }

        private List<MilvusCdcRecord> orderedRecords() {
            List<MilvusCdcRecord> records =
                    new ArrayList<>(deleteRecords.size() + insertRecords.size());
            records.addAll(deleteRecords);
            records.addAll(insertRecords);
            return records;
        }

        private String messageTypeSummary() {
            return String.format(
                    "TXN(delete=%d,insert=%d,ignored=%d,begin=%s)",
                    deleteRecords.size(),
                    insertRecords.size(),
                    checkpointOnlyMessageCount,
                    beginMessageId());
        }

        private String beginMessageId() {
            return beginOffset == null ? null : beginOffset.getConsumedMessageId();
        }
    }

    private static class MilvusCdcSourceMetrics {
        private static final String SOURCE_RECEIVE_DELAY_MS = "MilvusCdcSourceReceiveDelayMs";
        private static final String SOURCE_RECEIVE_TO_COMMIT_DELAY_MS =
                "MilvusCdcSourceReceiveToCommitDelayMs";
        private static final String SOURCE_LAST_COMMIT_TS_ALL_SPLITS_MIN_MS =
                "MilvusCdcSourceLastCommitTsAllSplitsMinMs";
        private static final String SOURCE_LAST_COMMIT_TS_ALL_SPLITS_MAX_MS =
                "MilvusCdcSourceLastCommitTsAllSplitsMaxMs";

        private final Counter sourceReceiveDelayMs;
        private final Counter sourceReceiveToCommitDelayMs;
        private final Counter sourceLastCommitTsAllSplitsMinMs;
        private final Counter sourceLastCommitTsAllSplitsMaxMs;
        private final Map<String, Long> sourceReceiveDelayMsBySplit = new ConcurrentHashMap<>();
        private final Map<String, Long> lastReceivedTimestampMsBySplit = new ConcurrentHashMap<>();
        private final Map<String, Long> lastCommitTimestampMsBySplit = new ConcurrentHashMap<>();

        private MilvusCdcSourceMetrics(MetricsContext metricsContext) {
            this.sourceReceiveDelayMs = counter(metricsContext, SOURCE_RECEIVE_DELAY_MS);
            this.sourceReceiveToCommitDelayMs =
                    counter(metricsContext, SOURCE_RECEIVE_TO_COMMIT_DELAY_MS);
            this.sourceLastCommitTsAllSplitsMinMs =
                    counter(metricsContext, SOURCE_LAST_COMMIT_TS_ALL_SPLITS_MIN_MS);
            this.sourceLastCommitTsAllSplitsMaxMs =
                    counter(metricsContext, SOURCE_LAST_COMMIT_TS_ALL_SPLITS_MAX_MS);
        }

        private void initializeCommittedOffset(String splitId, MilvusCdcOffset offset) {
            if (offset == null || !offset.hasTimetick()) {
                return;
            }
            lastCommitTimestampMsBySplit.put(
                    splitId, MilvusCdcRowMetadata.toPhysicalMillis(offset.getTimetick()));
        }

        private void markReceivedMessage(
                String splitId, Long eventTimestampMs, long receiveTimestampMs) {
            if (eventTimestampMs == null) {
                return;
            }
            Long previousTimestampMs = lastReceivedTimestampMsBySplit.get(splitId);
            if (previousTimestampMs != null && eventTimestampMs < previousTimestampMs) {
                return;
            }
            lastReceivedTimestampMsBySplit.put(splitId, eventTimestampMs);
            sourceReceiveDelayMsBySplit.put(splitId, receiveTimestampMs - eventTimestampMs);
            refreshReceiveDelayMetric();
            refreshReceiveToCommitDelayMetric();
        }

        private void markCompletedCheckpoint(
                Map<String, MilvusCdcOffset> checkpointOffsetsBySplit) {
            if (checkpointOffsetsBySplit == null || checkpointOffsetsBySplit.isEmpty()) {
                return;
            }
            for (Map.Entry<String, MilvusCdcOffset> entry : checkpointOffsetsBySplit.entrySet()) {
                MilvusCdcOffset offset = entry.getValue();
                if (offset == null || !offset.hasTimetick()) {
                    continue;
                }
                String splitId = entry.getKey();
                long timestampMs = MilvusCdcRowMetadata.toPhysicalMillis(offset.getTimetick());
                lastCommitTimestampMsBySplit.put(splitId, timestampMs);
            }
            refreshCommitTimestampMetrics();
            refreshReceiveToCommitDelayMetric();
        }

        private void refreshReceiveDelayMetric() {
            Long maxSourceReceiveDelayMs = max(sourceReceiveDelayMsBySplit);
            if (maxSourceReceiveDelayMs != null) {
                set(sourceReceiveDelayMs, maxSourceReceiveDelayMs);
            }
        }

        private void refreshCommitTimestampMetrics() {
            Long minCommitTimestampMs = min(lastCommitTimestampMsBySplit);
            if (minCommitTimestampMs != null) {
                set(sourceLastCommitTsAllSplitsMinMs, minCommitTimestampMs);
            }
            Long maxCommitTimestampMs = max(lastCommitTimestampMsBySplit);
            if (maxCommitTimestampMs != null) {
                set(sourceLastCommitTsAllSplitsMaxMs, maxCommitTimestampMs);
            }
        }

        private void refreshReceiveToCommitDelayMetric() {
            Long maxReceiveToCommitDelayMs = null;
            for (Map.Entry<String, Long> entry : lastCommitTimestampMsBySplit.entrySet()) {
                Long receiveTimestampMs = lastReceivedTimestampMsBySplit.get(entry.getKey());
                Long commitTimestampMs = entry.getValue();
                if (receiveTimestampMs == null || commitTimestampMs == null) {
                    continue;
                }
                long delayMs = receiveTimestampMs - commitTimestampMs;
                if (maxReceiveToCommitDelayMs == null || delayMs > maxReceiveToCommitDelayMs) {
                    maxReceiveToCommitDelayMs = delayMs;
                }
            }
            if (maxReceiveToCommitDelayMs != null) {
                set(sourceReceiveToCommitDelayMs, maxReceiveToCommitDelayMs);
            }
        }

        private static Counter counter(MetricsContext metricsContext, String name) {
            return metricsContext == null ? null : metricsContext.counter(name);
        }

        private static void set(Counter counter, long value) {
            if (counter != null) {
                counter.set(value);
            }
        }

        private static Long min(Map<String, Long> values) {
            Long min = null;
            for (Long value : values.values()) {
                if (value == null) {
                    continue;
                }
                if (min == null || value < min) {
                    min = value;
                }
            }
            return min;
        }

        private static Long max(Map<String, Long> values) {
            Long max = null;
            for (Long value : values.values()) {
                if (value == null) {
                    continue;
                }
                if (max == null || value > max) {
                    max = value;
                }
            }
            return max;
        }
    }
}
