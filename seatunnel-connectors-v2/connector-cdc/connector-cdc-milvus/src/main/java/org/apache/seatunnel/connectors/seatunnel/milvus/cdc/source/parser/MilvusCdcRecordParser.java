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

package org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.parser;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.config.MilvusCdcMessageType;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.offset.MilvusCdcOffset;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.schema.MilvusCdcCollectionSchema;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.schema.MilvusCdcCollectionSchemaRegistry;

import io.milvus.grpc.DumpMessagesResponse;
import io.milvus.grpc.ImmutableMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MilvusCdcRecordParser {

    private static final String MILVUS_MESSAGE_TYPE_KEY = "_t";
    private static final String MILVUS_TRANSACTION_CONTEXT_KEY = "_tx";
    private static final String MILVUS_TIMETICK_KEY = "_tt";
    private static final String MILVUS_RESUME_MESSAGE_ID_KEY = "_lc";
    private static final String MILVUS_RESUME_SAME_AS_CONSUMED_MESSAGE_ID_KEY = "_lcs";
    private static final int MILVUS_INSERT_MESSAGE_TYPE = 2;
    private static final int MILVUS_DELETE_MESSAGE_TYPE = 3;
    private static final int MILVUS_BEGIN_TXN_MESSAGE_TYPE = 900;
    private static final int MILVUS_COMMIT_TXN_MESSAGE_TYPE = 901;
    private static final int MILVUS_ROLLBACK_TXN_MESSAGE_TYPE = 902;
    private static final int MILVUS_TXN_MESSAGE_TYPE = 999;
    private static final int MILVUS_BASE36_RADIX = 36;

    private final Set<MilvusCdcMessageType> messageTypes;
    private final MilvusCdcCollectionSchemaRegistry schemaRegistry;
    private final MilvusCdcPayloadDecoder payloadDecoder;
    private final MilvusCdcRowConverter rowConverter;

    public MilvusCdcRecordParser(MilvusCdcCollectionSchemaRegistry schemaRegistry) {
        this(EnumSet.allOf(MilvusCdcMessageType.class), schemaRegistry);
    }

    public MilvusCdcRecordParser(
            Set<MilvusCdcMessageType> messageTypes,
            MilvusCdcCollectionSchemaRegistry schemaRegistry) {
        if (messageTypes == null || messageTypes.isEmpty()) {
            throw new IllegalArgumentException("Milvus CDC message types must not be empty.");
        }
        if (schemaRegistry == null) {
            throw new IllegalArgumentException("Milvus CDC schema registry must not be null.");
        }
        this.messageTypes = Collections.unmodifiableSet(EnumSet.copyOf(messageTypes));
        this.schemaRegistry = schemaRegistry;
        this.payloadDecoder = new MilvusCdcPayloadDecoder();
        this.rowConverter = new MilvusCdcRowConverter();
    }

    public MilvusCdcMessage parse(DumpMessagesResponse response) {
        if (!response.hasMessage()) {
            return new MilvusCdcIgnoredMessage(MilvusCdcMessageKind.EMPTY, null);
        }
        ImmutableMessage message = response.getMessage();
        Map<String, String> properties = new HashMap<>(message.getPropertiesMap());
        int milvusMessageType = requiredMilvusMessageType(properties);
        MilvusCdcOffset offset = parseOffset(message, properties).orElse(null);
        switch (milvusMessageType) {
            case MILVUS_INSERT_MESSAGE_TYPE:
                return parseDmlMessage(message, properties, offset, MilvusCdcMessageType.INSERT);
            case MILVUS_DELETE_MESSAGE_TYPE:
                return parseDmlMessage(message, properties, offset, MilvusCdcMessageType.DELETE);
            case MILVUS_BEGIN_TXN_MESSAGE_TYPE:
                return new MilvusCdcControlMessage(MilvusCdcControlMessageType.BEGIN_TXN, offset);
            case MILVUS_COMMIT_TXN_MESSAGE_TYPE:
                return new MilvusCdcControlMessage(MilvusCdcControlMessageType.COMMIT_TXN, offset);
            case MILVUS_ROLLBACK_TXN_MESSAGE_TYPE:
                return new MilvusCdcControlMessage(
                        MilvusCdcControlMessageType.ROLLBACK_TXN, offset);
            case MILVUS_TXN_MESSAGE_TYPE:
                return new MilvusCdcControlMessage(MilvusCdcControlMessageType.TXN, offset);
            default:
                return new MilvusCdcIgnoredMessage(MilvusCdcMessageKind.UNKNOWN, offset);
        }
    }

    private MilvusCdcMessage parseDmlMessage(
            ImmutableMessage message,
            Map<String, String> properties,
            MilvusCdcOffset offset,
            MilvusCdcMessageType messageType) {
        if (!messageTypes.contains(messageType)) {
            return new MilvusCdcIgnoredMessage(MilvusCdcMessageKind.FILTERED, offset);
        }

        List<MilvusCdcDecodedRecord> decodedRecords =
                payloadDecoder.decode(messageType, message.getPayload());

        List<MilvusCdcRecord> records = new ArrayList<>(decodedRecords.size());
        for (MilvusCdcDecodedRecord decodedRecord : decodedRecords) {
            Optional<MilvusCdcCollectionSchema> schema =
                    schemaRegistry.schemaForSourceCollection(
                            decodedRecord.getDatabase(), decodedRecord.getCollection());
            if (!schema.isPresent()) {
                continue;
            }
            SeaTunnelRow row = rowConverter.convert(decodedRecord, schema.get());
            MilvusCdcRowMetadata.setMessageMetadata(row, offset);
            records.add(new MilvusCdcRecord(row));
        }
        if (!records.isEmpty()) {
            MilvusCdcRecord lastRecord = records.get(records.size() - 1);
            MilvusCdcRowMetadata.markMessageEnd(lastRecord.getRow());
        }
        if (records.isEmpty()) {
            return new MilvusCdcIgnoredMessage(MilvusCdcMessageKind.FILTERED, offset);
        }
        return new MilvusCdcDmlMessage(
                offset,
                messageType,
                records,
                properties.containsKey(MILVUS_TRANSACTION_CONTEXT_KEY));
    }

    List<MilvusCdcRecord> parseRecords(DumpMessagesResponse response) {
        MilvusCdcMessage message = parse(response);
        if (message instanceof MilvusCdcDmlMessage) {
            return ((MilvusCdcDmlMessage) message).getRecords();
        }
        return Collections.emptyList();
    }

    public Optional<MilvusCdcOffset> parseOffset(DumpMessagesResponse response) {
        if (!response.hasMessage()) {
            return Optional.empty();
        }
        ImmutableMessage message = response.getMessage();
        Map<String, String> properties = new HashMap<>(message.getPropertiesMap());
        requiredMilvusMessageType(properties);
        return parseOffset(message, properties);
    }

    private int requiredMilvusMessageType(Map<String, String> properties) {
        String milvusMessageTypeValue = properties.get(MILVUS_MESSAGE_TYPE_KEY);
        if (milvusMessageTypeValue == null || milvusMessageTypeValue.isEmpty()) {
            throw new IllegalArgumentException(
                    "Milvus CDC message property '" + MILVUS_MESSAGE_TYPE_KEY + "' is required.");
        }
        return Integer.parseInt(milvusMessageTypeValue);
    }

    private Optional<MilvusCdcOffset> parseOffset(
            ImmutableMessage message, Map<String, String> properties) {
        if (!message.hasId()) {
            throw new IllegalArgumentException("Milvus CDC message id is required.");
        }
        MilvusCdcOffset offset = MilvusCdcOffset.fromMessageId(message.getId());
        offset.setResumeMessageId(parseResumeMessageId(message, properties));
        offset.setTimetick(parseTimetick(properties));
        return Optional.of(offset);
    }

    private String parseResumeMessageId(ImmutableMessage message, Map<String, String> properties) {
        if (properties.containsKey(MILVUS_RESUME_SAME_AS_CONSUMED_MESSAGE_ID_KEY)) {
            return message.getId().getId();
        }
        String value = properties.get(MILVUS_RESUME_MESSAGE_ID_KEY);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        throw new IllegalArgumentException(
                "Milvus CDC message property '"
                        + MILVUS_RESUME_MESSAGE_ID_KEY
                        + "' or '"
                        + MILVUS_RESUME_SAME_AS_CONSUMED_MESSAGE_ID_KEY
                        + "' is required.");
    }

    private Long parseTimetick(Map<String, String> properties) {
        String value = properties.get("timetick");
        if (value == null) {
            value = properties.get("time_tick");
        }
        if (value == null) {
            value = properties.get(MILVUS_TIMETICK_KEY);
            if (value != null && !value.isEmpty()) {
                return Long.parseUnsignedLong(value, MILVUS_BASE36_RADIX);
            }
        }
        if (value == null || value.isEmpty()) {
            return null;
        }
        return Long.parseLong(value);
    }
}
