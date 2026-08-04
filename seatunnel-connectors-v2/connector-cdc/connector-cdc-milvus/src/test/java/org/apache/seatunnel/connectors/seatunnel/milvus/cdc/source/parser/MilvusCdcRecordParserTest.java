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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.GeometryType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.config.MilvusCdcMessageType;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.schema.MilvusCdcCollectionSchema;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.schema.MilvusCdcCollectionSchemaRegistry;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.milvus.grpc.ArrayArray;
import io.milvus.grpc.Blob;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DumpMessagesResponse;
import io.milvus.grpc.FieldData;
import io.milvus.grpc.FloatArray;
import io.milvus.grpc.GeometryArray;
import io.milvus.grpc.IDs;
import io.milvus.grpc.ImmutableMessage;
import io.milvus.grpc.JSONArray;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.MessageID;
import io.milvus.grpc.ScalarField;
import io.milvus.grpc.SparseFloatArray;
import io.milvus.grpc.StringArray;
import io.milvus.grpc.StructArrayField;
import io.milvus.grpc.TimestamptzArray;
import io.milvus.grpc.VectorArray;
import io.milvus.grpc.VectorField;
import io.milvus.grpc.WALName;
import io.milvus.param.ParamUtils;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import milvus.proto.msg.Msg;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.seatunnel.connectors.seatunnel.milvus.cdc.common.MilvusCdcConstants.MILVUS_INTERNAL_DYNAMIC_FIELD;

class MilvusCdcRecordParserTest {

    @Test
    void parseInsertDumpMessage() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-1")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(insertPayload())
                                        .putProperties("timetick", "99")
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", ""))
                        .build();

        List<MilvusCdcRecord> records = parser("collection_a").parseRecords(response);

        Assertions.assertEquals(1, records.size());
        MilvusCdcRecord record = records.get(0);

        SeaTunnelRow row = record.getRow();
        Assertions.assertEquals(RowKind.INSERT, row.getRowKind());
        Assertions.assertEquals(1001L, row.getField(0));
        Assertions.assertEquals("alice", row.getField(1));
        Assertions.assertEquals("default.collection_a", row.getTableId());
        Assertions.assertEquals("partition_a", row.getPartitionName());
        Assertions.assertEquals(
                "message-1", row.getOptions().get(MilvusCdcRowMetadata.CURRENT_MESSAGE_ID));
        Assertions.assertEquals("Pulsar", row.getOptions().get(MilvusCdcRowMetadata.WAL_NAME));
        Assertions.assertEquals(99L, row.getOptions().get(MilvusCdcRowMetadata.TIMETICK));
    }

    @Test
    void rejectUnexpectedStaticFieldAddedAfterSchemaDiscovery() throws Exception {
        Msg.InsertRequest request = Msg.InsertRequest.parseFrom(insertPayload());
        FieldData addedStaticField =
                FieldData.newBuilder()
                        .setFieldName("new_static_field")
                        .setType(DataType.VarChar)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setStringData(
                                                StringArray.newBuilder().addData("new-value")))
                        .build();
        DumpMessagesResponse response =
                response(
                        "message-schema-drift",
                        request.toBuilder().addFieldsData(addedStaticField).build().toByteString(),
                        "100");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> parser("collection_a").parseRecords(response));

        Assertions.assertTrue(exception.getMessage().contains("schema changed"));
        Assertions.assertTrue(exception.getMessage().contains("new_static_field"));
        Assertions.assertTrue(exception.getMessage().contains("not supported yet"));
    }

    @Test
    void convertTreatsMetadataNameAsRegularFieldWhenNotMarked() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                new ArrayList<>(
                                        java.util.Arrays.asList(
                                                PhysicalColumn.builder()
                                                        .name("id")
                                                        .dataType(BasicType.LONG_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name(CommonOptions.METADATA.getName())
                                                        .dataType(BasicType.STRING_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name(MILVUS_INTERNAL_DYNAMIC_FIELD)
                                                        .dataType(BasicType.STRING_TYPE)
                                                        .options(metadataOptions())
                                                        .build())))
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id"), false))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("milvus", "default", null, "collection_a"),
                        tableSchema,
                        Collections.emptyMap(),
                        new ArrayList<>(),
                        "");
        MilvusCdcCollectionSchema schema =
                MilvusCdcCollectionSchema.builder()
                        .sourceDatabase("default")
                        .sourceCollection("collection_a")
                        .catalogTable(catalogTable)
                        .rowType(catalogTable.getSeaTunnelRowType())
                        .tableId(catalogTable.getTablePath().toString())
                        .primaryKeyField("id")
                        .primaryKeyIndex(0)
                        .enableDynamicField(true)
                        .build();
        Map<String, Object> data = new HashMap<>();
        data.put("id", 1001L);
        data.put(CommonOptions.METADATA.getName(), "static-metadata");
        data.put(MILVUS_INTERNAL_DYNAMIC_FIELD, "{\"dynamic\":\"value\"}");
        MilvusCdcDecodedRecord record =
                MilvusCdcDecodedRecord.builder()
                        .messageType(MilvusCdcMessageType.INSERT)
                        .database("default")
                        .collection("collection_a")
                        .partition("partition_a")
                        .eventTimestamp(500L)
                        .data(data)
                        .build();

        SeaTunnelRow row = new MilvusCdcRowConverter().convert(record, schema);

        Assertions.assertEquals(1001L, row.getField(0));
        Assertions.assertEquals("static-metadata", row.getField(1));
        Assertions.assertEquals("{\"dynamic\":\"value\"}", row.getField(2));
    }

    @Test
    void rejectDynamicPayloadWithoutMetadataColumn() {
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                new ArrayList<>(
                                        java.util.Arrays.asList(
                                                PhysicalColumn.builder()
                                                        .name("id")
                                                        .dataType(BasicType.LONG_TYPE)
                                                        .build())))
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id"), false))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("milvus", "default", null, "collection_a"),
                        tableSchema,
                        Collections.emptyMap(),
                        new ArrayList<>(),
                        "");
        MilvusCdcCollectionSchema schema =
                MilvusCdcCollectionSchema.builder()
                        .sourceDatabase("default")
                        .sourceCollection("collection_a")
                        .catalogTable(catalogTable)
                        .rowType(catalogTable.getSeaTunnelRowType())
                        .tableId(catalogTable.getTablePath().toString())
                        .primaryKeyField("id")
                        .primaryKeyIndex(0)
                        .enableDynamicField(true)
                        .build();
        Map<String, Object> data = new HashMap<>();
        data.put("id", 1001L);
        data.put(MILVUS_INTERNAL_DYNAMIC_FIELD, "{\"dynamic\":\"value\"}");
        MilvusCdcDecodedRecord record =
                MilvusCdcDecodedRecord.builder()
                        .messageType(MilvusCdcMessageType.INSERT)
                        .database("default")
                        .collection("collection_a")
                        .data(data)
                        .build();

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new MilvusCdcRowConverter().convert(record, schema));

        Assertions.assertTrue(exception.getMessage().contains(MILVUS_INTERNAL_DYNAMIC_FIELD));
        Assertions.assertTrue(exception.getMessage().contains("metadata column"));
    }

    @Test
    void parseDoesNotAttachEventTimeOrReceiveMetadata() {
        long rowTimestamp = hybridTimestamp(1_700_000_000_000L);
        long messageTimestamp = hybridTimestamp(1_700_000_001_000L);
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-metadata")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(insertPayload(rowTimestamp))
                                        .putProperties("timetick", Long.toString(messageTimestamp))
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", ""))
                        .build();

        List<MilvusCdcRecord> records = parser("collection_a").parseRecords(response);

        SeaTunnelRow row = records.get(0).getRow();
        Assertions.assertFalse(row.getOptions().containsKey(MilvusCdcRowMetadata.EVENT_TIMESTAMP));
        Assertions.assertFalse(
                row.getOptions().containsKey(MilvusCdcRowMetadata.EVENT_TIMESTAMP_MS));
        Assertions.assertFalse(row.getOptions().containsKey("MilvusCdcSourceReceiveTimestampMs"));
        Assertions.assertEquals(
                messageTimestamp, row.getOptions().get(MilvusCdcRowMetadata.TIMETICK));
    }

    @Test
    void eventMetadataKeepsNegativeDelay() {
        SeaTunnelRow row = new SeaTunnelRow(0);

        MilvusCdcRowMetadata.setEventMetadata(row, hybridTimestamp(1_000L), 900L);

        Assertions.assertEquals(-100L, row.getOptions().get(CommonOptions.DELAY.getName()));
    }

    @Test
    void parseNativeMilvusMessageTypeAndTimetickProperties() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("3jmdu6539vr7")
                                                        .setWALName(WALName.RocksMQ))
                                        .setPayload(insertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("_tt", Long.toString(99L, 36)))
                        .build();

        List<MilvusCdcRecord> records = parser("collection_a").parseRecords(response);

        Assertions.assertEquals(1, records.size());
        SeaTunnelRow row = records.get(0).getRow();
        Assertions.assertEquals(
                "3jmdu6539vr7", row.getOptions().get(MilvusCdcRowMetadata.CURRENT_MESSAGE_ID));
        Assertions.assertEquals(
                "3jmdu6539vr7", row.getOptions().get(MilvusCdcRowMetadata.RESUME_MESSAGE_ID));
        Assertions.assertEquals("RocksMQ", row.getOptions().get(MilvusCdcRowMetadata.WAL_NAME));
        Assertions.assertEquals(99L, row.getOptions().get(MilvusCdcRowMetadata.TIMETICK));
        Assertions.assertEquals(RowKind.INSERT, records.get(0).getRow().getRowKind());
        Assertions.assertEquals(1001L, records.get(0).getRow().getField(0));
    }

    @Test
    void missingMilvusMessageTypeFailsFast() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-without-type")
                                                        .setWALName(WALName.RocksMQ))
                                        .setPayload(insertPayload())
                                        .putProperties("_tt", Long.toString(99L, 36)))
                        .build();

        MilvusCdcRecordParser parser = parser("collection_a");
        IllegalArgumentException parseException =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> parser.parse(response));
        IllegalArgumentException offsetException =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> parser.parseOffset(response));

        Assertions.assertTrue(parseException.getMessage().contains("_t"));
        Assertions.assertTrue(offsetException.getMessage().contains("_t"));
    }

    @Test
    void missingMilvusLastConfirmedMessageIdFailsFast() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-without-last-confirmed")
                                                        .setWALName(WALName.RocksMQ))
                                        .setPayload(insertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_tt", Long.toString(99L, 36)))
                        .build();

        MilvusCdcRecordParser parser = parser("collection_a");
        IllegalArgumentException parseException =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> parser.parse(response));
        IllegalArgumentException offsetException =
                Assertions.assertThrows(
                        IllegalArgumentException.class, () -> parser.parseOffset(response));

        Assertions.assertTrue(parseException.getMessage().contains("_lc"));
        Assertions.assertTrue(parseException.getMessage().contains("_lcs"));
        Assertions.assertTrue(offsetException.getMessage().contains("_lc"));
        Assertions.assertTrue(offsetException.getMessage().contains("_lcs"));
    }

    @Test
    void parseMilvusControlMessageAsOffsetOnly() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-commit")
                                                        .setWALName(WALName.RocksMQ))
                                        .setPayload(ByteString.EMPTY)
                                        .putProperties("_t", "901")
                                        .putProperties("_tt", Long.toString(303L, 36))
                                        .putProperties("_lc", "resume-commit"))
                        .build();

        MilvusCdcRecordParser parser = parser("collection_a");
        MilvusCdcMessage message = parser.parse(response);
        List<MilvusCdcRecord> records = parser.parseRecords(response);

        Assertions.assertEquals(MilvusCdcMessageKind.CONTROL, message.getKind());
        Assertions.assertInstanceOf(MilvusCdcControlMessage.class, message);
        Assertions.assertEquals(
                MilvusCdcControlMessageType.COMMIT_TXN,
                ((MilvusCdcControlMessage) message).getControlType());
        Assertions.assertTrue(message.shouldCheckpoint());
        Assertions.assertEquals("message-commit", message.getOffset().getConsumedMessageId());
        Assertions.assertTrue(records.isEmpty());
        Assertions.assertEquals(
                "message-commit", parser.parseOffset(response).get().getConsumedMessageId());
        Assertions.assertEquals(
                "resume-commit", parser.parseOffset(response).get().getResumeMessageId());
        Assertions.assertEquals(303L, parser.parseOffset(response).get().getTimetick());
    }

    @Test
    void parseUnknownMilvusMessageAsCheckpointableIgnoredMessage() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-unknown")
                                                        .setWALName(WALName.RocksMQ))
                                        .setPayload(ByteString.EMPTY)
                                        .putProperties("_t", "12345")
                                        .putProperties("_lcs", "")
                                        .putProperties("_tt", Long.toString(304L, 36)))
                        .build();

        MilvusCdcRecordParser parser = parser("collection_a");
        MilvusCdcMessage message = parser.parse(response);

        Assertions.assertEquals(MilvusCdcMessageKind.UNKNOWN, message.getKind());
        Assertions.assertTrue(message.shouldCheckpoint());
        Assertions.assertEquals("message-unknown", message.getOffset().getConsumedMessageId());
        Assertions.assertTrue(parser.parseRecords(response).isEmpty());
    }

    @Test
    void keepSourceCollectionTableId() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-mapped")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(insertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "99"))
                        .build();
        MilvusCdcRecordParser parser =
                new MilvusCdcRecordParser(
                        EnumSet.allOf(MilvusCdcMessageType.class), schemaRegistry("collection_a"));

        List<MilvusCdcRecord> records = parser.parseRecords(response);

        Assertions.assertEquals(1, records.size());
        SeaTunnelRow row = records.get(0).getRow();
        Assertions.assertEquals("default.collection_a", row.getTableId());
    }

    @Test
    void filterDatabaseMismatchForSameCollectionName() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-wrong-db")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(insertPayload("other_db", "collection_a", 99L))
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "99"))
                        .build();

        MilvusCdcRecordParser parser = parser("collection_a");
        MilvusCdcMessage message = parser.parse(response);

        Assertions.assertEquals(MilvusCdcMessageKind.FILTERED, message.getKind());
        Assertions.assertTrue(message.shouldCheckpoint());
        Assertions.assertTrue(parser.parseRecords(response).isEmpty());
        Assertions.assertEquals(
                "message-wrong-db", parser.parseOffset(response).get().getConsumedMessageId());
    }

    @Test
    void rejectPayloadWithoutDatabase() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-missing-db")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(insertPayload(null, "collection_a", 99L))
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "99"))
                        .build();

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> parser("collection_a").parseRecords(response));

        Assertions.assertTrue(exception.getMessage().contains("db_name"));
    }

    @Test
    void rejectPayloadWithoutCollection() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-missing-collection")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(insertPayload("default", null, 99L))
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "99"))
                        .build();

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> parser("collection_a").parseRecords(response));

        Assertions.assertTrue(exception.getMessage().contains("collection_name"));
    }

    @Test
    void ignoreStatusOnlyResponse() {
        List<MilvusCdcRecord> records =
                parser("collection_a").parseRecords(DumpMessagesResponse.newBuilder().build());

        Assertions.assertTrue(records.isEmpty());
    }

    @Test
    void filterUnconfiguredMessageTypeButKeepOffset() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-2")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(deletePayload())
                                        .putProperties("_t", "3")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "100"))
                        .build();
        MilvusCdcRecordParser parser =
                new MilvusCdcRecordParser(
                        Collections.singleton(MilvusCdcMessageType.INSERT),
                        schemaRegistry("collection_a"));
        MilvusCdcMessage message = parser.parse(response);

        Assertions.assertEquals(MilvusCdcMessageKind.FILTERED, message.getKind());
        Assertions.assertTrue(message.shouldCheckpoint());
        Assertions.assertTrue(parser.parseRecords(response).isEmpty());
        Assertions.assertEquals(
                "message-2", parser.parseOffset(response).get().getConsumedMessageId());
        Assertions.assertEquals(100L, parser.parseOffset(response).get().getTimetick());
    }

    @Test
    void parseDeleteDumpMessage() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-3")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(deletePayload())
                                        .putProperties("_t", "3")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "101"))
                        .build();

        List<MilvusCdcRecord> records = parser("collection_a").parseRecords(response);

        Assertions.assertEquals(1, records.size());
        SeaTunnelRow row = records.get(0).getRow();
        Assertions.assertEquals(RowKind.DELETE, row.getRowKind());
        Assertions.assertEquals(1001L, row.getField(0));
        Assertions.assertNull(row.getField(1));
        Assertions.assertEquals("default.collection_a", row.getTableId());
    }

    @Test
    void parseMultiRowMessageAndMarkLastRowAsMessageEnd() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-multi")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(multiRowInsertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "200"))
                        .build();

        List<MilvusCdcRecord> records = parser("collection_a").parseRecords(response);

        Assertions.assertEquals(2, records.size());
        Assertions.assertEquals(1001L, records.get(0).getRow().getField(0));
        Assertions.assertEquals(1002L, records.get(1).getRow().getField(0));
        Assertions.assertEquals("bob", records.get(1).getRow().getField(1));
        Assertions.assertEquals(
                "message-multi",
                records.get(0).getRow().getOptions().get(MilvusCdcRowMetadata.CURRENT_MESSAGE_ID));
        Assertions.assertEquals(
                "message-multi",
                records.get(1).getRow().getOptions().get(MilvusCdcRowMetadata.CURRENT_MESSAGE_ID));
        Assertions.assertNull(
                records.get(0).getRow().getOptions().get(MilvusCdcRowMetadata.MESSAGE_END));
        Assertions.assertEquals(
                true, records.get(1).getRow().getOptions().get(MilvusCdcRowMetadata.MESSAGE_END));
    }

    @Test
    void parseFloatVectorAsByteBuffer() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-vector")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(floatVectorInsertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "250"))
                        .build();

        List<MilvusCdcRecord> records =
                new MilvusCdcRecordParser(vectorSchemaRegistry("collection_a"))
                        .parseRecords(response);

        SeaTunnelRow row = records.get(0).getRow();
        Assertions.assertTrue(row.getField(1) instanceof ByteBuffer);
        Assertions.assertArrayEquals(
                new Float[] {0.1F, 0.2F, 0.3F, 0.4F},
                BufferUtils.toFloatArray((ByteBuffer) row.getField(1)));
        Assertions.assertEquals(
                24, row.getBytesSize(vectorCatalogTable("collection_a").getSeaTunnelRowType()));
    }

    @Test
    void parseBinaryVectorByBitDimension() {
        DumpMessagesResponse response =
                response(
                        "message-binary-vector",
                        binaryVectorInsertPayload(16, (byte) 1, (byte) 2, (byte) 3, (byte) 4),
                        "260");

        List<MilvusCdcRecord> records =
                new MilvusCdcRecordParser(binaryVectorSchemaRegistry("collection_a"))
                        .parseRecords(response);

        Assertions.assertEquals(2, records.size());
        Assertions.assertArrayEquals(
                new byte[] {1, 2}, bytes((ByteBuffer) records.get(0).getRow().getField(1)));
        Assertions.assertArrayEquals(
                new byte[] {3, 4}, bytes((ByteBuffer) records.get(1).getRow().getField(1)));
    }

    @Test
    void parseNullableDenseScalarAndCompactVectorByValidData() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-nullable")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(nullableInsertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "251"))
                        .build();

        List<MilvusCdcRecord> records =
                new MilvusCdcRecordParser(nullableSchemaRegistry("collection_a"))
                        .parseRecords(response);

        Assertions.assertEquals(5, records.size());
        Assertions.assertArrayEquals(
                new Float[] {0.11F, 0.12F, 0.13F, 0.14F},
                BufferUtils.toFloatArray((ByteBuffer) records.get(0).getRow().getField(1)));
        Assertions.assertNull(records.get(1).getRow().getField(1));
        Assertions.assertArrayEquals(
                new Float[] {0.31F, 0.32F, 0.33F, 0.34F},
                BufferUtils.toFloatArray((ByteBuffer) records.get(2).getRow().getField(1)));
        Assertions.assertNull(records.get(3).getRow().getField(1));
        Assertions.assertNull(records.get(4).getRow().getField(1));
        Assertions.assertEquals(101L, records.get(0).getRow().getField(2));
        Assertions.assertNull(records.get(1).getRow().getField(2));
        Assertions.assertNull(records.get(2).getRow().getField(2));
        Assertions.assertEquals(404L, records.get(3).getRow().getField(2));
        Assertions.assertNull(records.get(4).getRow().getField(2));
        Assertions.assertEquals("mixed-null", records.get(3).getRow().getField(3));
        Assertions.assertNull(records.get(4).getRow().getField(3));
    }

    @Test
    void parseCompactNullableScalarByValidOrdinal() {
        DumpMessagesResponse response =
                response("message-compact-nullable-scalar", compactNullableScalarPayload(), "252");

        List<MilvusCdcRecord> records =
                new MilvusCdcRecordParser(nullableScalarSchemaRegistry("collection_a"))
                        .parseRecords(response);

        Assertions.assertEquals(5, records.size());
        Assertions.assertEquals(101L, records.get(0).getRow().getField(1));
        Assertions.assertNull(records.get(1).getRow().getField(1));
        Assertions.assertNull(records.get(2).getRow().getField(1));
        Assertions.assertEquals(404L, records.get(3).getRow().getField(1));
        Assertions.assertNull(records.get(4).getRow().getField(1));
    }

    @Test
    void parseMilvusTypedFieldsLikeFullReader() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-typed")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(typedInsertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "500"))
                        .build();

        List<MilvusCdcRecord> records =
                new MilvusCdcRecordParser(typedSchemaRegistry("collection_a"))
                        .parseRecords(response);

        SeaTunnelRow row = records.get(0).getRow();
        Assertions.assertEquals(1001L, row.getField(0));
        Assertions.assertEquals("{\"k\":\"v\"}", row.getField(1));
        Assertions.assertArrayEquals(new String[] {"x", "y"}, (String[]) row.getField(2));
        Assertions.assertArrayEquals(
                new String[] {
                    "{\"name\":\"a\",\"score\":1,\"embedding\":[0.1,0.2,0.3,0.4]}",
                    "{\"name\":\"b\",\"score\":2,\"embedding\":[0.5,0.6,0.7,0.8]}"
                },
                (String[]) row.getField(3));

        @SuppressWarnings("unchecked")
        SortedMap<Long, Float> sparse = (SortedMap<Long, Float>) row.getField(4);
        Assertions.assertEquals(0.25F, sparse.get(7L));
        Assertions.assertEquals(1.5F, sparse.get(42L));

        ByteBuffer int8Vector = ((ByteBuffer) row.getField(5)).duplicate();
        byte[] int8Bytes = new byte[int8Vector.remaining()];
        int8Vector.get(int8Bytes);
        Assertions.assertArrayEquals(new byte[] {1, -2, 3, -4}, int8Bytes);

        ByteBuffer geometry = ((ByteBuffer) row.getField(6)).duplicate();
        byte[] geometryBytes = new byte[geometry.remaining()];
        geometry.get(geometryBytes);
        Assertions.assertArrayEquals(new byte[] {1, 2, 3}, geometryBytes);
        Assertions.assertEquals("2023-11-14T22:13:20Z", row.getField(7));
        Assertions.assertEquals(
                "{\"extra\":\"dynamic-value\",\"key0\":\"value0\",\"dynamicInt\":106}",
                row.getField(8));
    }

    @Test
    void parseMilvusTimestamptzAsUnixMicroseconds() {
        assertParsedTimestamptz(946_684_800_000_000L, "2000-01-01T00:00:00Z");
        assertParsedTimestamptz(-315_619_200_000_000L, "1960-01-01T00:00:00Z");
    }

    @Test
    void parseFlattenedStructArrayFieldsLikeFullReader() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-flat-struct")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(flattenedStructInsertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "501"))
                        .build();

        List<MilvusCdcRecord> records =
                new MilvusCdcRecordParser(scalarStructSchemaRegistry("collection_a"))
                        .parseRecords(response);

        SeaTunnelRow row = records.get(0).getRow();
        Assertions.assertEquals(1001L, row.getField(0));
        Assertions.assertArrayEquals(
                new String[] {"{\"name\":\"a\",\"score\":1}", "{\"name\":\"b\",\"score\":2}"},
                (String[]) row.getField(1));
        Assertions.assertEquals("{\"extra\":\"dynamic-value\"}", row.getField(2));
    }

    @Test
    void parseMultipleFlattenedStructArraysByParentName() {
        DumpMessagesResponse response =
                response("message-multi-struct-fields", multipleFlattenedStructsPayload(), "502");

        List<MilvusCdcRecord> records =
                new MilvusCdcRecordParser(multipleStructSchemaRegistry("collection_a"))
                        .parseRecords(response);

        SeaTunnelRow row = records.get(0).getRow();
        Assertions.assertEquals(1001L, row.getField(0));
        Assertions.assertArrayEquals(
                new String[] {"{\"ca\":\"a1\",\"cb\":10}", "{\"ca\":\"a2\",\"cb\":20}"},
                (String[]) row.getField(1));
        Assertions.assertArrayEquals(
                new String[] {"{\"ca2\":\"b1\",\"cb2\":100}", "{\"ca2\":\"b2\",\"cb2\":200}"},
                (String[]) row.getField(2));
    }

    @Test
    void rejectDuplicateFlattenedStructChildFieldNames() {
        DumpMessagesResponse response =
                response(
                        "message-duplicate-struct-child",
                        duplicateFlattenedStructChildPayload(),
                        "503");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new MilvusCdcRecordParser(
                                                multipleStructSchemaRegistry("collection_a"))
                                        .parseRecords(response));
        Assertions.assertTrue(
                exception.getMessage().contains("Duplicate Milvus CDC insert field name"));
        Assertions.assertTrue(exception.getMessage().contains("pa[ca]"));
    }

    @Test
    void rejectFlattenedStructArrayWithMissingSchemaChildField() {
        DumpMessagesResponse response =
                response(
                        "message-missing-struct-child-field",
                        flattenedStructInsertPayloadMissingChildField(),
                        "504");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new MilvusCdcRecordParser(
                                                scalarStructSchemaRegistry("collection_a"))
                                        .parseRecords(response));

        Assertions.assertTrue(exception.getMessage().contains("missing flattened child fields"));
        Assertions.assertTrue(exception.getMessage().contains("score"));
    }

    @Test
    void rejectFlattenedStructArrayWithUnexpectedSchemaChildField() {
        DumpMessagesResponse response =
                response(
                        "message-unexpected-struct-child-field",
                        flattenedStructInsertPayloadUnexpectedChildField(),
                        "505");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new MilvusCdcRecordParser(
                                                scalarStructSchemaRegistry("collection_a"))
                                        .parseRecords(response));

        Assertions.assertTrue(
                exception.getMessage().contains("Unexpected Milvus CDC struct child field"));
        Assertions.assertTrue(exception.getMessage().contains("unknown"));
    }

    @Test
    void rejectFlattenedStructArrayWithScalarChildValue() {
        DumpMessagesResponse response =
                response(
                        "message-scalar-struct-child-value",
                        flattenedStructInsertPayloadWithScalarChildValue(),
                        "506");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new MilvusCdcRecordParser(
                                                scalarStructSchemaRegistry("collection_a"))
                                        .parseRecords(response));

        Assertions.assertTrue(exception.getMessage().contains("child field name must be a List"));
        Assertions.assertTrue(exception.getMessage().contains("structs"));
    }

    @Test
    void rejectFlattenedStructArrayWithMissingChildValues() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-flat-struct-missing")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(flattenedStructInsertPayloadWithMissingScore())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "502"))
                        .build();

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new MilvusCdcRecordParser(
                                                scalarStructSchemaRegistry("collection_a"))
                                        .parseRecords(response));
        Assertions.assertTrue(exception.getMessage().contains("struct array payload"));
    }

    @Test
    void parseNullableFlattenedStructArrayNullOuterRow() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-flat-struct-null")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(nullableFlattenedStructInsertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "503"))
                        .build();

        List<MilvusCdcRecord> records =
                new MilvusCdcRecordParser(scalarStructSchemaRegistry("collection_a"))
                        .parseRecords(response);

        Assertions.assertEquals(3, records.size());
        Assertions.assertArrayEquals(
                new String[] {"{\"name\":\"a\",\"score\":1}"},
                (String[]) records.get(0).getRow().getField(1));
        Assertions.assertNull(records.get(1).getRow().getField(1));
        Assertions.assertArrayEquals(
                new String[] {"{\"name\":\"c\",\"score\":3}"},
                (String[]) records.get(2).getRow().getField(1));
    }

    @Test
    void rejectNestedStructArrayPayloadBeforeProxyFlattening() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-nested-struct")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(nestedStructInsertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "502"))
                        .build();

        assertRejectsUnflattenedStructArrays(response);
    }

    @Test
    void parseFlattenedStructArrayVectorChildByStructElement() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-flat-struct-vector")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(flattenedStructVectorInsertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "506"))
                        .build();

        List<MilvusCdcRecord> records =
                new MilvusCdcRecordParser(vectorStructSchemaRegistry("collection_a", 4))
                        .parseRecords(response);

        Assertions.assertArrayEquals(
                new String[] {
                    "{\"name\":\"a\",\"embedding\":[1.0,2.0,3.0,4.0]}",
                    "{\"name\":\"b\",\"embedding\":[5.0,6.0,7.0,8.0]}"
                },
                (String[]) records.get(0).getRow().getField(1));
    }

    @Test
    void rejectNestedStructArrayVectorPayloadBeforeProxyFlattening() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-nested-struct-vector")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(nestedStructVectorInsertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "507"))
                        .build();

        assertRejectsUnflattenedStructArrays(response);
    }

    @Test
    void parseMultiRowFlattenedStructArrayVectorChildByOuterRow() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-multi-flat-struct-vector")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(multiRowFlattenedStructVectorInsertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "508"))
                        .build();

        List<MilvusCdcRecord> records =
                new MilvusCdcRecordParser(vectorStructSchemaRegistry("collection_a", 4))
                        .parseRecords(response);

        Assertions.assertEquals(2, records.size());
        Assertions.assertArrayEquals(
                new String[] {
                    "{\"name\":\"a\",\"embedding\":[1.0,2.0,3.0,4.0]}",
                    "{\"name\":\"b\",\"embedding\":[5.0,6.0,7.0,8.0]}"
                },
                (String[]) records.get(0).getRow().getField(1));
        Assertions.assertArrayEquals(
                new String[] {"{\"name\":\"c\",\"embedding\":[9.0,10.0,11.0,12.0]}"},
                (String[]) records.get(1).getRow().getField(1));
    }

    @Test
    void parseMultiRowFlattenedStructArrayOfVectorChildByOuterRow() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId(
                                                                "message-multi-flat-struct-array-vector")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(
                                                multiRowFlattenedStructArrayOfVectorInsertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "509"))
                        .build();

        List<MilvusCdcRecord> records =
                new MilvusCdcRecordParser(vectorStructSchemaRegistry("collection_a", 3))
                        .parseRecords(response);

        Assertions.assertEquals(2, records.size());
        Assertions.assertArrayEquals(
                new String[] {
                    "{\"name\":\"a\",\"embedding\":[1.0,2.0,3.0]}",
                    "{\"name\":\"b\",\"embedding\":[4.0,5.0,6.0]}",
                    "{\"name\":\"c\",\"embedding\":[7.0,8.0,9.0]}",
                    "{\"name\":\"d\",\"embedding\":[10.0,11.0,12.0]}"
                },
                (String[]) records.get(0).getRow().getField(1));
        Assertions.assertArrayEquals(
                new String[] {
                    "{\"name\":\"e\",\"embedding\":[13.0,14.0,15.0]}",
                    "{\"name\":\"f\",\"embedding\":[16.0,17.0,18.0]}",
                    "{\"name\":\"g\",\"embedding\":[19.0,20.0,21.0]}",
                    "{\"name\":\"h\",\"embedding\":[22.0,23.0,24.0]}"
                },
                (String[]) records.get(1).getRow().getField(1));
    }

    @Test
    void parseStructArrayOfVectorEmptyOuterRowsByOuterRow() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-empty-struct-array-vector")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(structArrayOfVectorWithEmptyOuterRowPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "510"))
                        .build();

        List<MilvusCdcRecord> records =
                new MilvusCdcRecordParser(vectorStructSchemaRegistry("collection_a", 3))
                        .parseRecords(response);

        Assertions.assertEquals(3, records.size());
        Assertions.assertArrayEquals(
                new String[] {
                    "{\"name\":\"a\",\"embedding\":[1.0,2.0,3.0]}",
                    "{\"name\":\"b\",\"embedding\":[4.0,5.0,6.0]}"
                },
                (String[]) records.get(0).getRow().getField(1));
        Assertions.assertEquals(0, ((String[]) records.get(1).getRow().getField(1)).length);
        Assertions.assertArrayEquals(
                new String[] {"{\"name\":\"c\",\"embedding\":[7.0,8.0,9.0]}"},
                (String[]) records.get(2).getRow().getField(1));
    }

    @Test
    void rejectStructArrayOfVectorWithoutOuterRowAlignment() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId(
                                                                "message-invalid-struct-array-vector")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(misalignedStructArrayOfVectorPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "511"))
                        .build();

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new MilvusCdcRecordParser(
                                                vectorStructSchemaRegistry("collection_a", 3))
                                        .parseRecords(response));
        Assertions.assertTrue(exception.getMessage().contains("Milvus CDC"));
    }

    @Test
    void rejectVectorArraySparseFloatElementType() {
        DumpMessagesResponse response =
                response(
                        "message-array-vector-sparse-element",
                        sparseVectorArrayElementPayload(DataType.SparseFloatVector),
                        "512");

        UnsupportedOperationException exception =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () ->
                                new MilvusCdcRecordParser(
                                                vectorStructSchemaRegistry("collection_a", 3))
                                        .parseRecords(response));
        Assertions.assertTrue(exception.getMessage().contains("SparseFloatVector"));
    }

    @Test
    void rejectVectorArrayElementDataCaseMismatch() {
        DumpMessagesResponse response =
                response(
                        "message-array-vector-mismatch",
                        sparseVectorArrayElementPayload(DataType.FloatVector),
                        "513");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new MilvusCdcRecordParser(
                                                vectorStructSchemaRegistry("collection_a", 3))
                                        .parseRecords(response));
        Assertions.assertTrue(exception.getMessage().contains("elementType=FloatVector"));
    }

    @Test
    void rejectMultiRowNestedStructArrayPayloadBeforeProxyFlattening() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-multi-struct")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(multiRowNestedStructInsertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "503"))
                        .build();

        assertRejectsUnflattenedStructArrays(response);
    }

    @Test
    void parseBatchFlattenedStructArrayFieldsByOuterRow() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-batch-flat-struct")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(batchFlattenedStructInsertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "504"))
                        .build();

        List<MilvusCdcRecord> records =
                new MilvusCdcRecordParser(scalarStructSchemaRegistry("collection_a"))
                        .parseRecords(response);

        Assertions.assertEquals(2, records.size());
        Assertions.assertArrayEquals(
                new String[] {"{\"name\":\"a\",\"score\":1}", "{\"name\":\"b\",\"score\":2}"},
                (String[]) records.get(0).getRow().getField(1));
        Assertions.assertArrayEquals(
                new String[] {"{\"name\":\"c\",\"score\":3}", "{\"name\":\"d\",\"score\":4}"},
                (String[]) records.get(1).getRow().getField(1));
    }

    @Test
    void rejectBatchNestedStructArrayPayloadBeforeProxyFlattening() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-batch-nested-struct")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(batchNestedStructInsertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "505"))
                        .build();

        assertRejectsUnflattenedStructArrays(response);
    }

    @Test
    void filterUnconfiguredSourceTableAndAdvanceOffset() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-filtered")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(insertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "300"))
                        .build();
        MilvusCdcRecordParser parser =
                new MilvusCdcRecordParser(
                        EnumSet.allOf(MilvusCdcMessageType.class),
                        schemaRegistry("other_collection"));
        MilvusCdcMessage message = parser.parse(response);

        Assertions.assertEquals(MilvusCdcMessageKind.FILTERED, message.getKind());
        Assertions.assertTrue(message.shouldCheckpoint());
        Assertions.assertTrue(parser.parseRecords(response).isEmpty());
        Assertions.assertEquals(
                "message-filtered", parser.parseOffset(response).get().getConsumedMessageId());
    }

    @Test
    void rejectRowBasedInsertPayload() {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-row-based")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(rowBasedInsertPayload())
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "400"))
                        .build();

        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> parser("collection_a").parseRecords(response));
    }

    @Test
    void rejectInsertPayloadWithoutNumRows() {
        DumpMessagesResponse response =
                response("message-no-num-rows", insertPayloadWithoutNumRows(), "401");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> parser("collection_a").parseRecords(response));
        Assertions.assertTrue(exception.getMessage().contains("num_rows"));
    }

    @Test
    void rejectInsertPayloadWithoutFieldsData() {
        DumpMessagesResponse response =
                response("message-no-fields-data", insertPayloadWithoutFieldsData(), "402");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> parser("collection_a").parseRecords(response));
        Assertions.assertTrue(exception.getMessage().contains("fields_data"));
    }

    @Test
    void rejectDeletePayloadWithoutPrimaryKeys() {
        DumpMessagesResponse response =
                response(
                        "message-no-primary-keys",
                        deletePayloadWithoutPrimaryKeys(),
                        "403",
                        "Delete");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> parser("collection_a").parseRecords(response));
        Assertions.assertTrue(exception.getMessage().contains("primary keys"));
    }

    @Test
    void rejectInsertPayloadWithoutFieldName() {
        DumpMessagesResponse response =
                response("message-no-field-name", insertPayloadWithoutFieldName(), "404");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> parser("collection_a").parseRecords(response));
        Assertions.assertTrue(exception.getMessage().contains("field_name"));
    }

    @Test
    void rejectFloatVectorPayloadWithoutDim() {
        DumpMessagesResponse response =
                response("message-vector-no-dim", floatVectorWithoutDimPayload(), "405");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new MilvusCdcRecordParser(vectorSchemaRegistry("collection_a"))
                                        .parseRecords(response));
        Assertions.assertTrue(exception.getMessage().contains("dim"));
    }

    @Test
    void rejectBinaryVectorDimNotMultipleOf8() {
        DumpMessagesResponse response =
                response(
                        "message-binary-vector-bad-dim",
                        binaryVectorInsertPayload(10, (byte) 1, (byte) 2),
                        "406");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new MilvusCdcRecordParser(
                                                binaryVectorSchemaRegistry("collection_a"))
                                        .parseRecords(response));
        Assertions.assertTrue(exception.getMessage().contains("multiple of 8"));
    }

    @Test
    void rejectStructColumnWithoutStructFieldsSchema() {
        DumpMessagesResponse response =
                response("message-struct-no-schema", flattenedStructInsertPayload(), "407");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new MilvusCdcRecordParser(
                                                structWithoutFieldsSchemaRegistry("collection_a"))
                                        .parseRecords(response));
        Assertions.assertTrue(exception.getMessage().contains("struct_fields"));
    }

    @Test
    void rejectStructSchemaWithStoredChildFieldName() {
        DumpMessagesResponse response =
                response(
                        "message-stored-struct-child-schema",
                        flattenedStructInsertPayload(),
                        "408");

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new MilvusCdcRecordParser(
                                                structWithStoredChildFieldsSchemaRegistry(
                                                        "collection_a"))
                                        .parseRecords(response));
        Assertions.assertTrue(exception.getMessage().contains("must use raw child field names"));
        Assertions.assertTrue(exception.getMessage().contains("structs[name]"));
    }

    private void assertRejectsUnflattenedStructArrays(DumpMessagesResponse response) {
        UnsupportedOperationException exception =
                Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () ->
                                new MilvusCdcRecordParser(typedSchemaRegistry("collection_a"))
                                        .parseRecords(response));
        Assertions.assertTrue(exception.getMessage().contains("flattened struct sub-fields"));
    }

    private DumpMessagesResponse response(String messageId, ByteString payload, String timetick) {
        return response(messageId, payload, timetick, "Insert");
    }

    private DumpMessagesResponse response(
            String messageId, ByteString payload, String timetick, String messageType) {
        return DumpMessagesResponse.newBuilder()
                .setMessage(
                        ImmutableMessage.newBuilder()
                                .setId(
                                        MessageID.newBuilder()
                                                .setId(messageId)
                                                .setWALName(WALName.Pulsar))
                                .setPayload(payload)
                                .putProperties("_t", milvusMessageType(messageType))
                                .putProperties("_lcs", "")
                                .putProperties("timetick", timetick))
                .build();
    }

    private String milvusMessageType(String messageType) {
        switch (messageType) {
            case "Insert":
                return "2";
            case "Delete":
                return "3";
            default:
                throw new IllegalArgumentException("Unsupported test message type: " + messageType);
        }
    }

    private ByteString insertPayload() {
        return insertPayload(99L);
    }

    private ByteString insertPayload(long timestamp) {
        return insertPayload("default", "collection_a", timestamp);
    }

    private ByteString insertPayload(String database, String collection, long timestamp) {
        Msg.InsertRequest.Builder builder =
                Msg.InsertRequest.newBuilder()
                        .setPartitionName("partition_a")
                        .setNumRows(1)
                        .addTimestamps(timestamp)
                        .addRowIDs(1001L)
                        .addFieldsData(
                                FieldData.newBuilder()
                                        .setFieldName("id")
                                        .setType(DataType.Int64)
                                        .setScalars(
                                                ScalarField.newBuilder()
                                                        .setLongData(
                                                                LongArray.newBuilder()
                                                                        .addData(1001L))))
                        .addFieldsData(
                                FieldData.newBuilder()
                                        .setFieldName("name")
                                        .setType(DataType.VarChar)
                                        .setScalars(
                                                ScalarField.newBuilder()
                                                        .setStringData(
                                                                StringArray.newBuilder()
                                                                        .addData("alice"))));
        if (database != null) {
            builder.setDbName(database);
        }
        if (collection != null) {
            builder.setCollectionName(collection);
        }
        return builder.build().toByteString();
    }

    private ByteString insertPayloadWithoutNumRows() {
        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .addTimestamps(401L)
                .addRowIDs(1001L)
                .addFieldsData(
                        FieldData.newBuilder()
                                .setFieldName("id")
                                .setType(DataType.Int64)
                                .setScalars(
                                        ScalarField.newBuilder()
                                                .setLongData(
                                                        LongArray.newBuilder().addData(1001L))))
                .addFieldsData(
                        FieldData.newBuilder()
                                .setFieldName("name")
                                .setType(DataType.VarChar)
                                .setScalars(
                                        ScalarField.newBuilder()
                                                .setStringData(
                                                        StringArray.newBuilder().addData("alice"))))
                .build()
                .toByteString();
    }

    private ByteString insertPayloadWithoutFieldsData() {
        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(402L)
                .addRowIDs(1001L)
                .build()
                .toByteString();
    }

    private ByteString insertPayloadWithoutFieldName() {
        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(402L)
                .addRowIDs(1001L)
                .addFieldsData(
                        FieldData.newBuilder()
                                .setType(DataType.Int64)
                                .setScalars(
                                        ScalarField.newBuilder()
                                                .setLongData(
                                                        LongArray.newBuilder().addData(1001L))))
                .addFieldsData(
                        FieldData.newBuilder()
                                .setFieldName("name")
                                .setType(DataType.VarChar)
                                .setScalars(
                                        ScalarField.newBuilder()
                                                .setStringData(
                                                        StringArray.newBuilder().addData("alice"))))
                .build()
                .toByteString();
    }

    private ByteString deletePayloadWithoutPrimaryKeys() {
        return Msg.DeleteRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(403L)
                .build()
                .toByteString();
    }

    private ByteString floatVectorInsertPayload() {
        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(250L)
                .addRowIDs(1001L)
                .addFieldsData(
                        FieldData.newBuilder()
                                .setFieldName("id")
                                .setType(DataType.Int64)
                                .setScalars(
                                        ScalarField.newBuilder()
                                                .setLongData(
                                                        LongArray.newBuilder().addData(1001L))))
                .addFieldsData(
                        FieldData.newBuilder()
                                .setFieldName("vec")
                                .setType(DataType.FloatVector)
                                .setVectors(
                                        VectorField.newBuilder()
                                                .setDim(4)
                                                .setFloatVector(
                                                        FloatArray.newBuilder()
                                                                .addData(0.1F)
                                                                .addData(0.2F)
                                                                .addData(0.3F)
                                                                .addData(0.4F))))
                .build()
                .toByteString();
    }

    private ByteString binaryVectorInsertPayload(int dim, byte... binaryVector) {
        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(2)
                .addTimestamps(260L)
                .addTimestamps(261L)
                .addRowIDs(1001L)
                .addRowIDs(1002L)
                .addFieldsData(
                        FieldData.newBuilder()
                                .setFieldName("id")
                                .setType(DataType.Int64)
                                .setScalars(
                                        ScalarField.newBuilder()
                                                .setLongData(
                                                        LongArray.newBuilder()
                                                                .addData(1001L)
                                                                .addData(1002L))))
                .addFieldsData(
                        FieldData.newBuilder()
                                .setFieldName("vec")
                                .setType(DataType.BinaryVector)
                                .setVectors(
                                        VectorField.newBuilder()
                                                .setDim(dim)
                                                .setBinaryVector(
                                                        ByteString.copyFrom(binaryVector))))
                .build()
                .toByteString();
    }

    private ByteString floatVectorWithoutDimPayload() {
        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(403L)
                .addRowIDs(1001L)
                .addFieldsData(
                        FieldData.newBuilder()
                                .setFieldName("id")
                                .setType(DataType.Int64)
                                .setScalars(
                                        ScalarField.newBuilder()
                                                .setLongData(
                                                        LongArray.newBuilder().addData(1001L))))
                .addFieldsData(
                        FieldData.newBuilder()
                                .setFieldName("vec")
                                .setType(DataType.FloatVector)
                                .setVectors(
                                        VectorField.newBuilder()
                                                .setFloatVector(
                                                        FloatArray.newBuilder()
                                                                .addData(0.1F)
                                                                .addData(0.2F)
                                                                .addData(0.3F)
                                                                .addData(0.4F))))
                .build()
                .toByteString();
    }

    private ByteString nullableInsertPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(
                                                LongArray.newBuilder()
                                                        .addData(9001L)
                                                        .addData(9002L)
                                                        .addData(9003L)
                                                        .addData(9004L)
                                                        .addData(9005L)))
                        .build();
        FieldData vectorField =
                FieldData.newBuilder()
                        .setFieldName("vector")
                        .setType(DataType.FloatVector)
                        .setVectors(
                                floatVectorField(
                                        4, 0.11F, 0.12F, 0.13F, 0.14F, 0.31F, 0.32F, 0.33F, 0.34F))
                        .addValidData(true)
                        .addValidData(false)
                        .addValidData(true)
                        .addValidData(false)
                        .addValidData(false)
                        .build();
        FieldData optIntField =
                FieldData.newBuilder()
                        .setFieldName("opt_int")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(
                                                LongArray.newBuilder()
                                                        .addData(101L)
                                                        .addData(0L)
                                                        .addData(0L)
                                                        .addData(404L)
                                                        .addData(0L)))
                        .addValidData(true)
                        .addValidData(false)
                        .addValidData(false)
                        .addValidData(true)
                        .addValidData(false)
                        .build();
        FieldData optTextField =
                FieldData.newBuilder()
                        .setFieldName("opt_text")
                        .setType(DataType.VarChar)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setStringData(
                                                StringArray.newBuilder()
                                                        .addData("all-present")
                                                        .addData("")
                                                        .addData("empty-struct-array")
                                                        .addData("mixed-null")
                                                        .addData("")))
                        .addValidData(true)
                        .addValidData(false)
                        .addValidData(true)
                        .addValidData(true)
                        .addValidData(false)
                        .build();
        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(5)
                .addTimestamps(251L)
                .addTimestamps(252L)
                .addTimestamps(253L)
                .addTimestamps(254L)
                .addTimestamps(255L)
                .addRowIDs(9001L)
                .addRowIDs(9002L)
                .addRowIDs(9003L)
                .addRowIDs(9004L)
                .addRowIDs(9005L)
                .addFieldsData(idField)
                .addFieldsData(vectorField)
                .addFieldsData(optIntField)
                .addFieldsData(optTextField)
                .build()
                .toByteString();
    }

    private ByteString compactNullableScalarPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(
                                                LongArray.newBuilder()
                                                        .addData(9001L)
                                                        .addData(9002L)
                                                        .addData(9003L)
                                                        .addData(9004L)
                                                        .addData(9005L)))
                        .build();
        FieldData optIntField =
                FieldData.newBuilder()
                        .setFieldName("opt_int")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(
                                                LongArray.newBuilder().addData(101L).addData(404L)))
                        .addValidData(true)
                        .addValidData(false)
                        .addValidData(false)
                        .addValidData(true)
                        .addValidData(false)
                        .build();
        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(5)
                .addTimestamps(252L)
                .addTimestamps(253L)
                .addTimestamps(254L)
                .addTimestamps(255L)
                .addTimestamps(256L)
                .addRowIDs(9001L)
                .addRowIDs(9002L)
                .addRowIDs(9003L)
                .addRowIDs(9004L)
                .addRowIDs(9005L)
                .addFieldsData(idField)
                .addFieldsData(optIntField)
                .build()
                .toByteString();
    }

    private void assertParsedTimestamptz(long unixMicros, String expected) {
        DumpMessagesResponse response =
                DumpMessagesResponse.newBuilder()
                        .setMessage(
                                ImmutableMessage.newBuilder()
                                        .setId(
                                                MessageID.newBuilder()
                                                        .setId("message-timestamptz")
                                                        .setWALName(WALName.Pulsar))
                                        .setPayload(typedInsertPayload(unixMicros))
                                        .putProperties("_t", "2")
                                        .putProperties("_lcs", "")
                                        .putProperties("timetick", "500"))
                        .build();

        List<MilvusCdcRecord> records =
                new MilvusCdcRecordParser(typedSchemaRegistry("collection_a"))
                        .parseRecords(response);

        Assertions.assertEquals(expected, records.get(0).getRow().getField(7));
    }

    private ByteString typedInsertPayload() {
        return typedInsertPayload(1_700_000_000_000_000L);
    }

    private ByteString typedInsertPayload(long timestamptzMicros) {
        SortedMap<Long, Float> sparseVector = new TreeMap<>();
        sparseVector.put(7L, 0.25F);
        sparseVector.put(42L, 1.5F);
        ByteBuffer encodedSparseVector = ParamUtils.encodeSparseFloatVector(sparseVector);
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(LongArray.newBuilder().addData(1001L)))
                        .build();
        FieldData jsonField =
                FieldData.newBuilder()
                        .setFieldName("json_col")
                        .setType(DataType.JSON)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setJsonData(
                                                JSONArray.newBuilder()
                                                        .addData(
                                                                ByteString.copyFromUtf8(
                                                                        "{\"k\":\"v\"}"))))
                        .build();
        FieldData tagsField =
                FieldData.newBuilder()
                        .setFieldName("tags")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(
                                                                ScalarField.newBuilder()
                                                                        .setStringData(
                                                                                StringArray
                                                                                        .newBuilder()
                                                                                        .addData(
                                                                                                "x")
                                                                                        .addData(
                                                                                                "y")))))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("structs[name]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(
                                                                ScalarField.newBuilder()
                                                                        .setStringData(
                                                                                StringArray
                                                                                        .newBuilder()
                                                                                        .addData(
                                                                                                "a")
                                                                                        .addData(
                                                                                                "b")))))
                        .build();
        FieldData structScoreField =
                FieldData.newBuilder()
                        .setFieldName("structs[score]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(
                                                                ScalarField.newBuilder()
                                                                        .setLongData(
                                                                                LongArray
                                                                                        .newBuilder()
                                                                                        .addData(1L)
                                                                                        .addData(
                                                                                                2L)))))
                        .build();
        FieldData structEmbeddingField =
                FieldData.newBuilder()
                        .setFieldName("structs[embedding]")
                        .setType(DataType.ArrayOfVector)
                        .setVectors(
                                vectorArrayField(
                                        4,
                                        new float[] {
                                            0.1F, 0.2F, 0.3F, 0.4F,
                                            0.5F, 0.6F, 0.7F, 0.8F
                                        }))
                        .build();
        FieldData sparseField =
                FieldData.newBuilder()
                        .setFieldName("sparse")
                        .setType(DataType.SparseFloatVector)
                        .setVectors(
                                VectorField.newBuilder()
                                        .setSparseFloatVector(
                                                SparseFloatArray.newBuilder()
                                                        .addContents(
                                                                ByteString.copyFrom(
                                                                        encodedSparseVector
                                                                                .array()))))
                        .build();
        FieldData int8VectorField =
                FieldData.newBuilder()
                        .setFieldName("int8_vec")
                        .setType(DataType.Int8Vector)
                        .setVectors(
                                VectorField.newBuilder()
                                        .setDim(4)
                                        .setInt8Vector(
                                                ByteString.copyFrom(new byte[] {1, -2, 3, -4})))
                        .build();
        FieldData geometryField =
                FieldData.newBuilder()
                        .setFieldName("geom")
                        .setType(DataType.Geometry)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setGeometryData(
                                                GeometryArray.newBuilder()
                                                        .addData(
                                                                ByteString.copyFrom(
                                                                        new byte[] {1, 2, 3}))))
                        .build();
        FieldData timestamptzField =
                FieldData.newBuilder()
                        .setFieldName("ts")
                        .setType(DataType.Timestamptz)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setTimestamptzData(
                                                TimestamptzArray.newBuilder()
                                                        .addData(timestamptzMicros)))
                        .build();
        FieldData dynamicField =
                FieldData.newBuilder()
                        .setFieldName("extra")
                        .setIsDynamic(true)
                        .setType(DataType.VarChar)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setStringData(
                                                StringArray.newBuilder().addData("dynamic-value")))
                        .build();
        FieldData internalDynamicField =
                FieldData.newBuilder()
                        .setFieldName("$meta")
                        .setIsDynamic(true)
                        .setType(DataType.JSON)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setJsonData(
                                                JSONArray.newBuilder()
                                                        .addData(
                                                                ByteString.copyFromUtf8(
                                                                        "{\"key0\":\"value0\",\"dynamicInt\":106}"))))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(500L)
                .addRowIDs(1001L)
                .addFieldsData(idField)
                .addFieldsData(jsonField)
                .addFieldsData(tagsField)
                .addFieldsData(structNameField)
                .addFieldsData(structScoreField)
                .addFieldsData(structEmbeddingField)
                .addFieldsData(sparseField)
                .addFieldsData(int8VectorField)
                .addFieldsData(geometryField)
                .addFieldsData(timestamptzField)
                .addFieldsData(dynamicField)
                .addFieldsData(internalDynamicField)
                .build()
                .toByteString();
    }

    private ByteString flattenedStructInsertPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(LongArray.newBuilder().addData(1001L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("structs[name]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(
                                                                ScalarField.newBuilder()
                                                                        .setStringData(
                                                                                StringArray
                                                                                        .newBuilder()
                                                                                        .addData(
                                                                                                "a")
                                                                                        .addData(
                                                                                                "b")))))
                        .build();
        FieldData structScoreField =
                FieldData.newBuilder()
                        .setFieldName("structs[score]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(
                                                                ScalarField.newBuilder()
                                                                        .setLongData(
                                                                                LongArray
                                                                                        .newBuilder()
                                                                                        .addData(1L)
                                                                                        .addData(
                                                                                                2L)))))
                        .build();
        FieldData dynamicField =
                FieldData.newBuilder()
                        .setFieldName("extra")
                        .setIsDynamic(true)
                        .setType(DataType.VarChar)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setStringData(
                                                StringArray.newBuilder().addData("dynamic-value")))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(501L)
                .addRowIDs(1001L)
                .addFieldsData(idField)
                .addFieldsData(structNameField)
                .addFieldsData(structScoreField)
                .addFieldsData(dynamicField)
                .build()
                .toByteString();
    }

    private ByteString multipleFlattenedStructsPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(LongArray.newBuilder().addData(1001L)))
                        .build();
        FieldData paCaField =
                FieldData.newBuilder()
                        .setFieldName("pa[ca]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(stringArray("a1", "a2"))))
                        .build();
        FieldData paCbField =
                FieldData.newBuilder()
                        .setFieldName("pa[cb]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(longArray(10L, 20L))))
                        .build();
        FieldData pbCa2Field =
                FieldData.newBuilder()
                        .setFieldName("pb[ca2]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(stringArray("b1", "b2"))))
                        .build();
        FieldData pbCb2Field =
                FieldData.newBuilder()
                        .setFieldName("pb[cb2]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(longArray(100L, 200L))))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(502L)
                .addRowIDs(1001L)
                .addFieldsData(idField)
                .addFieldsData(paCaField)
                .addFieldsData(paCbField)
                .addFieldsData(pbCa2Field)
                .addFieldsData(pbCb2Field)
                .build()
                .toByteString();
    }

    private ByteString duplicateFlattenedStructChildPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(LongArray.newBuilder().addData(1001L)))
                        .build();
        FieldData firstPaCaField =
                FieldData.newBuilder()
                        .setFieldName("pa[ca]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(stringArray("a1", "a2"))))
                        .build();
        FieldData secondPaCaField =
                FieldData.newBuilder()
                        .setFieldName("pa[ca]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(stringArray("a3", "a4"))))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(503L)
                .addRowIDs(1001L)
                .addFieldsData(idField)
                .addFieldsData(firstPaCaField)
                .addFieldsData(secondPaCaField)
                .build()
                .toByteString();
    }

    private ByteString flattenedStructInsertPayloadMissingChildField() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(LongArray.newBuilder().addData(1001L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("structs[name]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(stringArray("a", "b"))))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(504L)
                .addRowIDs(1001L)
                .addFieldsData(idField)
                .addFieldsData(structNameField)
                .build()
                .toByteString();
    }

    private ByteString flattenedStructInsertPayloadUnexpectedChildField() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(LongArray.newBuilder().addData(1001L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("structs[name]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(stringArray("a", "b"))))
                        .build();
        FieldData structScoreField =
                FieldData.newBuilder()
                        .setFieldName("structs[score]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder().addData(longArray(1L, 2L))))
                        .build();
        FieldData structUnknownField =
                FieldData.newBuilder()
                        .setFieldName("structs[unknown]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(stringArray("x", "y"))))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(505L)
                .addRowIDs(1001L)
                .addFieldsData(idField)
                .addFieldsData(structNameField)
                .addFieldsData(structScoreField)
                .addFieldsData(structUnknownField)
                .build()
                .toByteString();
    }

    private ByteString flattenedStructInsertPayloadWithScalarChildValue() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(LongArray.newBuilder().addData(1001L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("structs[name]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setStringData(StringArray.newBuilder().addData("a")))
                        .build();
        FieldData structScoreField =
                FieldData.newBuilder()
                        .setFieldName("structs[score]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder().addData(longArray(1L))))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(506L)
                .addRowIDs(1001L)
                .addFieldsData(idField)
                .addFieldsData(structNameField)
                .addFieldsData(structScoreField)
                .build()
                .toByteString();
    }

    private ByteString flattenedStructInsertPayloadWithMissingScore() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(LongArray.newBuilder().addData(1001L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("structs[name]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(
                                                                ScalarField.newBuilder()
                                                                        .setStringData(
                                                                                StringArray
                                                                                        .newBuilder()
                                                                                        .addData(
                                                                                                "a")
                                                                                        .addData(
                                                                                                "b")))))
                        .build();
        FieldData structScoreField =
                FieldData.newBuilder()
                        .setFieldName("structs[score]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(
                                                                ScalarField.newBuilder()
                                                                        .setLongData(
                                                                                LongArray
                                                                                        .newBuilder()
                                                                                        .addData(
                                                                                                1L)))))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(502L)
                .addRowIDs(1001L)
                .addFieldsData(idField)
                .addFieldsData(structNameField)
                .addFieldsData(structScoreField)
                .build()
                .toByteString();
    }

    private ByteString nullableFlattenedStructInsertPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(
                                                LongArray.newBuilder()
                                                        .addData(1001L)
                                                        .addData(1002L)
                                                        .addData(1003L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("structs[name]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(stringArray("a"))
                                                        .addData(stringArray("c"))))
                        .addValidData(true)
                        .addValidData(false)
                        .addValidData(true)
                        .build();
        FieldData structScoreField =
                FieldData.newBuilder()
                        .setFieldName("structs[score]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(longArray(1L))
                                                        .addData(longArray(3L))))
                        .addValidData(true)
                        .addValidData(false)
                        .addValidData(true)
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(3)
                .addTimestamps(503L)
                .addTimestamps(504L)
                .addTimestamps(505L)
                .addRowIDs(1001L)
                .addRowIDs(1002L)
                .addRowIDs(1003L)
                .addFieldsData(idField)
                .addFieldsData(structNameField)
                .addFieldsData(structScoreField)
                .build()
                .toByteString();
    }

    private ByteString nestedStructInsertPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(LongArray.newBuilder().addData(1001L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("name")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(wrappedStringArray("a", "b"))))
                        .build();
        FieldData structScoreField =
                FieldData.newBuilder()
                        .setFieldName("score")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(wrappedLongArray(1L, 2L))))
                        .build();
        FieldData structsField =
                FieldData.newBuilder()
                        .setFieldName("structs")
                        .setType(DataType.Array)
                        .setStructArrays(
                                StructArrayField.newBuilder()
                                        .addFields(structNameField)
                                        .addFields(structScoreField))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(502L)
                .addRowIDs(1001L)
                .addFieldsData(idField)
                .addFieldsData(structsField)
                .build()
                .toByteString();
    }

    private ByteString flattenedStructVectorInsertPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(LongArray.newBuilder().addData(1001L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("structs[name]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(
                                                                ScalarField.newBuilder()
                                                                        .setStringData(
                                                                                StringArray
                                                                                        .newBuilder()
                                                                                        .addData(
                                                                                                "a")
                                                                                        .addData(
                                                                                                "b")))))
                        .build();
        FieldData structEmbeddingField =
                FieldData.newBuilder()
                        .setFieldName("structs[embedding]")
                        .setType(DataType.ArrayOfVector)
                        .setVectors(
                                vectorArrayField(4, new float[] {1F, 2F, 3F, 4F, 5F, 6F, 7F, 8F}))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(506L)
                .addRowIDs(1001L)
                .addFieldsData(idField)
                .addFieldsData(structNameField)
                .addFieldsData(structEmbeddingField)
                .build()
                .toByteString();
    }

    private ByteString nestedStructVectorInsertPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(LongArray.newBuilder().addData(1001L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("name")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(
                                                                ScalarField.newBuilder()
                                                                        .setStringData(
                                                                                StringArray
                                                                                        .newBuilder()
                                                                                        .addData(
                                                                                                "a")
                                                                                        .addData(
                                                                                                "b")))))
                        .build();
        FieldData structEmbeddingField =
                FieldData.newBuilder()
                        .setFieldName("embedding")
                        .setType(DataType.ArrayOfVector)
                        .setVectors(
                                vectorArrayField(4, new float[] {1F, 2F, 3F, 4F, 5F, 6F, 7F, 8F}))
                        .build();
        FieldData structsField =
                FieldData.newBuilder()
                        .setFieldName("structs")
                        .setType(DataType.Array)
                        .setStructArrays(
                                StructArrayField.newBuilder()
                                        .addFields(structNameField)
                                        .addFields(structEmbeddingField))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(507L)
                .addRowIDs(1001L)
                .addFieldsData(idField)
                .addFieldsData(structsField)
                .build()
                .toByteString();
    }

    private ByteString multiRowFlattenedStructVectorInsertPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(
                                                LongArray.newBuilder()
                                                        .addData(1001L)
                                                        .addData(1002L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("structs[name]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(
                                                                ScalarField.newBuilder()
                                                                        .setStringData(
                                                                                StringArray
                                                                                        .newBuilder()
                                                                                        .addData(
                                                                                                "a")
                                                                                        .addData(
                                                                                                "b")))
                                                        .addData(
                                                                ScalarField.newBuilder()
                                                                        .setStringData(
                                                                                StringArray
                                                                                        .newBuilder()
                                                                                        .addData(
                                                                                                "c")))))
                        .build();
        FieldData structEmbeddingField =
                FieldData.newBuilder()
                        .setFieldName("structs[embedding]")
                        .setType(DataType.ArrayOfVector)
                        .setVectors(
                                vectorArrayField(
                                        4,
                                        new float[] {1F, 2F, 3F, 4F, 5F, 6F, 7F, 8F},
                                        new float[] {9F, 10F, 11F, 12F}))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(2)
                .addTimestamps(508L)
                .addTimestamps(509L)
                .addRowIDs(1001L)
                .addRowIDs(1002L)
                .addFieldsData(idField)
                .addFieldsData(structNameField)
                .addFieldsData(structEmbeddingField)
                .build()
                .toByteString();
    }

    private ByteString multiRowFlattenedStructArrayOfVectorInsertPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(
                                                LongArray.newBuilder()
                                                        .addData(1001L)
                                                        .addData(1002L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("structs[name]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(
                                                                ScalarField.newBuilder()
                                                                        .setStringData(
                                                                                StringArray
                                                                                        .newBuilder()
                                                                                        .addData(
                                                                                                "a")
                                                                                        .addData(
                                                                                                "b")
                                                                                        .addData(
                                                                                                "c")
                                                                                        .addData(
                                                                                                "d")))
                                                        .addData(
                                                                ScalarField.newBuilder()
                                                                        .setStringData(
                                                                                StringArray
                                                                                        .newBuilder()
                                                                                        .addData(
                                                                                                "e")
                                                                                        .addData(
                                                                                                "f")
                                                                                        .addData(
                                                                                                "g")
                                                                                        .addData(
                                                                                                "h")))))
                        .build();
        FieldData structEmbeddingField =
                FieldData.newBuilder()
                        .setFieldName("structs[embedding]")
                        .setType(DataType.ArrayOfVector)
                        .setVectors(
                                vectorArrayField(
                                        3,
                                        new float[] {
                                            1F, 2F, 3F,
                                            4F, 5F, 6F,
                                            7F, 8F, 9F,
                                            10F, 11F, 12F
                                        },
                                        new float[] {
                                            13F, 14F, 15F,
                                            16F, 17F, 18F,
                                            19F, 20F, 21F,
                                            22F, 23F, 24F
                                        }))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(2)
                .addTimestamps(509L)
                .addTimestamps(510L)
                .addRowIDs(1001L)
                .addRowIDs(1002L)
                .addFieldsData(idField)
                .addFieldsData(structNameField)
                .addFieldsData(structEmbeddingField)
                .build()
                .toByteString();
    }

    private ByteString structArrayOfVectorWithEmptyOuterRowPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(
                                                LongArray.newBuilder()
                                                        .addData(1001L)
                                                        .addData(1002L)
                                                        .addData(1003L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("structs[name]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(stringArray("a", "b"))
                                                        .addData(stringArray())
                                                        .addData(stringArray("c"))))
                        .build();
        FieldData structEmbeddingField =
                FieldData.newBuilder()
                        .setFieldName("structs[embedding]")
                        .setType(DataType.ArrayOfVector)
                        .setVectors(
                                vectorArrayField(
                                        3,
                                        new float[] {1F, 2F, 3F, 4F, 5F, 6F},
                                        new float[] {},
                                        new float[] {7F, 8F, 9F}))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(3)
                .addTimestamps(510L)
                .addTimestamps(511L)
                .addTimestamps(512L)
                .addRowIDs(1001L)
                .addRowIDs(1002L)
                .addRowIDs(1003L)
                .addFieldsData(idField)
                .addFieldsData(structNameField)
                .addFieldsData(structEmbeddingField)
                .build()
                .toByteString();
    }

    private ByteString misalignedStructArrayOfVectorPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(
                                                LongArray.newBuilder()
                                                        .addData(1001L)
                                                        .addData(1002L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("structs[name]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(stringArray("a"))
                                                        .addData(stringArray("b"))))
                        .build();
        FieldData structEmbeddingField =
                FieldData.newBuilder()
                        .setFieldName("structs[embedding]")
                        .setType(DataType.ArrayOfVector)
                        .setVectors(vectorArrayField(3, new float[] {1F, 2F, 3F}))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(2)
                .addTimestamps(511L)
                .addTimestamps(512L)
                .addRowIDs(1001L)
                .addRowIDs(1002L)
                .addFieldsData(idField)
                .addFieldsData(structNameField)
                .addFieldsData(structEmbeddingField)
                .build()
                .toByteString();
    }

    private ByteString sparseVectorArrayElementPayload(DataType elementType) {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(LongArray.newBuilder().addData(1001L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("structs[name]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder().addData(stringArray("a"))))
                        .build();
        FieldData structEmbeddingField =
                FieldData.newBuilder()
                        .setFieldName("structs[embedding]")
                        .setType(DataType.ArrayOfVector)
                        .setVectors(sparseVectorArrayField(elementType))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addTimestamps(512L)
                .addRowIDs(1001L)
                .addFieldsData(idField)
                .addFieldsData(structNameField)
                .addFieldsData(structEmbeddingField)
                .build()
                .toByteString();
    }

    private ByteString multiRowNestedStructInsertPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(
                                                LongArray.newBuilder()
                                                        .addData(1001L)
                                                        .addData(1002L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("name")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(wrappedStringArray("a", "b"))
                                                        .addData(wrappedStringArray("c"))))
                        .build();
        FieldData structScoreField =
                FieldData.newBuilder()
                        .setFieldName("score")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(wrappedLongArray(1L, 2L))
                                                        .addData(wrappedLongArray(3L))))
                        .build();
        FieldData structsField =
                FieldData.newBuilder()
                        .setFieldName("structs")
                        .setType(DataType.Array)
                        .setStructArrays(
                                StructArrayField.newBuilder()
                                        .addFields(structNameField)
                                        .addFields(structScoreField))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(2)
                .addTimestamps(502L)
                .addTimestamps(503L)
                .addRowIDs(1001L)
                .addRowIDs(1002L)
                .addFieldsData(idField)
                .addFieldsData(structsField)
                .build()
                .toByteString();
    }

    private ByteString batchFlattenedStructInsertPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(
                                                LongArray.newBuilder()
                                                        .addData(1001L)
                                                        .addData(1002L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("structs[name]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(stringArray("a", "b"))
                                                        .addData(stringArray("c", "d"))))
                        .build();
        FieldData structScoreField =
                FieldData.newBuilder()
                        .setFieldName("structs[score]")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(longArray(1L, 2L))
                                                        .addData(longArray(3L, 4L))))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(2)
                .addTimestamps(504L)
                .addTimestamps(505L)
                .addRowIDs(1001L)
                .addRowIDs(1002L)
                .addFieldsData(idField)
                .addFieldsData(structNameField)
                .addFieldsData(structScoreField)
                .build()
                .toByteString();
    }

    private ByteString batchNestedStructInsertPayload() {
        FieldData idField =
                FieldData.newBuilder()
                        .setFieldName("id")
                        .setType(DataType.Int64)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setLongData(
                                                LongArray.newBuilder()
                                                        .addData(1001L)
                                                        .addData(1002L)))
                        .build();
        FieldData structNameField =
                FieldData.newBuilder()
                        .setFieldName("name")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(stringArray("a", "b"))
                                                        .addData(stringArray("c", "d"))))
                        .build();
        FieldData structScoreField =
                FieldData.newBuilder()
                        .setFieldName("score")
                        .setType(DataType.Array)
                        .setScalars(
                                ScalarField.newBuilder()
                                        .setArrayData(
                                                ArrayArray.newBuilder()
                                                        .addData(longArray(1L, 2L))
                                                        .addData(longArray(3L, 4L))))
                        .build();
        FieldData structsField =
                FieldData.newBuilder()
                        .setFieldName("structs")
                        .setType(DataType.Array)
                        .setStructArrays(
                                StructArrayField.newBuilder()
                                        .addFields(structNameField)
                                        .addFields(structScoreField))
                        .build();

        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(2)
                .addTimestamps(504L)
                .addTimestamps(505L)
                .addRowIDs(1001L)
                .addRowIDs(1002L)
                .addFieldsData(idField)
                .addFieldsData(structsField)
                .build()
                .toByteString();
    }

    private ScalarField wrappedStringArray(String... values) {
        return ScalarField.newBuilder()
                .setArrayData(ArrayArray.newBuilder().addData(stringArray(values)))
                .build();
    }

    private ScalarField stringArray(String... values) {
        StringArray.Builder builder = StringArray.newBuilder();
        for (String value : values) {
            builder.addData(value);
        }
        return ScalarField.newBuilder().setStringData(builder).build();
    }

    private ScalarField wrappedLongArray(long... values) {
        return ScalarField.newBuilder()
                .setArrayData(ArrayArray.newBuilder().addData(longArray(values)))
                .build();
    }

    private ScalarField longArray(long... values) {
        LongArray.Builder builder = LongArray.newBuilder();
        for (long value : values) {
            builder.addData(value);
        }
        return ScalarField.newBuilder().setLongData(builder).build();
    }

    private VectorField floatVectorField(int dim, float... values) {
        FloatArray.Builder builder = FloatArray.newBuilder();
        for (float value : values) {
            builder.addData(value);
        }
        return VectorField.newBuilder().setDim(dim).setFloatVector(builder).build();
    }

    private VectorField vectorArrayField(int dim, float[]... rowValues) {
        VectorArray.Builder builder =
                VectorArray.newBuilder().setDim(dim).setElementType(DataType.FloatVector);
        for (float[] values : rowValues) {
            builder.addData(floatVectorField(dim, values));
        }
        return VectorField.newBuilder().setVectorArray(builder).build();
    }

    private VectorField sparseVectorArrayField(DataType elementType) {
        SortedMap<Long, Float> sparseVector = new TreeMap<>();
        sparseVector.put(7L, 0.25F);
        ByteBuffer encodedSparseVector = ParamUtils.encodeSparseFloatVector(sparseVector);
        VectorField nested =
                VectorField.newBuilder()
                        .setSparseFloatVector(
                                SparseFloatArray.newBuilder()
                                        .addContents(
                                                ByteString.copyFrom(encodedSparseVector.array())))
                        .build();
        return VectorField.newBuilder()
                .setVectorArray(
                        VectorArray.newBuilder()
                                .setDim(8)
                                .setElementType(elementType)
                                .addData(nested))
                .build();
    }

    private byte[] bytes(ByteBuffer byteBuffer) {
        ByteBuffer duplicate = byteBuffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

    private ByteString deletePayload() {
        return Msg.DeleteRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setPartitionName("partition_a")
                .setNumRows(1)
                .addTimestamps(1L)
                .setPrimaryKeys(IDs.newBuilder().setIntId(LongArray.newBuilder().addData(1001L)))
                .build()
                .toByteString();
    }

    private ByteString multiRowInsertPayload() {
        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setPartitionName("partition_a")
                .setNumRows(2)
                .addTimestamps(199L)
                .addTimestamps(200L)
                .addRowIDs(1001L)
                .addRowIDs(1002L)
                .addFieldsData(
                        FieldData.newBuilder()
                                .setFieldName("id")
                                .setType(DataType.Int64)
                                .setScalars(
                                        ScalarField.newBuilder()
                                                .setLongData(
                                                        LongArray.newBuilder()
                                                                .addData(1001L)
                                                                .addData(1002L))))
                .addFieldsData(
                        FieldData.newBuilder()
                                .setFieldName("name")
                                .setType(DataType.VarChar)
                                .setScalars(
                                        ScalarField.newBuilder()
                                                .setStringData(
                                                        StringArray.newBuilder()
                                                                .addData("alice")
                                                                .addData("bob"))))
                .build()
                .toByteString();
    }

    private ByteString rowBasedInsertPayload() {
        return Msg.InsertRequest.newBuilder()
                .setDbName("default")
                .setCollectionName("collection_a")
                .setNumRows(1)
                .addRowData(Blob.newBuilder().setValue(ByteString.copyFromUtf8("legacy-row")))
                .build()
                .toByteString();
    }

    private MilvusCdcRecordParser parser(String sourceCollection) {
        return new MilvusCdcRecordParser(schemaRegistry(sourceCollection));
    }

    private MilvusCdcCollectionSchemaRegistry schemaRegistry(String sourceCollection) {
        CatalogTable catalogTable = catalogTable(sourceCollection);
        return schemaRegistry(sourceCollection, catalogTable);
    }

    private MilvusCdcCollectionSchemaRegistry vectorSchemaRegistry(String sourceCollection) {
        CatalogTable catalogTable = vectorCatalogTable(sourceCollection);
        return schemaRegistry(sourceCollection, catalogTable);
    }

    private MilvusCdcCollectionSchemaRegistry binaryVectorSchemaRegistry(String sourceCollection) {
        CatalogTable catalogTable = binaryVectorCatalogTable(sourceCollection);
        return schemaRegistry(sourceCollection, catalogTable);
    }

    private MilvusCdcCollectionSchemaRegistry nullableSchemaRegistry(String sourceCollection) {
        CatalogTable catalogTable = nullableCatalogTable(sourceCollection);
        return schemaRegistry(sourceCollection, catalogTable);
    }

    private MilvusCdcCollectionSchemaRegistry nullableScalarSchemaRegistry(
            String sourceCollection) {
        CatalogTable catalogTable = nullableScalarCatalogTable(sourceCollection);
        return schemaRegistry(sourceCollection, catalogTable);
    }

    private MilvusCdcCollectionSchemaRegistry typedSchemaRegistry(String sourceCollection) {
        return typedSchemaRegistry(sourceCollection, 4);
    }

    private MilvusCdcCollectionSchemaRegistry typedSchemaRegistry(
            String sourceCollection, int structEmbeddingDimension) {
        CatalogTable catalogTable = typedCatalogTable(sourceCollection, structEmbeddingDimension);
        return schemaRegistry(sourceCollection, catalogTable);
    }

    private MilvusCdcCollectionSchemaRegistry scalarStructSchemaRegistry(String sourceCollection) {
        CatalogTable catalogTable = structCatalogTable(sourceCollection, 4, true, false);
        return schemaRegistry(sourceCollection, catalogTable);
    }

    private MilvusCdcCollectionSchemaRegistry multipleStructSchemaRegistry(
            String sourceCollection) {
        CatalogTable catalogTable = multipleStructCatalogTable(sourceCollection);
        return schemaRegistry(sourceCollection, catalogTable);
    }

    private MilvusCdcCollectionSchemaRegistry vectorStructSchemaRegistry(
            String sourceCollection, int structEmbeddingDimension) {
        CatalogTable catalogTable =
                structCatalogTable(sourceCollection, structEmbeddingDimension, false, true);
        return schemaRegistry(sourceCollection, catalogTable);
    }

    private MilvusCdcCollectionSchemaRegistry structWithoutFieldsSchemaRegistry(
            String sourceCollection) {
        CatalogTable catalogTable = structWithoutFieldsCatalogTable(sourceCollection);
        return schemaRegistry(sourceCollection, catalogTable);
    }

    private MilvusCdcCollectionSchemaRegistry structWithStoredChildFieldsSchemaRegistry(
            String sourceCollection) {
        CatalogTable catalogTable = structWithStoredChildFieldsCatalogTable(sourceCollection);
        return schemaRegistry(sourceCollection, catalogTable);
    }

    private MilvusCdcCollectionSchemaRegistry schemaRegistry(
            String sourceCollection, CatalogTable catalogTable) {
        MilvusCdcCollectionSchema schema =
                MilvusCdcCollectionSchema.builder()
                        .sourceDatabase("default")
                        .sourceCollection(sourceCollection)
                        .catalogTable(catalogTable)
                        .rowType(catalogTable.getSeaTunnelRowType())
                        .tableId(catalogTable.getTablePath().toString())
                        .primaryKeyField("id")
                        .primaryKeyIndex(0)
                        .enableDynamicField(hasDynamicFieldColumn(catalogTable))
                        .build();
        return new MilvusCdcCollectionSchemaRegistry(Collections.singletonList(schema));
    }

    private boolean hasDynamicFieldColumn(CatalogTable catalogTable) {
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        for (Column column : columns) {
            if (MILVUS_INTERNAL_DYNAMIC_FIELD.equals(column.getName())
                    && column.getOptions() != null
                    && Boolean.TRUE.equals(
                            column.getOptions().get(CommonOptions.METADATA.getName()))) {
                return true;
            }
        }
        return false;
    }

    private CatalogTable catalogTable(String collection) {
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                new ArrayList<>(
                                        java.util.Arrays.asList(
                                                PhysicalColumn.builder()
                                                        .name("id")
                                                        .dataType(BasicType.LONG_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("name")
                                                        .dataType(BasicType.STRING_TYPE)
                                                        .build())))
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id"), false))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("milvus", "default", null, collection),
                tableSchema,
                Collections.emptyMap(),
                new ArrayList<>(),
                "");
    }

    private CatalogTable vectorCatalogTable(String collection) {
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                new ArrayList<>(
                                        java.util.Arrays.asList(
                                                PhysicalColumn.builder()
                                                        .name("id")
                                                        .dataType(BasicType.LONG_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("vec")
                                                        .dataType(VectorType.VECTOR_FLOAT_TYPE)
                                                        .build())))
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id"), false))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("milvus", "default", null, collection),
                tableSchema,
                Collections.emptyMap(),
                new ArrayList<>(),
                "");
    }

    private CatalogTable binaryVectorCatalogTable(String collection) {
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                new ArrayList<>(
                                        java.util.Arrays.asList(
                                                PhysicalColumn.builder()
                                                        .name("id")
                                                        .dataType(BasicType.LONG_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("vec")
                                                        .dataType(VectorType.VECTOR_BINARY_TYPE)
                                                        .build())))
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id"), false))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("milvus", "default", null, collection),
                tableSchema,
                Collections.emptyMap(),
                new ArrayList<>(),
                "");
    }

    private CatalogTable nullableCatalogTable(String collection) {
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                new ArrayList<>(
                                        java.util.Arrays.asList(
                                                PhysicalColumn.builder()
                                                        .name("id")
                                                        .dataType(BasicType.LONG_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("vector")
                                                        .dataType(VectorType.VECTOR_FLOAT_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("opt_int")
                                                        .dataType(BasicType.LONG_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("opt_text")
                                                        .dataType(BasicType.STRING_TYPE)
                                                        .build())))
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id"), false))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("milvus", "default", null, collection),
                tableSchema,
                Collections.emptyMap(),
                new ArrayList<>(),
                "");
    }

    private CatalogTable nullableScalarCatalogTable(String collection) {
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                new ArrayList<>(
                                        java.util.Arrays.asList(
                                                PhysicalColumn.builder()
                                                        .name("id")
                                                        .dataType(BasicType.LONG_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("opt_int")
                                                        .dataType(BasicType.LONG_TYPE)
                                                        .build())))
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id"), false))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("milvus", "default", null, collection),
                tableSchema,
                Collections.emptyMap(),
                new ArrayList<>(),
                "");
    }

    private CatalogTable typedCatalogTable(String collection) {
        return typedCatalogTable(collection, 4);
    }

    private CatalogTable typedCatalogTable(String collection, int structEmbeddingDimension) {
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                new ArrayList<>(
                                        java.util.Arrays.asList(
                                                PhysicalColumn.builder()
                                                        .name("id")
                                                        .dataType(BasicType.LONG_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("json_col")
                                                        .dataType(BasicType.STRING_TYPE)
                                                        .options(jsonOptions())
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("tags")
                                                        .dataType(ArrayType.STRING_ARRAY_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("structs")
                                                        .dataType(ArrayType.STRING_ARRAY_TYPE)
                                                        .options(
                                                                structJsonOptions(
                                                                        structEmbeddingDimension))
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("sparse")
                                                        .dataType(
                                                                VectorType.VECTOR_SPARSE_FLOAT_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("int8_vec")
                                                        .dataType(VectorType.VECTOR_INT8_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("geom")
                                                        .dataType(GeometryType.GEOMETRY_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("ts")
                                                        .dataType(
                                                                LocalTimeType.LOCAL_DATE_TIME_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name(MILVUS_INTERNAL_DYNAMIC_FIELD)
                                                        .dataType(BasicType.STRING_TYPE)
                                                        .options(metadataOptions())
                                                        .build())))
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id"), false))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("milvus", "default", null, collection),
                tableSchema,
                Collections.emptyMap(),
                new ArrayList<>(),
                "");
    }

    private CatalogTable structCatalogTable(
            String collection,
            int structEmbeddingDimension,
            boolean includeScore,
            boolean includeEmbedding) {
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                new ArrayList<>(
                                        java.util.Arrays.asList(
                                                PhysicalColumn.builder()
                                                        .name("id")
                                                        .dataType(BasicType.LONG_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("structs")
                                                        .dataType(ArrayType.STRING_ARRAY_TYPE)
                                                        .options(
                                                                structJsonOptions(
                                                                        structEmbeddingDimension,
                                                                        includeScore,
                                                                        includeEmbedding))
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name(MILVUS_INTERNAL_DYNAMIC_FIELD)
                                                        .dataType(BasicType.STRING_TYPE)
                                                        .options(metadataOptions())
                                                        .build())))
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id"), false))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("milvus", "default", null, collection),
                tableSchema,
                Collections.emptyMap(),
                new ArrayList<>(),
                "");
    }

    private CatalogTable multipleStructCatalogTable(String collection) {
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                new ArrayList<>(
                                        java.util.Arrays.asList(
                                                PhysicalColumn.builder()
                                                        .name("id")
                                                        .dataType(BasicType.LONG_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("pa")
                                                        .dataType(ArrayType.STRING_ARRAY_TYPE)
                                                        .options(
                                                                structJsonOptions(
                                                                        CreateCollectionReq
                                                                                .FieldSchema
                                                                                .builder()
                                                                                .name("ca")
                                                                                .dataType(
                                                                                        io.milvus.v2
                                                                                                .common
                                                                                                .DataType
                                                                                                .VarChar)
                                                                                .maxLength(128)
                                                                                .build(),
                                                                        CreateCollectionReq
                                                                                .FieldSchema
                                                                                .builder()
                                                                                .name("cb")
                                                                                .dataType(
                                                                                        io.milvus.v2
                                                                                                .common
                                                                                                .DataType
                                                                                                .Int64)
                                                                                .build()))
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("pb")
                                                        .dataType(ArrayType.STRING_ARRAY_TYPE)
                                                        .options(
                                                                structJsonOptions(
                                                                        CreateCollectionReq
                                                                                .FieldSchema
                                                                                .builder()
                                                                                .name("ca2")
                                                                                .dataType(
                                                                                        io.milvus.v2
                                                                                                .common
                                                                                                .DataType
                                                                                                .VarChar)
                                                                                .maxLength(128)
                                                                                .build(),
                                                                        CreateCollectionReq
                                                                                .FieldSchema
                                                                                .builder()
                                                                                .name("cb2")
                                                                                .dataType(
                                                                                        io.milvus.v2
                                                                                                .common
                                                                                                .DataType
                                                                                                .Int64)
                                                                                .build()))
                                                        .build())))
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id"), false))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("milvus", "default", null, collection),
                tableSchema,
                Collections.emptyMap(),
                new ArrayList<>(),
                "");
    }

    private CatalogTable structWithoutFieldsCatalogTable(String collection) {
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                new ArrayList<>(
                                        java.util.Arrays.asList(
                                                PhysicalColumn.builder()
                                                        .name("id")
                                                        .dataType(BasicType.LONG_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("structs")
                                                        .dataType(ArrayType.STRING_ARRAY_TYPE)
                                                        .options(jsonOptions())
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name(MILVUS_INTERNAL_DYNAMIC_FIELD)
                                                        .dataType(BasicType.STRING_TYPE)
                                                        .options(metadataOptions())
                                                        .build())))
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id"), false))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("milvus", "default", null, collection),
                tableSchema,
                Collections.emptyMap(),
                new ArrayList<>(),
                "");
    }

    private CatalogTable structWithStoredChildFieldsCatalogTable(String collection) {
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                new ArrayList<>(
                                        java.util.Arrays.asList(
                                                PhysicalColumn.builder()
                                                        .name("id")
                                                        .dataType(BasicType.LONG_TYPE)
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name("structs")
                                                        .dataType(ArrayType.STRING_ARRAY_TYPE)
                                                        .options(
                                                                structJsonOptions(
                                                                        CreateCollectionReq
                                                                                .FieldSchema
                                                                                .builder()
                                                                                .name(
                                                                                        "structs[name]")
                                                                                .dataType(
                                                                                        io.milvus.v2
                                                                                                .common
                                                                                                .DataType
                                                                                                .VarChar)
                                                                                .maxLength(128)
                                                                                .build(),
                                                                        CreateCollectionReq
                                                                                .FieldSchema
                                                                                .builder()
                                                                                .name(
                                                                                        "structs[score]")
                                                                                .dataType(
                                                                                        io.milvus.v2
                                                                                                .common
                                                                                                .DataType
                                                                                                .Int64)
                                                                                .build()))
                                                        .build(),
                                                PhysicalColumn.builder()
                                                        .name(MILVUS_INTERNAL_DYNAMIC_FIELD)
                                                        .dataType(BasicType.STRING_TYPE)
                                                        .options(metadataOptions())
                                                        .build())))
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id"), false))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("milvus", "default", null, collection),
                tableSchema,
                Collections.emptyMap(),
                new ArrayList<>(),
                "");
    }

    private Map<String, Object> jsonOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put(CommonOptions.JSON.getName(), true);
        return options;
    }

    private Map<String, Object> structJsonOptions(int structEmbeddingDimension) {
        return structJsonOptions(structEmbeddingDimension, true, true);
    }

    private Map<String, Object> structJsonOptions(CreateCollectionReq.FieldSchema... structFields) {
        Map<String, Object> options = jsonOptions();
        options.put("elementType", io.milvus.v2.common.DataType.Struct.getCode());
        options.put("struct_fields", new Gson().toJson(java.util.Arrays.asList(structFields)));
        return options;
    }

    private Map<String, Object> structJsonOptions(
            int structEmbeddingDimension, boolean includeScore, boolean includeEmbedding) {
        Map<String, Object> options = jsonOptions();
        options.put("elementType", io.milvus.v2.common.DataType.Struct.getCode());
        List<CreateCollectionReq.FieldSchema> structFields = new ArrayList<>();
        structFields.add(
                CreateCollectionReq.FieldSchema.builder()
                        .name("name")
                        .dataType(io.milvus.v2.common.DataType.VarChar)
                        .maxLength(128)
                        .build());
        if (includeScore) {
            structFields.add(
                    CreateCollectionReq.FieldSchema.builder()
                            .name("score")
                            .dataType(io.milvus.v2.common.DataType.Int64)
                            .build());
        }
        if (includeEmbedding) {
            structFields.add(
                    CreateCollectionReq.FieldSchema.builder()
                            .name("embedding")
                            .dataType(io.milvus.v2.common.DataType.FloatVector)
                            .dimension(structEmbeddingDimension)
                            .build());
        }
        options.put("struct_fields", new Gson().toJson(structFields));
        return options;
    }

    private Map<String, Object> metadataOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put(CommonOptions.METADATA.getName(), true);
        return options;
    }

    private static long hybridTimestamp(long physicalTimestampMs) {
        return physicalTimestampMs << 18;
    }
}
