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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class MilvusCdcSourceConfigParserTest {

    @Test
    void parseChannelPositions() {
        Map<String, Object> start = new HashMap<>();
        start.put("resume_message_id", "message-1");
        start.put("wal_name", "Pulsar");
        start.put("timetick", 99L);

        Map<String, Object> channel = new HashMap<>();
        channel.put("pchannel", "by-dev-rootcoord-dml_0");
        channel.put("start", start);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("startup_mode", "cdc");
        configMap.put("channel_positions", Collections.singletonList(channel));

        List<MilvusCdcSplit> splits =
                MilvusCdcSourceConfigParser.parseChannelPositions(
                        ReadonlyConfig.fromMap(configMap));

        Assertions.assertEquals(1, splits.size());
        MilvusCdcSplit split = splits.get(0);
        Assertions.assertEquals("by-dev-rootcoord-dml_0", split.getPchannel());
        Assertions.assertEquals("message-1", split.getStartOffset().getResumeMessageId());
        Assertions.assertEquals("message-1", split.getStartOffset().getConsumedMessageId());
        Assertions.assertEquals("Pulsar", split.getStartOffset().getWalName());
        Assertions.assertEquals(99L, split.getStartOffset().getTimetick());
    }

    @Test
    void rejectDuplicatePchannels() {
        Map<String, Object> start = new HashMap<>();
        start.put("resume_message_id", "message-1");
        start.put("wal_name", "Pulsar");
        start.put("timetick", 99L);

        Map<String, Object> firstChannel = new HashMap<>();
        firstChannel.put("pchannel", "by-dev-rootcoord-dml_0");
        firstChannel.put("start", start);

        Map<String, Object> secondChannel = new HashMap<>();
        secondChannel.put("pchannel", "by-dev-rootcoord-dml_0");
        secondChannel.put("start", start);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("channel_positions", Arrays.asList(firstChannel, secondChannel));

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MilvusCdcSourceConfigParser.parseChannelPositions(
                                        ReadonlyConfig.fromMap(configMap)));

        Assertions.assertTrue(exception.getMessage().contains("Duplicate pchannel"));
    }

    @Test
    void parseLegacyMessageIdAsResumeMessageId() {
        Map<String, Object> start = new HashMap<>();
        start.put("message_id", "message-1");
        start.put("wal_name", "Pulsar");
        start.put("timetick", 99L);

        Map<String, Object> channel = new HashMap<>();
        channel.put("pchannel", "by-dev-rootcoord-dml_0");
        channel.put("start", start);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("startup_mode", "cdc");
        configMap.put("channel_positions", Collections.singletonList(channel));

        List<MilvusCdcSplit> splits =
                MilvusCdcSourceConfigParser.parseChannelPositions(
                        ReadonlyConfig.fromMap(configMap));

        Assertions.assertEquals("message-1", splits.get(0).getStartOffset().getResumeMessageId());
        Assertions.assertEquals("message-1", splits.get(0).getStartOffset().getConsumedMessageId());
    }

    @Test
    void parseDatabaseCollections() {
        Map<String, Object> configMap = new HashMap<>();
        Map<String, List<String>> databaseCollections = new LinkedHashMap<>();
        databaseCollections.put("source_db_a", Arrays.asList("collection_a", "collection_b"));
        databaseCollections.put("source_db_b", Collections.singletonList("collection_a"));
        configMap.put("database_collections", databaseCollections);

        List<MilvusCdcSourceTable> sourceTables =
                MilvusCdcSourceConfigParser.parseDatabaseCollections(
                        ReadonlyConfig.fromMap(configMap));

        Assertions.assertEquals(3, sourceTables.size());
        Assertions.assertEquals("source_db_a", sourceTables.get(0).getDatabase());
        Assertions.assertEquals("collection_a", sourceTables.get(0).getCollection());
        Assertions.assertEquals("source_db_a", sourceTables.get(1).getDatabase());
        Assertions.assertEquals("collection_b", sourceTables.get(1).getCollection());
        Assertions.assertEquals("source_db_b", sourceTables.get(2).getDatabase());
        Assertions.assertEquals("collection_a", sourceTables.get(2).getCollection());
    }

    @Test
    void rejectEmptyDatabaseCollections() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("database_collections", Collections.emptyMap());

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MilvusCdcSourceConfigParser.parseDatabaseCollections(
                                        ReadonlyConfig.fromMap(configMap)));

        Assertions.assertTrue(exception.getMessage().contains("database_collections"));
    }

    @Test
    void rejectBlankDatabaseCollectionsDatabase() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                "database_collections",
                Collections.singletonMap("  ", Collections.singletonList("collection_a")));

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MilvusCdcSourceConfigParser.parseDatabaseCollections(
                                        ReadonlyConfig.fromMap(configMap)));

        Assertions.assertTrue(exception.getMessage().contains("database"));
    }

    @Test
    void rejectWhitespaceDatabaseCollectionsDatabase() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                "database_collections",
                Collections.singletonMap(" source_db", Collections.singletonList("collection_a")));

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MilvusCdcSourceConfigParser.parseDatabaseCollections(
                                        ReadonlyConfig.fromMap(configMap)));

        Assertions.assertTrue(exception.getMessage().contains("database"));
        Assertions.assertTrue(exception.getMessage().contains("whitespace"));
    }

    @Test
    void rejectBlankDatabaseCollectionsCollection() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                "database_collections",
                Collections.singletonMap("source_db", Collections.singletonList("  ")));

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MilvusCdcSourceConfigParser.parseDatabaseCollections(
                                        ReadonlyConfig.fromMap(configMap)));

        Assertions.assertTrue(exception.getMessage().contains("collection"));
    }

    @Test
    void rejectWhitespaceDatabaseCollectionsCollection() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                "database_collections",
                Collections.singletonMap("source_db", Collections.singletonList("collection_a ")));

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MilvusCdcSourceConfigParser.parseDatabaseCollections(
                                        ReadonlyConfig.fromMap(configMap)));

        Assertions.assertTrue(exception.getMessage().contains("collection"));
        Assertions.assertTrue(exception.getMessage().contains("whitespace"));
    }

    @Test
    void rejectWildcardDatabaseCollections() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                "database_collections",
                Collections.singletonMap("source_db", Collections.singletonList("*")));

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MilvusCdcSourceConfigParser.parseDatabaseCollections(
                                        ReadonlyConfig.fromMap(configMap)));

        Assertions.assertTrue(exception.getMessage().contains("wildcard"));
        Assertions.assertTrue(exception.getMessage().contains("dynamic schema discovery"));
    }

    @Test
    void rejectDuplicateDatabaseCollections() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                "database_collections",
                Collections.singletonMap(
                        "source_db", Arrays.asList("collection_a", "collection_a")));

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MilvusCdcSourceConfigParser.parseDatabaseCollections(
                                        ReadonlyConfig.fromMap(configMap)));

        Assertions.assertTrue(exception.getMessage().contains("Duplicate collection"));
    }

    @Test
    void rejectMissingStartPosition() {
        Map<String, Object> channel = new HashMap<>();
        channel.put("pchannel", "p0");
        channel.put("start", Collections.emptyMap());

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("channel_positions", Collections.singletonList(channel));

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        MilvusCdcSourceConfigParser.parseChannelPositions(
                                ReadonlyConfig.fromMap(configMap)));
    }

    @Test
    void rejectTimetickOnlyStartPosition() {
        Map<String, Object> start = new HashMap<>();
        start.put("timetick", 100L);

        Map<String, Object> channel = new HashMap<>();
        channel.put("pchannel", "p0");
        channel.put("start", start);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("channel_positions", Collections.singletonList(channel));

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        MilvusCdcSourceConfigParser.parseChannelPositions(
                                ReadonlyConfig.fromMap(configMap)));
    }

    @Test
    void rejectStartMessageIdWithoutWalName() {
        Map<String, Object> start = new HashMap<>();
        start.put("resume_message_id", "message-1");
        start.put("timetick", 99L);

        Map<String, Object> channel = new HashMap<>();
        channel.put("pchannel", "p0");
        channel.put("start", start);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("channel_positions", Collections.singletonList(channel));

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        MilvusCdcSourceConfigParser.parseChannelPositions(
                                ReadonlyConfig.fromMap(configMap)));
    }

    @Test
    void rejectStartMessageIdWithoutTimetick() {
        Map<String, Object> start = new HashMap<>();
        start.put("resume_message_id", "message-1");
        start.put("wal_name", "Pulsar");

        Map<String, Object> channel = new HashMap<>();
        channel.put("pchannel", "p0");
        channel.put("start", start);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("channel_positions", Collections.singletonList(channel));

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        MilvusCdcSourceConfigParser.parseChannelPositions(
                                ReadonlyConfig.fromMap(configMap)));
    }

    @Test
    void rejectInvalidRocksMqStartMessageId() {
        Map<String, Object> start = new HashMap<>();
        String invalidMessageId = "R" + (char) 0 + "4)6" + (char) 0xb7 + "y" + (char) 6;
        start.put("resume_message_id", invalidMessageId);
        start.put("wal_name", "RocksMQ");
        start.put("timetick", 99L);

        Map<String, Object> channel = new HashMap<>();
        channel.put("pchannel", "p0");
        channel.put("start", start);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("channel_positions", Collections.singletonList(channel));

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MilvusCdcSourceConfigParser.parseChannelPositions(
                                        ReadonlyConfig.fromMap(configMap)));

        Assertions.assertTrue(exception.getMessage().contains("RocksMQ message_id"));
    }

    @Test
    void rejectVirtualChannelAsPchannel() {
        Map<String, Object> start = new HashMap<>();
        start.put("resume_message_id", "-1");
        start.put("wal_name", "RocksMQ");
        start.put("timetick", 0L);

        Map<String, Object> channel = new HashMap<>();
        channel.put("pchannel", "by-dev-rootcoord-dml_0_466605479545411016v0");
        channel.put("start", start);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("channel_positions", Collections.singletonList(channel));

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MilvusCdcSourceConfigParser.parseChannelPositions(
                                        ReadonlyConfig.fromMap(configMap)));

        Assertions.assertTrue(exception.getMessage().contains("physical Milvus pchannel"));
    }

    @Test
    void rejectUnsupportedMessageType() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("message_types", Collections.singletonList("flush"));

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        MilvusCdcSourceConfigParser.parseMessageTypes(
                                ReadonlyConfig.fromMap(configMap)));
    }
}
