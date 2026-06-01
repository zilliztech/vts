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
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.config.MilvusCdcMessageType;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.config.MilvusCdcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.config.MilvusCdcStartupMode;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.offset.MilvusCdcOffset;

import io.milvus.v2.client.ConnectConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public class MilvusCdcSourceConfigParser {

    private static final String PCHANNEL = "pchannel";
    private static final String START = "start";
    private static final String RESUME_MESSAGE_ID = "resume_message_id";
    private static final String CONSUMED_MESSAGE_ID = "consumed_message_id";
    private static final String LEGACY_MESSAGE_ID = "message_id";
    private static final String LEGACY_RESUME_MESSAGE_ID = "last_confirmed_message_id";
    private static final String TIMETICK = "timetick";
    private static final String WAL_NAME = "wal_name";
    private static final Pattern VCHANNEL_SUFFIX = Pattern.compile(".*_\\d+v\\d+$");

    public static List<MilvusCdcSplit> parseChannelPositions(ReadonlyConfig config) {
        MilvusCdcStartupMode startupMode = config.get(MilvusCdcSourceConfig.STARTUP_MODE);
        if (startupMode != MilvusCdcStartupMode.CDC) {
            throw new IllegalArgumentException(
                    String.format(
                            "Milvus-CDC currently supports startup_mode=CDC only, but got %s",
                            startupMode.name().toLowerCase(Locale.ROOT)));
        }

        List<Map<String, Object>> channelPositions =
                config.get(MilvusCdcSourceConfig.CHANNEL_POSITIONS);
        if (channelPositions == null || channelPositions.isEmpty()) {
            throw new IllegalArgumentException(
                    "Milvus-CDC requires at least one channel_positions entry.");
        }

        List<MilvusCdcSplit> splits = new ArrayList<>(channelPositions.size());
        Set<String> seenPchannels = new HashSet<>();
        for (Map<String, Object> channelPosition : channelPositions) {
            String pchannel = requiredPchannel(channelPosition);
            if (!seenPchannels.add(pchannel)) {
                throw new IllegalArgumentException(
                        "Duplicate pchannel in channel_positions: " + pchannel);
            }
            MilvusCdcOffset startOffset = requiredStartOffset(channelPosition);
            splits.add(
                    MilvusCdcSplit.builder()
                            .splitId(pchannel)
                            .pchannel(pchannel)
                            .startOffset(startOffset)
                            .build());
        }
        return splits;
    }

    public static Set<MilvusCdcMessageType> parseMessageTypes(ReadonlyConfig config) {
        List<String> configuredTypes = config.get(MilvusCdcSourceConfig.MESSAGE_TYPES);
        if (configuredTypes == null || configuredTypes.isEmpty()) {
            throw new IllegalArgumentException(
                    "Milvus-CDC message_types requires at least one type.");
        }

        Set<MilvusCdcMessageType> messageTypes = EnumSet.noneOf(MilvusCdcMessageType.class);
        for (String configuredType : configuredTypes) {
            MilvusCdcMessageType messageType =
                    MilvusCdcMessageType.parse(configuredType)
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    String.format(
                                                            "Milvus-CDC currently supports message_types "
                                                                    + "insert and delete only, but got %s.",
                                                            configuredType)));
            messageTypes.add(messageType);
        }
        return messageTypes;
    }

    public static List<MilvusCdcSourceTable> parseDatabaseCollections(ReadonlyConfig config) {
        Map<String, List<String>> databaseCollections =
                config.get(MilvusCdcSourceConfig.DATABASE_COLLECTIONS);
        if (databaseCollections == null || databaseCollections.isEmpty()) {
            throw new IllegalArgumentException(
                    "Milvus-CDC requires at least one database_collections entry.");
        }

        List<MilvusCdcSourceTable> sourceTables = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : databaseCollections.entrySet()) {
            String database = requireNonBlank(entry.getKey(), "database_collections database");
            List<String> collections = entry.getValue();
            if (collections == null || collections.isEmpty()) {
                throw new IllegalArgumentException(
                        "database_collections." + database + " requires at least one collection.");
            }
            Set<String> seenCollections = new HashSet<>();
            for (String configuredCollection : collections) {
                String collection =
                        requireNonBlank(
                                configuredCollection,
                                "database_collections." + database + " collection");
                if ("*".equals(collection)) {
                    throw new IllegalArgumentException(
                            "database_collections."
                                    + database
                                    + " does not support wildcard collection capture yet because "
                                    + "dynamic schema discovery is not supported.");
                }
                if (!seenCollections.add(collection)) {
                    throw new IllegalArgumentException(
                            "Duplicate collection in database_collections."
                                    + database
                                    + ": "
                                    + collection);
                }
                sourceTables.add(
                        MilvusCdcSourceTable.builder()
                                .database(database)
                                .collection(collection)
                                .build());
            }
        }
        return Collections.unmodifiableList(sourceTables);
    }

    public static ConnectConfig parseConnectConfig(ReadonlyConfig config) {
        ConnectConfig connectConfig =
                ConnectConfig.builder()
                        .uri(config.get(MilvusCdcSourceConfig.URL))
                        .token(config.get(MilvusCdcSourceConfig.TOKEN))
                        .dbName("default")
                        .connectTimeoutMs(30000)
                        .build();
        config.getOptional(MilvusCdcSourceConfig.CLIENT_PEM_PATH)
                .ifPresent(connectConfig::setClientPemPath);
        config.getOptional(MilvusCdcSourceConfig.CLIENT_KEY_PATH)
                .ifPresent(connectConfig::setClientKeyPath);
        config.getOptional(MilvusCdcSourceConfig.CA_PEM_PATH)
                .ifPresent(connectConfig::setCaPemPath);
        config.getOptional(MilvusCdcSourceConfig.SERVER_NAME)
                .ifPresent(connectConfig::setServerName);
        return connectConfig;
    }

    private static MilvusCdcOffset requiredStartOffset(Map<String, Object> parent) {
        MilvusCdcOffset offset = optionalOffset(parent, START);
        if (offset == null || !offset.hasResumeMessageId()) {
            throw new IllegalArgumentException(
                    "channel_positions.start requires resume_message_id. "
                            + "Milvus DumpMessages does not accept timetick-only starts. "
                            + "For compatibility, message_id is also accepted as resume_message_id.");
        }
        if (offset.getWalName() == null || offset.getWalName().isEmpty()) {
            throw new IllegalArgumentException(
                    "channel_positions.start requires wal_name when resume_message_id is set.");
        }
        if (!offset.hasTimetick()) {
            throw new IllegalArgumentException(
                    "channel_positions.start requires timetick when resume_message_id is set.");
        }
        if (!offset.hasConsumedMessageId()) {
            offset.setConsumedMessageId(offset.getResumeMessageId());
        }
        offset.validateMessageId();
        return offset;
    }

    private static String requiredPchannel(Map<String, Object> channelPosition) {
        String pchannel = requiredString(channelPosition, PCHANNEL);
        if (VCHANNEL_SUFFIX.matcher(pchannel).matches()) {
            throw new IllegalArgumentException(
                    "channel_positions.pchannel must be a physical Milvus pchannel, "
                            + "for example by-dev-rootcoord-dml_0. "
                            + "Do not use a virtual channel with collection id suffix such as "
                            + pchannel
                            + ".");
        }
        return pchannel;
    }

    @SuppressWarnings("unchecked")
    private static MilvusCdcOffset optionalOffset(Map<String, Object> parent, String key) {
        Object value = parent.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException(
                    String.format("channel_positions.%s must be an object.", key));
        }
        Map<String, Object> map = (Map<String, Object>) value;
        Optional<String> legacyMessageId = optionalString(map, LEGACY_MESSAGE_ID);
        Optional<String> legacyResumeMessageId = optionalString(map, LEGACY_RESUME_MESSAGE_ID);
        String resumeMessageId =
                optionalString(map, RESUME_MESSAGE_ID)
                        .orElseGet(
                                () ->
                                        legacyResumeMessageId.orElseGet(
                                                () -> legacyMessageId.orElse(null)));
        String consumedMessageId =
                optionalString(map, CONSUMED_MESSAGE_ID)
                        .orElseGet(
                                () ->
                                        legacyResumeMessageId.isPresent()
                                                ? legacyMessageId.orElse(null)
                                                : null);
        return MilvusCdcOffset.builder()
                .resumeMessageId(resumeMessageId)
                .consumedMessageId(consumedMessageId)
                .walName(optionalString(map, WAL_NAME).orElse(null))
                .timetick(optionalLong(map, TIMETICK).orElse(null))
                .build();
    }

    private static String requiredString(Map<String, Object> map, String key) {
        return requiredString(map, key, "channel_positions");
    }

    private static String requiredString(Map<String, Object> map, String key, String path) {
        return optionalString(map, key)
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format("%s.%s is required.", path, key)));
    }

    private static String requireNonBlank(String value, String path) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(path + " must not be empty.");
        }
        if (!value.equals(value.trim())) {
            throw new IllegalArgumentException(
                    path + " must not contain leading or trailing whitespace.");
        }
        return value;
    }

    private static Optional<String> optionalString(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return Optional.empty();
        }
        String stringValue = String.valueOf(value);
        if (stringValue.trim().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(stringValue);
    }

    private static Optional<Long> optionalLong(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return Optional.empty();
        }
        if (value instanceof Number) {
            return Optional.of(((Number) value).longValue());
        }
        return Optional.of(Long.parseLong(String.valueOf(value)));
    }
}
