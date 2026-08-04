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

import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.offset.MilvusCdcOffset;

public final class MilvusCdcRowMetadata {

    public static final String EVENT_TIMESTAMP = "MilvusCdcEventTimestamp";
    public static final String EVENT_TIMESTAMP_MS = "MilvusCdcEventTimestampMs";
    public static final String CURRENT_MESSAGE_ID = "MilvusCdcCurrentMessageId";
    public static final String RESUME_MESSAGE_ID = "MilvusCdcResumeMessageId";
    public static final String MESSAGE_END = "MilvusCdcMessageEnd";
    public static final String WAL_NAME = "MilvusCdcWalName";
    public static final String TIMETICK = "MilvusCdcTimetick";

    private static final int HYBRID_TIMESTAMP_LOGICAL_BITS = 18;

    private MilvusCdcRowMetadata() {}

    public static void setEventMetadata(
            SeaTunnelRow row, Long eventTimestamp, long receiveTimestampMs) {
        if (eventTimestamp == null) {
            return;
        }
        long eventTimestampMs = toPhysicalMillis(eventTimestamp);
        row.getOptions().put(EVENT_TIMESTAMP, eventTimestamp);
        row.getOptions().put(EVENT_TIMESTAMP_MS, eventTimestampMs);
        MetadataUtil.setEventTime(row, eventTimestampMs);
        MetadataUtil.setDelay(row, receiveTimestampMs - eventTimestampMs);
    }

    public static void setMessageMetadata(SeaTunnelRow row, MilvusCdcOffset offset) {
        if (offset == null) {
            return;
        }
        if (offset.getConsumedMessageId() != null) {
            row.getOptions().put(CURRENT_MESSAGE_ID, offset.getConsumedMessageId());
        }
        if (offset.getResumeMessageId() != null) {
            row.getOptions().put(RESUME_MESSAGE_ID, offset.getResumeMessageId());
        }
        if (offset.getWalName() != null) {
            row.getOptions().put(WAL_NAME, offset.getWalName());
        }
        if (offset.getTimetick() != null) {
            row.getOptions().put(TIMETICK, offset.getTimetick());
        }
    }

    public static void markMessageEnd(SeaTunnelRow row) {
        row.getOptions().put(MESSAGE_END, true);
    }

    public static Long toPhysicalMillis(Long hybridTimestamp) {
        if (hybridTimestamp == null) {
            return null;
        }
        return hybridTimestamp >>> HYBRID_TIMESTAMP_LOGICAL_BITS;
    }

    public static Long eventTimestamp(SeaTunnelRow row) {
        return longOption(row, EVENT_TIMESTAMP);
    }

    public static Long eventTimestampMs(SeaTunnelRow row) {
        return longOption(row, EVENT_TIMESTAMP_MS);
    }

    public static String currentMessageId(SeaTunnelRow row) {
        return stringOption(row, CURRENT_MESSAGE_ID);
    }

    public static String resumeMessageId(SeaTunnelRow row) {
        return stringOption(row, RESUME_MESSAGE_ID);
    }

    public static String walName(SeaTunnelRow row) {
        return stringOption(row, WAL_NAME);
    }

    public static Long timetick(SeaTunnelRow row) {
        return longOption(row, TIMETICK);
    }

    private static String stringOption(SeaTunnelRow row, String key) {
        Object value = row.getOptions().get(key);
        return value == null ? null : value.toString();
    }

    private static Long longOption(SeaTunnelRow row, String key) {
        Object value = row.getOptions().get(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value != null) {
            return Long.parseLong(value.toString());
        }
        return null;
    }
}
