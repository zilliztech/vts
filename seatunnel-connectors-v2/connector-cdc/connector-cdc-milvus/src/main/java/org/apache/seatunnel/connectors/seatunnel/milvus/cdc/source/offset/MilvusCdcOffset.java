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

package org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.offset;

import io.milvus.grpc.MessageID;
import io.milvus.grpc.WALName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MilvusCdcOffset implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int ROCKSMQ_MESSAGE_ID_RADIX = 36;

    private String walName;
    private String resumeMessageId;
    private String consumedMessageId;
    private Long timetick;

    public static MilvusCdcOffset fromMessageId(MessageID messageID) {
        if (messageID == null) {
            return null;
        }
        return MilvusCdcOffset.builder()
                .walName(messageID.getWALName().name())
                .consumedMessageId(messageID.getId())
                .build();
    }

    public boolean hasResumeMessageId() {
        return resumeMessageId != null && !resumeMessageId.isEmpty();
    }

    public boolean hasConsumedMessageId() {
        return consumedMessageId != null && !consumedMessageId.isEmpty();
    }

    public boolean hasTimetick() {
        return timetick != null;
    }

    public MessageID toResumeMessageId() {
        if (!hasResumeMessageId()) {
            throw new IllegalStateException("Milvus CDC resume_message_id is required.");
        }
        return toMessageId(resumeMessageId);
    }

    public MessageID toConsumedMessageId() {
        if (!hasConsumedMessageId()) {
            throw new IllegalStateException("Milvus CDC consumed_message_id is required.");
        }
        return toMessageId(consumedMessageId);
    }

    private MessageID toMessageId(String messageId) {
        MessageID.Builder builder = MessageID.newBuilder().setId(messageId);
        if (walName != null && !walName.isEmpty()) {
            WALName resolvedWalName = resolveWalName(walName);
            validateMessageId(resolvedWalName, messageId);
            builder.setWALName(resolvedWalName);
        }
        return builder.build();
    }

    public void validateMessageId() {
        if (walName == null || walName.isEmpty()) {
            return;
        }
        WALName resolvedWalName = resolveWalName(walName);
        if (hasResumeMessageId()) {
            validateMessageId(resolvedWalName, resumeMessageId);
        }
        if (hasConsumedMessageId()) {
            validateMessageId(resolvedWalName, consumedMessageId);
        }
    }

    private WALName resolveWalName(String walName) {
        for (WALName value : WALName.values()) {
            if (value.name().equalsIgnoreCase(walName)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown Milvus WAL name: " + walName);
    }

    private void validateMessageId(WALName resolvedWalName, String messageId) {
        if (resolvedWalName != WALName.RocksMQ) {
            return;
        }
        try {
            Long.parseLong(messageId, ROCKSMQ_MESSAGE_ID_RADIX);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid Milvus RocksMQ message_id. "
                            + "RocksMQ expects a base36-encoded int64 text offset, for example -1. "
                            + "Do not copy escaped binary msgID bytes from Milvus logs.",
                    e);
        }
    }
}
