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

import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.config.MilvusCdcMessageType;

import lombok.Builder;
import lombok.Getter;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Builder
@Getter
class MilvusCdcDecodedRecord {
    private final MilvusCdcMessageType messageType;
    private final String database;
    private final String collection;
    private final String partition;
    private final int rowIndex;
    private final Long rowId;
    private final Long eventTimestamp;
    private final Object primaryKey;
    private final Map<String, Object> data;

    @Builder.Default private final Set<String> staticFieldNames = Collections.emptySet();
    private final boolean dynamicFieldsPresent;
}
