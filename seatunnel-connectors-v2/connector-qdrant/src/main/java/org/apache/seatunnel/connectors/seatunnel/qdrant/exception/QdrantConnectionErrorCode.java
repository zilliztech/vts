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

package org.apache.seatunnel.connectors.seatunnel.qdrant.exception;

import lombok.Getter;
import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

@Getter
public enum QdrantConnectionErrorCode implements SeaTunnelErrorCode {
    FAILED_CONNECT_QDRANT("QDRANT-01", "Failed to connect to Qdrant"),
    EMPTY_COLLECTION("QDRANT-02", "No data in collection" );

    private final String code;
    private final String description;

    QdrantConnectionErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }
}
