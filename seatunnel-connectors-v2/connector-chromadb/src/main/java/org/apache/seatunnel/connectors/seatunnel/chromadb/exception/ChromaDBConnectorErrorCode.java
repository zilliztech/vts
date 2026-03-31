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

package org.apache.seatunnel.connectors.seatunnel.chromadb.exception;

import lombok.Getter;
import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

@Getter
public enum ChromaDBConnectorErrorCode implements SeaTunnelErrorCode {
    CONNECT_FAILED("CHROMADB-01", "Failed to connect to ChromaDB server"),
    COLLECTION_NOT_FOUND("CHROMADB-02", "ChromaDB collection not found or empty"),
    READ_DATA_FAIL("CHROMADB-03", "Failed to read data from ChromaDB");

    private final String code;
    private final String description;

    ChromaDBConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }
}
