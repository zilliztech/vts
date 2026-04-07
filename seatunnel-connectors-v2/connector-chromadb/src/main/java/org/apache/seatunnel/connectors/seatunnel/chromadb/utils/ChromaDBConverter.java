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

package org.apache.seatunnel.connectors.seatunnel.chromadb.utils;

import com.google.gson.Gson;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.chromadb.config.ChromaDBSourceConfig;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ChromaDBConverter {

    private static final Gson GSON = new Gson();

    public static SeaTunnelRow convert(
            TableSchema tableSchema,
            String id,
            List<Float> embedding,
            String document,
            String uri,
            Map<String, Object> metadata) {

        if (id == null) {
            throw new IllegalArgumentException("ChromaDB record id must not be null");
        }

        SeaTunnelRowType rowType = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[rowType.getTotalFields()];
        String[] fieldNames = rowType.getFieldNames();

        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String fieldName = fieldNames[i];

            if (Objects.equals(fieldName, ChromaDBSourceConfig.FIELD_ID)) {
                fields[i] = id;
            } else if (Objects.equals(fieldName, CommonOptions.METADATA.getName())) {
                fields[i] = metadata != null ? GSON.toJson(metadata) : "{}";
            } else if (Objects.equals(fieldName, ChromaDBSourceConfig.FIELD_DOCUMENT)) {
                fields[i] = document;
            } else if (Objects.equals(fieldName, ChromaDBSourceConfig.FIELD_URI)) {
                fields[i] = uri;
            } else if (Objects.equals(fieldName, ChromaDBSourceConfig.FIELD_EMBEDDING)) {
                if (embedding != null && !embedding.isEmpty()) {
                    fields[i] = BufferUtils.toByteBuffer(embedding.toArray(new Float[0]));
                }
            }
        }

        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setRowKind(RowKind.INSERT);
        return row;
    }
}
