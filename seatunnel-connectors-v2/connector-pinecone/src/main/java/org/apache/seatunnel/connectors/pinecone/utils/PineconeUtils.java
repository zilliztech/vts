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

package org.apache.seatunnel.connectors.pinecone.utils;

import io.pinecone.clients.Index;
import io.pinecone.clients.Pinecone;
import io.pinecone.proto.DescribeIndexStatsResponse;
import io.pinecone.proto.FetchResponse;
import io.pinecone.proto.ListItem;
import io.pinecone.proto.ListResponse;
import io.pinecone.proto.NamespaceSummary;
import io.pinecone.proto.Vector;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.VectorType.VECTOR_FLOAT_TYPE;
import static org.apache.seatunnel.api.table.type.VectorType.VECTOR_SPARSE_FLOAT_TYPE;
import org.apache.seatunnel.common.constants.CommonOptions;
import static org.apache.seatunnel.connectors.pinecone.config.PineconeSourceConfig.API_KEY;
import static org.apache.seatunnel.connectors.pinecone.config.PineconeSourceConfig.INDEX;
import org.openapitools.control.client.model.IndexModel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PineconeUtils {
    private ReadonlyConfig config;
    Map<TablePath, CatalogTable> sourceTables;

    public PineconeUtils(ReadonlyConfig config) {
        this.config = config;
        this.sourceTables = new HashMap<>();
    }

    public Map<TablePath, CatalogTable> getSourceTables() {
        Pinecone pinecone = new Pinecone.Builder(config.get(API_KEY)).build();
        String indexName = config.get(INDEX);
        IndexModel indexMetadata = pinecone.describeIndex(indexName);
        TablePath tablePath = TablePath.of("default", indexName);

        Index index = pinecone.getIndexConnection(indexName);

        Vector vector = null;

        DescribeIndexStatsResponse describeIndexStatsResponse = index.describeIndexStats();
        Map<String, NamespaceSummary> namespaceSummaryMap = describeIndexStatsResponse.getNamespacesMap();
        for(Map.Entry<String, NamespaceSummary> entry : namespaceSummaryMap.entrySet()) {
            NamespaceSummary namespaceSummary = entry.getValue();
            if (namespaceSummary.getVectorCount() != 0) {
                ListResponse listResponse = index.list(entry.getKey(), 10);
                List<ListItem> vectorsList = listResponse.getVectorsList();
                List<String> ids = vectorsList.stream().map(ListItem::getId).collect(Collectors.toList());
                if (ids.isEmpty()) {
                    // no data in the index
                    return sourceTables;
                }
                FetchResponse fetchResponse = index.fetch(ids, entry.getKey());
                Map<String, Vector> vectorMap = fetchResponse.getVectorsMap();
                vector = vectorMap.entrySet().stream().iterator().next().getValue();
                break;
            }
        }
        if(vector == null) {
            // no data in the index
            return sourceTables;
        }

        List<Column> columns = new ArrayList<>();

        PhysicalColumn idColumn = PhysicalColumn.builder()
                .name("id")
                .dataType(STRING_TYPE)
                .build();

        columns.add(idColumn);

        Map<String, Object> options = new HashMap<>();

        options.put(CommonOptions.METADATA.getName(), true);
        PhysicalColumn dynamicColumn = PhysicalColumn.builder()
                .name(CommonOptions.METADATA.getName())
                .dataType(STRING_TYPE)
                .options(options)
                .build();
        columns.add(dynamicColumn);

        if(!vector.getValuesList().isEmpty()) {
            PhysicalColumn vectorColumn = PhysicalColumn.builder()
                    .name("vector")
                    .dataType(VECTOR_FLOAT_TYPE)
                    .scale(indexMetadata.getDimension())
                    .build();
            columns.add(vectorColumn);
        }
        if (!vector.getSparseValues().getIndicesList().isEmpty()) {
            PhysicalColumn sparseVectorColumn = PhysicalColumn.builder()
                    .name("sparse_vector")
                    .dataType(VECTOR_SPARSE_FLOAT_TYPE)
                    .scale(indexMetadata.getDimension())
                    .build();
            columns.add(sparseVectorColumn);
        }

        TableSchema tableSchema = TableSchema.builder()
                .primaryKey(PrimaryKey.of("id", Collections.singletonList("id")))
                .columns(columns)
                .build();
        Map<TablePath, CatalogTable> sourceTables = new HashMap<>();
        CatalogTable catalogTable = CatalogTable.of(TableIdentifier.of("pinecone", tablePath),
                tableSchema, new HashMap<>(), new ArrayList<>(), "");
        sourceTables.put(tablePath, catalogTable);
        return sourceTables;
    }
}

