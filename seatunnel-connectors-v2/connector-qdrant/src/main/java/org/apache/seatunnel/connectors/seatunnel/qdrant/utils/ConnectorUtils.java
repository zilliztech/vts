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

package org.apache.seatunnel.connectors.seatunnel.qdrant.utils;

import com.google.common.collect.Lists;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import static io.qdrant.client.WithPayloadSelectorFactory.enable;
import io.qdrant.client.WithVectorsSelectorFactory;
import io.qdrant.client.grpc.Collections;
import io.qdrant.client.grpc.Points;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import static org.apache.seatunnel.api.table.type.BasicType.JSON_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.VectorType.VECTOR_FLOAT_TYPE;
import static org.apache.seatunnel.api.table.type.VectorType.VECTOR_SPARSE_FLOAT_TYPE;
import org.apache.seatunnel.connectors.seatunnel.qdrant.config.QdrantConfig;
import static org.apache.seatunnel.connectors.seatunnel.qdrant.config.QdrantConfig.COLLECTION_NAME;
import org.apache.seatunnel.connectors.seatunnel.qdrant.exception.QdrantConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.qdrant.exception.QdrantConnectorException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ConnectorUtils {

    public static CatalogTable buildCatalogTable(ReadonlyConfig config) {
        QdrantClient client = new QdrantClient(QdrantGrpcClient.
                newBuilder(config.get(QdrantConfig.HOST), config.get(QdrantConfig.PORT), config.get(QdrantConfig.USE_TLS))
                .withApiKey(config.get(QdrantConfig.API_KEY))
                .build());

        String collectionName = config.get(COLLECTION_NAME);

        Collections.CollectionInfo collectionInfo = null;
        try {
            collectionInfo = client.getCollectionInfoAsync(collectionName).get();
            TablePath tablePath = TablePath.of("default", config.get(COLLECTION_NAME));
            List<Column> columns = new ArrayList<>();
            int SCROLL_SIZE = 1;
            Points.ScrollPoints request =
                    Points.ScrollPoints.newBuilder()
                            .setCollectionName(config.get(COLLECTION_NAME))
                            .setLimit(SCROLL_SIZE)
                            .setWithPayload(enable(false))
                            .setWithVectors(WithVectorsSelectorFactory.enable(true))
                            .build();


            Points.ScrollResponse response = client.scrollAsync(request).get();
            List<Points.RetrievedPoint> points = response.getResultList();
            if(points.isEmpty()){
                throw new QdrantConnectorException(QdrantConnectionErrorCode.EMPTY_COLLECTION, "No data in collection");
            }
            Points.PointId id = points.get(0).getId();
            if (id.hasNum()) {
                PhysicalColumn idColumn = PhysicalColumn.builder()
                        .name("id")
                        .dataType(LONG_TYPE)
                        .build();
                columns.add(idColumn);
            } else if (id.hasUuid()) {
                PhysicalColumn idColumn = PhysicalColumn.builder()
                        .name("id")
                        .dataType(STRING_TYPE)
                        .build();
                columns.add(idColumn);
            }
            Collections.VectorsConfig vectorsConfig = collectionInfo.getConfig().getParams().getVectorsConfig();
            if(vectorsConfig.getParamsMap().getMapMap().isEmpty()) {
                // single vector
                long dimension = vectorsConfig.getParams().getSize();
                PhysicalColumn vectorColumn = PhysicalColumn.builder()
                        .name("vector")
                        .dataType(VECTOR_FLOAT_TYPE)
                        .scale(Math.toIntExact(dimension))
                        .build();
                columns.add(vectorColumn);
            } else if (!vectorsConfig.getParamsMap().getMapMap().isEmpty()) {
                // multiple vectors
                for (Map.Entry<String, Collections.VectorParams> entry : vectorsConfig.getParamsMap().getMapMap().entrySet()) {
                    Collections.VectorParams params = entry.getValue();
                    long dimension = params.getSize();
                    PhysicalColumn vectorColumn = PhysicalColumn.builder()
                            .name(entry.getKey())
                            .dataType(VECTOR_FLOAT_TYPE)
                            .scale(Math.toIntExact(dimension))
                            .build();
                    columns.add(vectorColumn);
                }
            }
            Collections.SparseVectorConfig sparseVectorConfig = collectionInfo.getConfig().getParams().getSparseVectorsConfig();
            if (!sparseVectorConfig.getMapMap().isEmpty()) {
                sparseVectorConfig.getMapMap().forEach((key, value) -> {
                    PhysicalColumn sparseVectorColumn = PhysicalColumn.builder()
                            .name(key)
                            .dataType(VECTOR_SPARSE_FLOAT_TYPE)
                            .build();
                    columns.add(sparseVectorColumn);
                });
            }
            Map<String, Object> options = new HashMap<>();
            options.put("isDynamicField", true);
            // dynamic column
            PhysicalColumn dynamicColumn = PhysicalColumn.builder()
                    .name("meta")
                    .dataType(JSON_TYPE)
                    .options(options)
                    .build();

            columns.add(dynamicColumn);

            TableSchema tableSchema = TableSchema.builder()
                    .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                    .columns(columns)
                    .build();

            CatalogTable catalogTable = CatalogTable.of(TableIdentifier.of("qdrant", tablePath),
                    tableSchema, new HashMap<>(), new ArrayList<>(), "");
            return catalogTable;
        }catch (ExecutionException | InterruptedException e) {
            throw new QdrantConnectorException(QdrantConnectionErrorCode.FAILED_CONNECT_QDRANT, e.getMessage());
        }
    }
}
