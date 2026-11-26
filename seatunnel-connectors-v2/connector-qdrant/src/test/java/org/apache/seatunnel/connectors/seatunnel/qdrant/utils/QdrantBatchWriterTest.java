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

import com.google.common.util.concurrent.Futures;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.grpc.Common.PointId;
import io.qdrant.client.grpc.Points;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.qdrant.config.QdrantParameters;
import org.apache.seatunnel.connectors.seatunnel.qdrant.sink.QdrantBatchWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

class QdrantBatchWriterTest {
    @ParameterizedTest
    @CsvSource({"false,vector", "false,renamed", "true,vector", "true,renamed"})
    void preservesDenseWritesByFieldName(boolean namedSource, String targetName) {
        Column vector = ConnectorUtils.vectorColumn("vector",
                io.qdrant.client.grpc.Collections.VectorParams.newBuilder().setSize(2).build());
        TableSchema sourceSchema = TableSchema.builder()
                .column(PhysicalColumn.builder().name("id").dataType(BasicType.STRING_TYPE).build())
                .column(vector)
                .primaryKey(PrimaryKey.of("id", Collections.singletonList("id"))).build();
        Points.VectorOutput dense = Points.VectorOutput.newBuilder()
                .setDense(Points.DenseVector.newBuilder().addData(1.0f).addData(2.0f)).build();
        Points.VectorsOutput.Builder vectors = Points.VectorsOutput.newBuilder();
        if (namedSource) {
            vectors.setVectors(Points.NamedVectorsOutput.newBuilder().putVectors("vector", dense));
        } else {
            vectors.setVector(dense);
        }
        String uuid = "00000000-0000-0000-0000-000000000007";
        SeaTunnelRow row = ConverterUtils.convertToSeaTunnelRowWithMeta(sourceSchema,
                Points.RetrievedPoint.newBuilder().setId(PointId.newBuilder().setUuid(uuid))
                        .setVectors(vectors).build());
        // Column.rename preserves options, as FieldMapper does.
        TableSchema targetSchema = TableSchema.builder().column(sourceSchema.getColumn("id"))
                .column(vector.rename(targetName)).primaryKey(sourceSchema.getPrimaryKey()).build();
        CatalogTable table = CatalogTable.of(TableIdentifier.of("qdrant", TablePath.of("default", "source")),
                targetSchema, Collections.emptyMap(), Collections.emptyList(), "");
        QdrantClient client = Mockito.mock(QdrantClient.class);
        QdrantParameters parameters = Mockito.mock(QdrantParameters.class);
        Mockito.when(parameters.buildQdrantClient()).thenReturn(client);
        Mockito.when(parameters.getCollectionName()).thenReturn("target");
        Mockito.when(client.upsertAsync(Mockito.any(Points.UpsertPoints.class)))
                .thenReturn(Futures.immediateFuture(Points.UpdateResult.getDefaultInstance()));
        QdrantBatchWriter writer = new QdrantBatchWriter(table, 64, parameters);
        writer.addToBatch(row);
        writer.flush();
        ArgumentCaptor<Points.UpsertPoints> request = ArgumentCaptor.forClass(Points.UpsertPoints.class);
        Mockito.verify(client).upsertAsync(request.capture());
        Points.PointStruct point = request.getValue().getPoints(0);
        Assertions.assertEquals(uuid, point.getId().getUuid());
        Assertions.assertTrue(point.getVectors().hasVectors());
        Assertions.assertEquals(1, point.getVectors().getVectors().getVectorsCount());
        Assertions.assertEquals(Arrays.asList(1.0f, 2.0f), point.getVectors().getVectors()
                .getVectorsOrThrow(targetName).getDense().getDataList());
        Mockito.verify(client, Mockito.never()).getCollectionInfoAsync(Mockito.anyString());
    }
}
