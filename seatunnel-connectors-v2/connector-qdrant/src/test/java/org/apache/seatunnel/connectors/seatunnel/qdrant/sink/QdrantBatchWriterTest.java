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

package org.apache.seatunnel.connectors.seatunnel.qdrant.sink;

import com.google.common.util.concurrent.Futures;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.grpc.Points;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.qdrant.config.QdrantParameters;
import org.apache.seatunnel.connectors.seatunnel.qdrant.utils.ConnectorUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.UUID;

class QdrantBatchWriterTest {
    private final QdrantClient client = Mockito.mock(QdrantClient.class);
    private final QdrantParameters parameters = Mockito.mock(QdrantParameters.class);

    @AfterEach
    void neverReadsTargetSchema() {
        Mockito.verify(client, Mockito.never()).getCollectionInfoAsync(Mockito.anyString());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void preservesUnnamedSourceAfterRename(boolean multi) {
        configure();
        PhysicalColumn column = PhysicalColumn.builder().name("renamed")
                .dataType(multi ? ArrayType.of(ArrayType.FLOAT_ARRAY_TYPE) : VectorType.VECTOR_FLOAT_TYPE)
                .options(java.util.Collections.singletonMap(ConnectorUtils.UNNAMED_VECTOR, true)).build();
        QdrantBatchWriter writer = new QdrantBatchWriter(table(column), 64, parameters);
        writer.addToBatch(new SeaTunnelRow(new Object[] {7L, multi
                ? new Float[][] {{1.0f, 2.0f}} : BufferUtils.toByteBuffer(new Float[] {1.0f, 2.0f})}));
        writer.flush();
        ArgumentCaptor<Points.UpsertPoints> request = ArgumentCaptor.forClass(Points.UpsertPoints.class);
        Mockito.verify(client).upsertAsync(request.capture());
        Assertions.assertTrue(request.getValue().getPoints(0).getVectors().hasVector());
        Assertions.assertEquals(multi, request.getValue().getPoints(0).getVectors().getVector().hasMultiDense());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void writesNamedAndUnnamedMultiVectors(boolean named) {
        String field = named ? "colbert" : "vector";
        configure();
        QdrantBatchWriter writer = new QdrantBatchWriter(table(multiColumn(field, 2, !named)), 64, parameters);
        for (int count : new int[] {1, 3, 4097}) {
            Float[][] matrix = new Float[count][];
            for (int i = 0; i < count; i++) {
                matrix[i] = new Float[] {(float) i, (float) -i};
            }
            writer.addToBatch(new SeaTunnelRow(new Object[] {4294967297L, matrix}));
            Assertions.assertEquals((float) (count - 1), matrix[count - 1][0]);
        }
        writer.flush();
        ArgumentCaptor<Points.UpsertPoints> request = ArgumentCaptor.forClass(Points.UpsertPoints.class);
        Mockito.verify(client).upsertAsync(request.capture());
        Assertions.assertEquals("target", request.getValue().getCollectionName());
        Assertions.assertEquals(3, request.getValue().getPointsCount());
        int[] counts = {1, 3, 4097};
        for (int i = 0; i < counts.length; i++) {
            Points.PointStruct point = request.getValue().getPoints(i);
            Assertions.assertEquals(4294967297L, point.getId().getNum());
            Assertions.assertEquals(named, point.getVectors().hasVectors());
            Assertions.assertEquals(!named, point.getVectors().hasVector());
            Points.Vector vector = named ? point.getVectors().getVectors().getVectorsOrThrow(field)
                    : point.getVectors().getVector();
            Assertions.assertTrue(vector.hasMultiDense());
            Assertions.assertEquals(counts[i], vector.getMultiDense().getVectorsCount());
            for (int j = 0; j < counts[i]; j++) {
                Assertions.assertEquals(Arrays.asList((float) j, (float) -j),
                        vector.getMultiDense().getVectors(j).getDataList());
            }
        }
        writer.close();
        Mockito.verify(client).close();
    }

    @Test
    void mixesNamedMultiVectorsWithDenseVectorsAndPayload() {
        configure();
        QdrantBatchWriter writer = new QdrantBatchWriter(table(
                multiColumn("a", 2), multiColumn("b", 3),
                PhysicalColumn.builder().name("dense").dataType(VectorType.VECTOR_FLOAT_TYPE).scale(2).build(),
                PhysicalColumn.builder().name("label").dataType(BasicType.STRING_TYPE).build()), 64, parameters);
        writer.addToBatch(new SeaTunnelRow(new Object[] {7L,
                new Float[][] {{1.0f, 2.0f}}, new Float[][] {{3.0f, 4.0f, 5.0f}},
                BufferUtils.toByteBuffer(new Float[] {6.0f, 7.0f}), "hello"}));
        writer.flush();
        ArgumentCaptor<Points.UpsertPoints> request = ArgumentCaptor.forClass(Points.UpsertPoints.class);
        Mockito.verify(client).upsertAsync(request.capture());
        Points.PointStruct point = request.getValue().getPoints(0);
        Assertions.assertEquals(3, point.getVectors().getVectors().getVectorsCount());
        Assertions.assertEquals(Arrays.asList(3.0f, 4.0f, 5.0f), point.getVectors().getVectors()
                .getVectorsOrThrow("b").getMultiDense().getVectors(0).getDataList());
        Assertions.assertEquals(Arrays.asList(6.0f, 7.0f), point.getVectors().getVectors()
                .getVectorsOrThrow("dense").getDense().getDataList());
        Assertions.assertEquals("hello", point.getPayloadOrThrow("label").getStringValue());
    }

    @Test
    void preservesAdditionalVectorFieldsForServerValidation() {
        configure();
        QdrantBatchWriter writer = new QdrantBatchWriter(table(multiColumn("vector", 2, true),
                PhysicalColumn.builder().name("other").dataType(VectorType.VECTOR_FLOAT_TYPE).build()),
                64, parameters);
        writer.addToBatch(new SeaTunnelRow(new Object[] {7L, new Float[][] {{1.0f, 2.0f}},
                BufferUtils.toByteBuffer(new Float[] {3.0f, 4.0f})}));
        writer.flush();
        ArgumentCaptor<Points.UpsertPoints> request = ArgumentCaptor.forClass(Points.UpsertPoints.class);
        Mockito.verify(client).upsertAsync(request.capture());
        Points.NamedVectors vectors = request.getValue().getPoints(0).getVectors().getVectors();
        Assertions.assertEquals(2, vectors.getVectorsCount());
        Assertions.assertTrue(vectors.containsVectors(""));
        Assertions.assertTrue(vectors.containsVectors("other"));
    }

    @Test
    void forwardsEncodableMatricesAndPropagatesServerErrors() {
        configure();
        QdrantBatchWriter writer = new QdrantBatchWriter(table(multiColumn("colbert", 0)), 64, parameters);
        for (Float[][] matrix : new Float[][][] {{}, {{1.0f}},
                {{1.0f, 2.0f}, {3.0f}}, {{Float.NaN, Float.POSITIVE_INFINITY}}}) {
            writer.addToBatch(new SeaTunnelRow(new Object[] {7L, matrix}));
        }
        IllegalArgumentException serverError = new IllegalArgumentException("server: wrong vector dimension");
        Mockito.when(client.upsertAsync(Mockito.any(Points.UpsertPoints.class)))
                .thenReturn(Futures.immediateFailedFuture(serverError));
        RuntimeException failure = Assertions.assertThrows(RuntimeException.class, writer::flush);
        Assertions.assertSame(serverError, failure.getCause().getCause());
        ArgumentCaptor<Points.UpsertPoints> request = ArgumentCaptor.forClass(Points.UpsertPoints.class);
        Mockito.verify(client).upsertAsync(request.capture());
        Assertions.assertEquals(4, request.getValue().getPointsCount());
        Assertions.assertEquals(0, request.getValue().getPoints(0).getVectors().getVectors()
                .getVectorsOrThrow("colbert").getMultiDense().getVectorsCount());
        Assertions.assertEquals(Arrays.asList(3.0f), request.getValue().getPoints(2).getVectors().getVectors()
                .getVectorsOrThrow("colbert").getMultiDense().getVectors(1).getDataList());
        Assertions.assertTrue(Float.isNaN(request.getValue().getPoints(3).getVectors().getVectors()
                .getVectorsOrThrow("colbert").getMultiDense().getVectors(0).getData(0)));
    }

    @Test
    void preservesNamedVectorNameAndOmitsMissingVector() {
        configure();
        QdrantBatchWriter writer = new QdrantBatchWriter(table(multiColumn("vector", 2)), 64, parameters);
        writer.addToBatch(new SeaTunnelRow(new Object[] {7L, new Float[][] {{1.0f, 2.0f}}}));
        writer.addToBatch(new SeaTunnelRow(new Object[] {8L, null}));
        writer.flush();
        ArgumentCaptor<Points.UpsertPoints> request = ArgumentCaptor.forClass(Points.UpsertPoints.class);
        Mockito.verify(client).upsertAsync(request.capture());
        Assertions.assertTrue(request.getValue().getPoints(0).getVectors().hasVectors());
        Assertions.assertTrue(request.getValue().getPoints(0).getVectors().getVectors().containsVectors("vector"));
        Assertions.assertEquals(0, request.getValue().getPoints(1).getVectors().getVectors().getVectorsCount());
    }

    @Test
    void leavesOrdinaryVectorWritesOnExistingPath() {
        configure();
        QdrantBatchWriter writer = new QdrantBatchWriter(table(
                PhysicalColumn.builder().name("dense").dataType(VectorType.VECTOR_FLOAT_TYPE).build()),
                64, parameters);
        Mockito.verify(client, Mockito.never()).getCollectionInfoAsync(Mockito.anyString());
        writer.addToBatch(new SeaTunnelRow(new Object[] {7L,
                BufferUtils.toByteBuffer(new Float[] {1.0f, 2.0f})}));
        writer.flush();
        Mockito.verify(client).upsertAsync(Mockito.any(Points.UpsertPoints.class));
        String uuid = UUID.randomUUID().toString();
        Assertions.assertEquals(uuid, QdrantBatchWriter.pointId(BasicType.STRING_TYPE, uuid).getUuid());
        Assertions.assertEquals(7L, QdrantBatchWriter.pointId(BasicType.INT_TYPE, 7).getNum());
    }

    private void configure() {
        Mockito.when(parameters.buildQdrantClient()).thenReturn(client);
        Mockito.when(parameters.getCollectionName()).thenReturn("target");
        Mockito.when(client.upsertAsync(Mockito.any(Points.UpsertPoints.class)))
                .thenReturn(Futures.immediateFuture(Points.UpdateResult.getDefaultInstance()));
    }

    private PhysicalColumn multiColumn(String name, int dimension) {
        return multiColumn(name, dimension, false);
    }

    private PhysicalColumn multiColumn(String name, int dimension, boolean unnamed) {
        return PhysicalColumn.builder().name(name).dataType(ArrayType.of(ArrayType.FLOAT_ARRAY_TYPE))
                .scale(dimension)
                .options(java.util.Collections.singletonMap(ConnectorUtils.UNNAMED_VECTOR, unnamed))
                .build();
    }

    private CatalogTable table(PhysicalColumn... fields) {
        TableSchema.Builder schema = TableSchema.builder()
                .column(PhysicalColumn.builder().name("id").dataType(BasicType.LONG_TYPE).build())
                .primaryKey(PrimaryKey.of("id", java.util.Collections.singletonList("id")));
        for (PhysicalColumn field : fields) {
            schema.column(field);
        }
        return CatalogTable.of(TableIdentifier.of("qdrant", TablePath.of("default", "source")),
                schema.build(), java.util.Collections.emptyMap(), java.util.Collections.emptyList(), "");
    }
}
