package org.apache.seatunnel.connectors.seatunnel.qdrant.utils;

import io.qdrant.client.grpc.Common.PointId;
import io.qdrant.client.grpc.Points;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.qdrant.exception.QdrantConnectorException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConverterUtilsTest {
    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void preservesDenseVectorForBothWireFormatsAndNamingModes(boolean modern, boolean named) throws Exception {
        List<Float> values = new ArrayList<>();
        for (int i = 0; i < 384; i++) {
            values.add((i - 192) / 384.0f);
        }
        Points.VectorOutput vector = modern
                ? Points.VectorOutput.newBuilder()
                        .setDense(Points.DenseVector.newBuilder().addAllData(values)).build()
                : Points.VectorOutput.newBuilder().addAllData(values).build();
        String field = named ? "dense_vector" : "vector";
        Points.VectorsOutput vectors = named
                ? Points.VectorsOutput.newBuilder().setVectors(Points.NamedVectorsOutput.newBuilder()
                        .putVectors(field, vector)).build()
                : Points.VectorsOutput.newBuilder().setVector(vector).build();
        Points.RetrievedPoint point = Points.RetrievedPoint.newBuilder()
                .setId(PointId.newBuilder().setUuid("00000000-0000-0000-0000-000000000007"))
                .setVectors(vectors).build();

        SeaTunnelRow row = ConverterUtils.convertToSeaTunnelRowWithMeta(
                schema(field, VectorType.VECTOR_FLOAT_TYPE),
                Points.RetrievedPoint.parseFrom(point.toByteArray()));

        assertEquals("00000000-0000-0000-0000-000000000007", row.getField(0));
        assertArrayEquals(values.toArray(new Float[0]), BufferUtils.toFloatArray((ByteBuffer) row.getField(1)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void preservesSparseVectorForBothWireFormats(boolean modern) {
        List<Integer> indices = Arrays.asList(2, -1);
        List<Float> values = Arrays.asList(0.25f, 0.75f);
        Points.VectorOutput vector = modern
                ? Points.VectorOutput.newBuilder().setSparse(Points.SparseVector.newBuilder()
                        .addAllIndices(indices).addAllValues(values)).build()
                : Points.VectorOutput.newBuilder().setIndices(Points.SparseIndices.newBuilder()
                        .addAllData(indices)).addAllData(values).build();

        SeaTunnelRow row = convertNamed("sparse_vector", VectorType.VECTOR_SPARSE_FLOAT_TYPE, vector);

        Map<?, ?> sparse = (Map<?, ?>) row.getField(1);
        assertEquals(2, sparse.size());
        assertEquals(0.25f, sparse.get(2L));
        assertEquals(0.75f, sparse.get(4294967295L));
    }

    @Test
    void missingNamedVectorRemainsNull() {
        SeaTunnelRow row = ConverterUtils.convertToSeaTunnelRowWithMeta(
                schema("dense_vector", VectorType.VECTOR_FLOAT_TYPE),
                Points.RetrievedPoint.newBuilder().setId(PointId.newBuilder().setNum(7)).build());
        assertEquals(7L, row.getField(0));
        assertNull(row.getField(1));
    }

    @Test
    void preservesEmptySparseVector() {
        SeaTunnelRow row = convertNamed("sparse_vector", VectorType.VECTOR_SPARSE_FLOAT_TYPE,
                Points.VectorOutput.newBuilder().setSparse(Points.SparseVector.getDefaultInstance()).build());
        assertEquals(Collections.emptyMap(), row.getField(1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void rejectsMultiDenseWhenSchemaDeclaresSingleVector(boolean modern) {
        Points.VectorOutput vector = modern
                ? Points.VectorOutput.newBuilder().setMultiDense(Points.MultiDenseVector.newBuilder()
                        .addVectors(Points.DenseVector.newBuilder().addData(1.0f))).build()
                : Points.VectorOutput.newBuilder().addData(1.0f).setVectorsCount(1).build();
        assertThrows(QdrantConnectorException.class,
                () -> convertNamed("dense_vector", VectorType.VECTOR_FLOAT_TYPE, vector));
    }

    @Test
    void rejectsSparseVectorForDenseField() {
        Points.VectorOutput vector = Points.VectorOutput.newBuilder()
                .setSparse(Points.SparseVector.newBuilder().addIndices(1).addValues(0.5f)).build();
        assertThrows(QdrantConnectorException.class,
                () -> convertNamed("dense_vector", VectorType.VECTOR_FLOAT_TYPE, vector));
    }

    @Test
    void rejectsDenseVectorForSparseField() {
        Points.VectorOutput vector = Points.VectorOutput.newBuilder()
                .setDense(Points.DenseVector.newBuilder().addData(0.5f)).build();
        assertThrows(QdrantConnectorException.class,
                () -> convertNamed("sparse_vector", VectorType.VECTOR_SPARSE_FLOAT_TYPE, vector));
    }

    @Test
    void rejectsMismatchedSparseIndicesAndValues() {
        Points.VectorOutput vector = Points.VectorOutput.newBuilder()
                .setSparse(Points.SparseVector.newBuilder().addIndices(1)).build();
        assertThrows(QdrantConnectorException.class,
                () -> convertNamed("sparse_vector", VectorType.VECTOR_SPARSE_FLOAT_TYPE, vector));
    }

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void preservesMultiDenseBoundaries(boolean modern, boolean named) {
        String field = named ? "colbert" : "vector";
        io.qdrant.client.grpc.Collections.VectorParams params =
                io.qdrant.client.grpc.Collections.VectorParams.newBuilder()
                        .setSize(2)
                        .setDistance(io.qdrant.client.grpc.Collections.Distance.Cosine)
                        .setMultivectorConfig(
                                io.qdrant.client.grpc.Collections.MultiVectorConfig.newBuilder()
                                        .setComparator(
                                                io.qdrant.client.grpc.Collections
                                                        .MultiVectorComparator.MaxSim))
                        .build();
        org.apache.seatunnel.api.table.catalog.Column column =
                ConnectorUtils.vectorColumn(field, params, !named);
        assertEquals(ArrayType.of(ArrayType.FLOAT_ARRAY_TYPE), column.getDataType());
        assertEquals(2, column.getScale());
        assertNull(column.getSourceType());
        assertEquals(!named, Boolean.TRUE.equals(column.getOptions().get(ConnectorUtils.UNNAMED_VECTOR)));
        TableSchema schema =
                TableSchema.builder()
                        .columns(
                                Arrays.asList(
                                        PhysicalColumn.builder()
                                                .name("id")
                                                .dataType(BasicType.LONG_TYPE)
                                                .build(),
                                        column))
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id")))
                        .build();
        for (int count : new int[] {1, 3, 4096, 4097}) {
            Points.VectorOutput.Builder vector = Points.VectorOutput.newBuilder();
            if (modern) {
                Points.MultiDenseVector.Builder multi = Points.MultiDenseVector.newBuilder();
                for (int i = 0; i < count; i++) {
                    multi.addVectors(Points.DenseVector.newBuilder().addData(i).addData(-i));
                }
                vector.setMultiDense(multi);
            } else {
                for (int i = 0; i < count; i++) {
                    vector.addData(i).addData(-i);
                }
                vector.setVectorsCount(count);
            }
            Points.VectorsOutput.Builder vectors = Points.VectorsOutput.newBuilder();
            if (named) {
                vectors.setVectors(
                        Points.NamedVectorsOutput.newBuilder().putVectors(field, vector.build()));
            } else {
                vectors.setVector(vector);
            }
            SeaTunnelRow row =
                    ConverterUtils.convertToSeaTunnelRowWithMeta(
                            schema,
                            Points.RetrievedPoint.newBuilder()
                                    .setId(PointId.newBuilder().setNum(7))
                                    .setVectors(vectors)
                                    .build());
            Float[][] embeddings = (Float[][]) row.getField(1);
            assertEquals(count, embeddings.length);
            for (int i = 0; i < count; i++) {
                assertArrayEquals(new Float[] {(float) i, (float) -i},
                        embeddings[i]);
            }
        }
    }

    @Test
    void rejectsMalformedMultiDenseWithPointAndFieldContext() {
        io.qdrant.client.grpc.Collections.VectorParams params =
                io.qdrant.client.grpc.Collections.VectorParams.newBuilder()
                        .setSize(2)
                        .setDistance(io.qdrant.client.grpc.Collections.Distance.Cosine)
                        .setMultivectorConfig(
                                io.qdrant.client.grpc.Collections.MultiVectorConfig.newBuilder()
                                        .setComparator(
                                                io.qdrant.client.grpc.Collections
                                                        .MultiVectorComparator.MaxSim))
                        .build();
        TableSchema schema =
                TableSchema.builder()
                        .columns(
                                Arrays.asList(
                                        PhysicalColumn.builder()
                                                .name("id")
                                                .dataType(BasicType.LONG_TYPE)
                                                .build(),
                                        ConnectorUtils.vectorColumn("colbert", params, false)))
                        .primaryKey(PrimaryKey.of("id", Collections.singletonList("id")))
                        .build();
        List<Points.VectorOutput> invalid = Arrays.asList(
                Points.VectorOutput.newBuilder().setVectorsCount(2).addData(1).addData(2).addData(3).build(),
                Points.VectorOutput.newBuilder().setVectorsCount(0).addData(1).build(),
                Points.VectorOutput.newBuilder().setVectorsCount(-1).build(),
                Points.VectorOutput.newBuilder().setVectorsCount(1).addData(1)
                        .setIndices(Points.SparseIndices.newBuilder().addData(1)).build(),
                Points.VectorOutput.newBuilder()
                        .setDense(Points.DenseVector.newBuilder().addData(1).addData(2)).build());
        for (Points.VectorOutput vector : invalid) {
            QdrantConnectorException error =
                    assertThrows(
                            QdrantConnectorException.class,
                            () ->
                                    ConverterUtils.convertToSeaTunnelRowWithMeta(
                                            schema,
                                            Points.RetrievedPoint.newBuilder()
                                                    .setId(PointId.newBuilder().setNum(42))
                                                    .setVectors(
                                                            Points.VectorsOutput.newBuilder()
                                                                    .setVectors(
                                                                            Points
                                                                                    .NamedVectorsOutput
                                                                                    .newBuilder()
                                                                                    .putVectors(
                                                                                            "colbert",
                                                                                            vector)))
                                                    .build()));
            org.junit.jupiter.api.Assertions.assertTrue(error.getMessage().contains("colbert"));
            org.junit.jupiter.api.Assertions.assertTrue(error.getMessage().contains("42"));
        }
    }

    @Test
    void decodesMultiDenseWithoutValidatingDimensionsOrValues() {
        Points.VectorOutput vector = Points.VectorOutput.newBuilder().setMultiDense(
                Points.MultiDenseVector.newBuilder()
                        .addVectors(Points.DenseVector.newBuilder().addData(1))
                        .addVectors(Points.DenseVector.newBuilder().addData(Float.NaN).addData(2))).build();
        Float[][] values = (Float[][]) convertNamed("colbert", ArrayType.of(ArrayType.FLOAT_ARRAY_TYPE), vector).getField(1);
        assertArrayEquals(new Float[] {1.0f}, values[0]);
        assertArrayEquals(new Float[] {Float.NaN, 2.0f}, values[1]);
        Points.VectorOutput legacy = Points.VectorOutput.newBuilder().setVectorsCount(2)
                .addData(Float.NaN).addData(2).build();
        Float[][] legacyValues = (Float[][]) convertNamed("colbert", ArrayType.of(ArrayType.FLOAT_ARRAY_TYPE), legacy).getField(1);
        assertArrayEquals(new Float[] {Float.NaN}, legacyValues[0]);
        assertArrayEquals(new Float[] {2.0f}, legacyValues[1]);
        for (Points.VectorOutput empty : Arrays.asList(
                Points.VectorOutput.newBuilder().setMultiDense(Points.MultiDenseVector.getDefaultInstance()).build(),
                Points.VectorOutput.newBuilder().setVectorsCount(0).build())) {
            assertEquals(0, ((Float[][]) convertNamed("colbert", ArrayType.of(ArrayType.FLOAT_ARRAY_TYPE), empty).getField(1)).length);
        }
    }

    private SeaTunnelRow convertNamed(String field, SeaTunnelDataType<?> type, Points.VectorOutput vector) {
        Points.RetrievedPoint point = Points.RetrievedPoint.newBuilder()
                .setId(PointId.newBuilder().setNum(7))
                .setVectors(Points.VectorsOutput.newBuilder().setVectors(Points.NamedVectorsOutput.newBuilder()
                        .putVectors(field, vector))).build();
        return ConverterUtils.convertToSeaTunnelRowWithMeta(schema(field, type), point);
    }

    private TableSchema schema(String field, SeaTunnelDataType<?> type) {
        return TableSchema.builder()
                .column(PhysicalColumn.builder().name("id").dataType(BasicType.STRING_TYPE).build())
                .column(PhysicalColumn.builder().name(field).dataType(type).build())
                .primaryKey(PrimaryKey.of("id", Collections.singletonList("id")))
                .build();
    }
}
