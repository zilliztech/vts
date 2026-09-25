package org.apache.seatunnel.connectors.seatunnel.qdrant.utils;

import io.qdrant.client.grpc.Common.PointId;
import io.qdrant.client.grpc.Points;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
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
    void rejectsMultiDenseInsteadOfFlatteningOrReturningEmptyVector(boolean modern) {
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
