package org.apache.seatunnel.connectors.seatunnel.qdrant.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.qdrant.client.grpc.JsonWithInt;
import io.qdrant.client.grpc.Points;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.qdrant.exception.QdrantConnectorException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.seatunnel.api.table.catalog.PrimaryKey.isPrimaryKeyField;

public class ConverterUtils {
    public static SeaTunnelRow convertToSeaTunnelRow(TableSchema tableSchema, Points.RetrievedPoint point) {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        PrimaryKey primaryKey = tableSchema.getPrimaryKey();
        Map<String, JsonWithInt.Value> payloadMap = point.getPayloadMap();
        Points.Vectors vectors = point.getVectors();
        Map<String, Points.Vector> vectorsMap = new HashMap<>();
        String DEFAULT_VECTOR_KEY = "vector";

        if (vectors.hasVector()) {
            vectorsMap.put(DEFAULT_VECTOR_KEY, vectors.getVector());
        } else if (vectors.hasVectors()) {
            vectorsMap = vectors.getVectors().getVectorsMap();
        }
        Object[] fields = new Object[typeInfo.getTotalFields()];
        String[] fieldNames = typeInfo.getFieldNames();
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            String fieldName = fieldNames[fieldIndex];

            if (isPrimaryKeyField(primaryKey, fieldName)) {
                Points.PointId id = point.getId();
                if (id.hasNum()) {
                    fields[fieldIndex] = id.getNum();
                } else if (id.hasUuid()) {
                    fields[fieldIndex] = id.getUuid();
                }
                continue;
            }
            JsonWithInt.Value value = payloadMap.get(fieldName);
            Points.Vector vector = vectorsMap.get(fieldName);
            switch (seaTunnelDataType.getSqlType()) {
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case STRING:
                    fields[fieldIndex] = value.getStringValue();
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = value.getBoolValue();
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    fields[fieldIndex] = value.getIntegerValue();
                    break;
                case FLOAT:
                case DECIMAL:
                case DOUBLE:
                    fields[fieldIndex] = value.getDoubleValue();
                    break;
                case FLOAT_VECTOR:
                    List<Float> floats = vector.getDataList();
                    Float[] floats1 = new Float[floats.size()];
                    floats.toArray(floats1);
                    fields[fieldIndex] = floats1;
                    break;
                case BINARY_VECTOR:
                case FLOAT16_VECTOR:
                case BFLOAT16_VECTOR:
                    List<Float> list = vector.getDataList();
                    Float[] vectorArray = new Float[list.size()];
                    list.toArray(vectorArray);
                    fields[fieldIndex] = BufferUtils.toByteBuffer(vectorArray);
                    break;
                default:
                    throw new QdrantConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType.getSqlType().name());
            }
        }

        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
        //seaTunnelRow.setTableId(tablePath.getFullName());
        seaTunnelRow.setRowKind(RowKind.INSERT);
        return seaTunnelRow;
    }

    public static SeaTunnelRow convertToSeaTunnelRowWithMeta(TableSchema tableSchema, Points.RetrievedPoint point) {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        PrimaryKey primaryKey = tableSchema.getPrimaryKey();
        Points.Vectors vectors = point.getVectors();
        Map<String, Points.Vector> vectorsMap = new HashMap<>();
        String DEFAULT_VECTOR_KEY = "vector";
        Map<String, JsonWithInt.Value> payloadMap = point.getPayloadMap();
        if (vectors.hasVector()) {
            vectorsMap.put(DEFAULT_VECTOR_KEY, vectors.getVector());
        } else if (vectors.hasVectors()) {
            vectorsMap = vectors.getVectors().getVectorsMap();
        }
        Object[] fields = new Object[typeInfo.getTotalFields()];
        String[] fieldNames = typeInfo.getFieldNames();
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            String fieldName = fieldNames[fieldIndex];

            if(Objects.equals(fieldName, "meta")){
                JsonObject data = new JsonObject();
                for (String entry : payloadMap.keySet()) {
                    data.add(entry, convertValueToJsonElement(payloadMap.get(entry)));
                }
                fields[fieldIndex] = data;
                continue;
            }

            if (isPrimaryKeyField(primaryKey, fieldName)) {
                Points.PointId id = point.getId();
                if (id.hasNum()) {
                    fields[fieldIndex] = id.getNum();
                } else if (id.hasUuid()) {
                    fields[fieldIndex] = id.getUuid();
                }
                continue;
            }

            Points.Vector vector = vectorsMap.get(fieldName);
            switch (seaTunnelDataType.getSqlType()) {
                case FLOAT_VECTOR:
                    List<Float> floats = vector.getDataList();
                    Float[] floats1 = new Float[floats.size()];
                    floats.toArray(floats1);
                    fields[fieldIndex] = floats1;
                    break;
                case BINARY_VECTOR:
                case FLOAT16_VECTOR:
                case BFLOAT16_VECTOR:
                    List<Float> list = vector.getDataList();
                    Float[] vectorArray = new Float[list.size()];
                    list.toArray(vectorArray);
                    fields[fieldIndex] = BufferUtils.toByteBuffer(vectorArray);
                    break;
                default:
                    throw new QdrantConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType.getSqlType().name());
            }
        }

        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
        seaTunnelRow.setRowKind(RowKind.INSERT);
        return seaTunnelRow;
    }

    private static JsonElement convertValueToJsonElement(JsonWithInt.Value value) {
        switch (value.getKindCase()) {
            case BOOL_VALUE:
                return new JsonPrimitive(value.getBoolValue());
            case INTEGER_VALUE:
                return new JsonPrimitive(value.getIntegerValue());
            case STRING_VALUE:
                return new JsonPrimitive(value.getStringValue());
            case NULL_VALUE:
                return new JsonPrimitive("");  // Handle nulls if required
            case LIST_VALUE:
                // If the value is a list, recursively convert each element
                JsonArray jsonArray = new JsonArray();
                for (JsonWithInt.Value listItem : value.getListValue().getValuesList()) {
                    jsonArray.add(convertValueToJsonElement(listItem));
                }
                return jsonArray;
            case STRUCT_VALUE:
                // If the value is a struct (map), convert it to a JsonObject
                JsonObject jsonObject = new JsonObject();
                for (Map.Entry<String, JsonWithInt.Value> entry : value.getStructValue().getFieldsMap().entrySet()) {
                    jsonObject.add(entry.getKey(), convertValueToJsonElement(entry.getValue()));
                }
                return jsonObject;
            default:
                throw new QdrantConnectorException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "Unexpected value: " + value); // Handle unexpected or unsupported cases
        }
    }
}
