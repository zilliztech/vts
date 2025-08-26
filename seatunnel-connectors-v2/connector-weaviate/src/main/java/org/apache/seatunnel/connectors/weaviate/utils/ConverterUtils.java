package org.apache.seatunnel.connectors.weaviate.utils;

import io.weaviate.client.v1.data.model.WeaviateObject;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.weaviate.exception.WeaviateConnectoreException;

import java.util.Map;

public class ConverterUtils {
    public static final String VECTOR_KEY = "vector";

    public static SeaTunnelRow convertToSeaTunnelRow(TableSchema tableSchema, WeaviateObject weaviateObject) {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[typeInfo.getTotalFields()];
        String[] fieldNames = typeInfo.getFieldNames();

        Map<String, Object> properties = weaviateObject.getProperties();
        Map<String, Float[]> vectors = weaviateObject.getVectors();
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            String fieldName = fieldNames[fieldIndex];

            if (PrimaryKey.isPrimaryKeyField(tableSchema.getPrimaryKey(), fieldName)) {
                fields[fieldIndex] = weaviateObject.getId();
                continue;
            }

            if (properties.containsKey(fieldName)) {
                fields[fieldIndex] = properties.get(fieldName);
                continue;
            }

            Float[] vector;
            if (VECTOR_KEY.equals(fieldName) && weaviateObject.getVector() != null) {
                vector = weaviateObject.getVector();
            } else if (vectors != null && vectors.containsKey(fieldName)) {
                vector = vectors.get(fieldName);
            } else {
                continue;
            }

            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            switch (seaTunnelDataType.getSqlType()) {
                case FLOAT_VECTOR:
                case BINARY_VECTOR:
                case FLOAT16_VECTOR:
                case BFLOAT16_VECTOR:
                    fields[fieldIndex] = BufferUtils.toByteBuffer(vector);
                    break;
                default:
                    throw new WeaviateConnectoreException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType.getSqlType().name());
            }
        }

        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setRowKind(RowKind.INSERT);

        return row;
    }
}
