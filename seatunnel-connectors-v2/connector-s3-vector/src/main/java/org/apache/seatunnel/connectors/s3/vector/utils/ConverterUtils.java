package org.apache.seatunnel.connectors.s3.vector.utils;

import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.s3.vector.exception.S3VectorConnectorErrorCode;
import org.apache.seatunnel.connectors.s3.vector.exception.S3VectorConnectorException;
import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.services.s3vectors.model.ListOutputVector;

import java.util.Map;
import java.util.Objects;

public class ConverterUtils {
    public static final String VECTOR_KEY = "vector";

    public static SeaTunnelRow convertToSeaTunnelRow(TableSchema tableSchema, ListOutputVector vector) {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[typeInfo.getTotalFields()];
        String[] fieldNames = typeInfo.getFieldNames();

        Map<String, Document> document = vector.metadata().asMap();
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            String fieldName = fieldNames[fieldIndex];

            if (PrimaryKey.isPrimaryKeyField(tableSchema.getPrimaryKey(), fieldName)) {
                fields[fieldIndex] = vector.key();
            }

            if (document.containsKey(fieldName)) {
                SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
                switch (seaTunnelDataType.getSqlType()) {
                    case STRING:
                        fields[fieldIndex] = document.get(fieldName).asString();
                        break;
                    case DOUBLE:
                        fields[fieldIndex] = document.get(fieldName).asNumber().doubleValue();
                        break;
                    case BOOLEAN:
                        fields[fieldIndex] = document.get(fieldName).asBoolean();
                        break;
                    default:
                        throw new S3VectorConnectorException(S3VectorConnectorErrorCode.UNSUPPORTED_DATA_TYPE, "Unexpected value: " + seaTunnelDataType.getSqlType().name());
                }
            }

            if (VECTOR_KEY.equals(fieldName) && vector.data().hasFloat32()) {
                SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
                if (Objects.requireNonNull(seaTunnelDataType.getSqlType()) == SqlType.FLOAT_VECTOR) {
                    fields[fieldIndex] = BufferUtils.toByteBuffer(vector.data().float32().toArray(new Float[0]));
                } else {
                    throw new S3VectorConnectorException(
                            S3VectorConnectorErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType.getSqlType().name());
                }
            }
        }

        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setRowKind(RowKind.INSERT);

        return row;
    }
}
