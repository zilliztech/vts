package org.apache.seatunnel.connectors.seatunnel.milvus.utils.sink;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import io.milvus.common.utils.JacksonUtils;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Collectors;

public class MilvusSinkConverter {
    private static final Gson gson = new Gson();

    public Object convertBySeaTunnelType(SeaTunnelDataType<?> fieldType, Object value) {
        SqlType sqlType = fieldType.getSqlType();
        switch (sqlType) {
            case INT:
                return Integer.parseInt(value.toString());
            case TINYINT:
                return Byte.parseByte(value.toString());
            case BIGINT:
                return Long.parseLong(value.toString());
            case SMALLINT:
                return Short.parseShort(value.toString());
            case STRING:
            case DATE:
                return value.toString();
            case JSON:
                return value;
            case FLOAT_VECTOR:
                ByteBuffer floatVectorBuffer = (ByteBuffer) value;
                Float[] floats = BufferUtils.toFloatArray(floatVectorBuffer);
                return Arrays.stream(floats).collect(Collectors.toList());
            case BINARY_VECTOR:
            case BFLOAT16_VECTOR:
            case FLOAT16_VECTOR:
                ByteBuffer binaryVector = (ByteBuffer) value;
                return gson.toJsonTree(binaryVector.array());
            case SPARSE_FLOAT_VECTOR:
                return JsonParser.parseString(JacksonUtils.toJsonString(value)).getAsJsonObject();
            case FLOAT:
                return Float.parseFloat(value.toString());
            case BOOLEAN:
                return Boolean.parseBoolean(value.toString());
            case DOUBLE:
                return Double.parseDouble(value.toString());
            case ARRAY:
                ArrayType<?, ?> arrayType = (ArrayType<?, ?>) fieldType;
                switch (arrayType.getElementType().getSqlType()) {
                    case STRING:
                        String[] stringArray = (String[]) value;
                        return Arrays.asList(stringArray);
                    case SMALLINT:
                        Short[] shortArray = (Short[]) value;
                        return Arrays.asList(shortArray);
                    case TINYINT:
                        Byte[] byteArray = (Byte[]) value;
                        return Arrays.asList(byteArray);
                    case INT:
                        Integer[] intArray = (Integer[]) value;
                        return Arrays.asList(intArray);
                    case BIGINT:
                        Long[] longArray = (Long[]) value;
                        return Arrays.asList(longArray);
                    case FLOAT:
                        Float[] floatArray = (Float[]) value;
                        return Arrays.asList(floatArray);
                    case DOUBLE:
                        Double[] doubleArray = (Double[]) value;
                        return Arrays.asList(doubleArray);
                }
            case ROW:
                SeaTunnelRow row = (SeaTunnelRow) value;
                return JsonUtils.toJsonString(row.getFields());
            case MAP:
                return JacksonUtils.toJsonString(value);
            default:
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.NOT_SUPPORT_TYPE, sqlType.name());
        }
    }
}
