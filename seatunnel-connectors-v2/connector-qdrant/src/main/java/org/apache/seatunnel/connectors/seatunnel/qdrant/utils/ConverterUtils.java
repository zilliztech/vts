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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.qdrant.client.grpc.JsonWithInt;
import io.qdrant.client.grpc.Points;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import static org.apache.seatunnel.api.table.catalog.PrimaryKey.isPrimaryKeyField;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.qdrant.exception.QdrantConnectorException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ConverterUtils {

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

            if(Objects.equals(fieldName, CommonOptions.METADATA.getName())){
                JsonObject data = new JsonObject();
                for (String entry : payloadMap.keySet()) {
                    data.add(entry, convertValueToJsonElement(payloadMap.get(entry)));
                }
                fields[fieldIndex] = data.toString();
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
                case BINARY_VECTOR:
                case FLOAT16_VECTOR:
                case BFLOAT16_VECTOR:
                    List<Float> list = vector.getDataList();
                    Float[] vectorArray = new Float[list.size()];
                    list.toArray(vectorArray);
                    fields[fieldIndex] = BufferUtils.toByteBuffer(vectorArray);
                    break;
                case SPARSE_FLOAT_VECTOR:
                    Map<Long, Float> sparseMap = new HashMap<>();
                    Points.SparseIndices sparseIndices = vector.getIndices();
                    for (int i = 0; i < sparseIndices.getDataCount(); i++) {
                        sparseMap.put((long) sparseIndices.getData(i), vector.getData(i));
                    }
                    fields[fieldIndex] = sparseMap;
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
            case DOUBLE_VALUE:
                return new JsonPrimitive(value.getDoubleValue());
            case KIND_NOT_SET:
                return new JsonPrimitive("");  // Handle unset values if required
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
