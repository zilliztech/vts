package org.apache.seatunnel.connectors.pinecone.utils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.pinecone.proto.Vector;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.List;
import java.util.Map;

public class ConverterUtils {
    public static SeaTunnelRow convertToSeatunnelRow(Vector vector) {
        Object[] fields = new Object[3];
        fields[0] = vector.getId();
        List<Float> floats = vector.getValuesList();
        Float[] arrays = floats.toArray(new Float[0]);
        fields[1] = arrays;
        Struct meta = vector.getMetadata();
        JsonObject data = new JsonObject();
        for (Map.Entry<String, Value> entry : meta.getFieldsMap().entrySet()) {
            data.add(entry.getKey(), convertValueToJsonElement(entry.getValue()));
        }
        fields[2] = data;
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
        seaTunnelRow.setRowKind(RowKind.INSERT);
        return seaTunnelRow;
    }

    private static JsonElement convertValueToJsonElement(Value value) {
        Gson gson = new Gson();
        switch (value.getKindCase()) {
            case NULL_VALUE:
                return gson.toJsonTree(null);  // Null value
            case NUMBER_VALUE:
                return gson.toJsonTree(value.getNumberValue());  // Double value
            case STRING_VALUE:
                return gson.toJsonTree(value.getStringValue());  // String value
            case BOOL_VALUE:
                return gson.toJsonTree(value.getBoolValue());  // Boolean value
            case STRUCT_VALUE:
                // Convert Struct to a JsonObject
                JsonObject structJson = new JsonObject();
                value.getStructValue().getFieldsMap().forEach((k, v) ->
                        structJson.add(k, convertValueToJsonElement(v))
                );
                return structJson;
            case LIST_VALUE:
                // Convert List to a JsonArray
                return gson.toJsonTree(
                        value.getListValue().getValuesList().stream()
                                .map(ConverterUtils::convertValueToJsonElement)
                                .toArray()
                );
            default:
                return gson.toJsonTree(null);  // Default or unsupported case
        }
    }
}
