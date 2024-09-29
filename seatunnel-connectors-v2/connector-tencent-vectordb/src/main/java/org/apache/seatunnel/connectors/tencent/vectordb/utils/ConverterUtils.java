package org.apache.seatunnel.connectors.tencent.vectordb.utils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.tencent.tcvectordb.model.DocField;
import com.tencent.tcvectordb.model.Document;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.BufferUtils;

import java.util.List;
import java.util.Map;

public class ConverterUtils {
    public static SeaTunnelRow convertToSeatunnelRow(Document vector) {
        Object[] fields = new Object[3];
        fields[0] = vector.getId();

        // Convert each Double to Float
        Float[] arrays = vector.getVector().stream().map(Double::floatValue).toArray(Float[]::new);
        fields[1] = BufferUtils.toByteBuffer(arrays);
        List<DocField> meta = vector.getDocFields();
        JsonObject data = new JsonObject();
        for (DocField entry : meta) {
            data.add(entry.getName(), convertValueToJsonElement(entry.getValue()));
        }
        fields[2] = data;
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
        seaTunnelRow.setRowKind(RowKind.INSERT);
        return seaTunnelRow;
    }

    private static JsonElement convertValueToJsonElement(Object value) {
        Gson gson = new Gson();
        return gson.toJsonTree(value);
    }
}
