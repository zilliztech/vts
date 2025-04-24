package org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils;

import com.google.gson.Gson;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.common.StageBucket;

import java.util.Map;

public class StageHelper {
    public static StageBucket getStageBucket(Map<String, String> bulkConfig) {
        if(bulkConfig.isEmpty()){
            return null;
        }
        Gson gson = new Gson();
        return gson.fromJson(gson.toJson(bulkConfig), StageBucket.class);
    }
}
