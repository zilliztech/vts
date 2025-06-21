package org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils;

import com.google.gson.reflect.TypeToken;
import io.milvus.bulkwriter.response.RestfulResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import kong.unirest.HttpResponse;
import kong.unirest.Unirest;

import org.apache.commons.lang3.StringUtils;
import io.milvus.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.external.api.VTSAPI;
import org.apache.seatunnel.connectors.seatunnel.milvus.external.dto.SentEventRequest;
import org.apache.seatunnel.connectors.seatunnel.milvus.external.dto.VTSJob;

@Slf4j
public class EventHelper {
    private final String baseUrl;
    private final String jobId;
    public EventHelper(String url, String jobId) {
        this.baseUrl = VTSAPI.getVTSAPI(url);
        this.jobId = jobId;
    }

    public void noticeSuccess(String collectionName, Map<String, String> errorMap) {
        if(StringUtils.isEmpty(jobId)){
            log.error("jobId is empty");
            return;
        }
        String requestURL = baseUrl + "/dm/v1/vts/sent_event";
        VTSJob vtsJob = VTSJob.builder()
            .collectionName(collectionName)
            .skippedData(new ArrayList<>(errorMap.keySet()))
            .build();
        SentEventRequest sentEventRequest = SentEventRequest.builder()
            .jobId(jobId)
            .eventType(0)
            .eventData(JsonUtils.toJson(vtsJob))
            .build();
        HttpResponse<String> body = Unirest.post(requestURL)
                .connectTimeout(60000)
                .headers(httpHeaders(null))
                .body(JsonUtils.toJson(sentEventRequest))
                .asString();
        if (body.getStatus() != 200) {
            log.error("Failed to call sent event api, response: {}", body.getBody());
            return;
        }
        RestfulResponse<String> response =  JsonUtils.fromJson(body.getBody(), (new TypeToken<RestfulResponse<String>>() {
        }).getType());
        if (response.getCode() != 0) {
            log.error("Failed to send event to VTS service, response: {}", response);
        }
    }
    protected static Map<String, String> httpHeaders(String apiKey) {
        Map<String, String> header = new HashMap<>();
        header.put("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11");
        header.put("Accept", "application/json");
        header.put("Content-Type", "application/json");
        header.put("Accept-Encodin", "gzip,deflate,sdch");
        header.put("Accept-Languag", "en-US,en;q=0.5");
        if (StringUtils.isNotEmpty(apiKey)) {
            header.put("Authorization", "Bearer " + apiKey);
        }

        return header;
    }
}
