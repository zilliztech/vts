package org.apache.seatunnel.connectors.seatunnel.milvus.sink.common;

import lombok.Getter;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import java.net.URI;
import java.net.URISyntaxException;

@Getter
public enum ControllerAPI {
    GLOBAL("https://api.cloud.zilliz.com"),
    CHINA("https://api.cloud.zilliz.com.cn"),
    GLOBAL_UAT("https://api.cloud-uat3.zilliz.com"),
    CHINA_UAT("https://api.cloud-uat3.zilliz.com.cn"),
    GLOBAL_SIT("https://api.cloud-sit.zilliz.com");

    private String url;
    ControllerAPI(String url) {
        this.url = url;
    }

    public static String getControllerAPI(String endpoint) {
        URI uri = null;
        try {
            uri = new URI(endpoint);
        } catch (URISyntaxException e) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.INVALID_PARAM, "Invalid url");
        }
        String url = uri.getHost();
        if(url.contains("sit")){
            return ControllerAPI.GLOBAL_SIT.getUrl();
        } else if(url.contains("uat") && url.endsWith("com")){
            return ControllerAPI.GLOBAL_UAT.getUrl();
        } else if(url.contains("uat") && url.endsWith("cn")){
            return ControllerAPI.CHINA_UAT.getUrl();
        } else if(url.endsWith("com")) {
            return ControllerAPI.GLOBAL.getUrl();
        } else if (url.endsWith("cn")) {
            return ControllerAPI.CHINA.getUrl();
        }
        return null;
    }
}
