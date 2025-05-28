package org.apache.seatunnel.connectors.seatunnel.milvus.sink.common;

import lombok.Getter;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import java.net.URI;
import java.net.URISyntaxException;

@Getter
public enum VTSAPI {
    GLOBAL("https://cloud-dm-api.cloud.zilliz.com"),
    CHINA("https://cloud-dm-api.cloud.zilliz.com.cn"),
    GLOBAL_UAT("https://cloud-dm-api.cloud-uat3.zilliz.com"),
    CHINA_UAT("https://cloud-dm-api.cloud-uat.zilliz.cn"),
    GLOBAL_SIT("https://cloud-dm-api.cloud-sit.zilliz.com");

    private String url;
    VTSAPI(String url) {
        this.url = url;
    }

    public static String getVTSAPI(String endpoint) {
        URI uri = null;
        try {
            uri = new URI(endpoint);
        } catch (URISyntaxException e) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.INVALID_PARAM, "Invalid url");
        }
        String url = uri.getHost();
        if(url.contains("sit")){
            return VTSAPI.GLOBAL_SIT.getUrl();
        } else if(url.contains("uat") && url.endsWith("com")){
            return VTSAPI.GLOBAL_UAT.getUrl();
        } else if(url.contains("uat") && url.endsWith("cn")){
            return VTSAPI.CHINA_UAT.getUrl();
        } else if(url.endsWith("com")) {
            return VTSAPI.GLOBAL.getUrl();
        } else if (url.endsWith("cn")) {
            return VTSAPI.CHINA.getUrl();
        }
        return null;
    }
}
