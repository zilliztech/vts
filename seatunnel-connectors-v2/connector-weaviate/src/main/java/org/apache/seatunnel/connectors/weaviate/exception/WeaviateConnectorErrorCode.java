package org.apache.seatunnel.connectors.weaviate.exception;

import lombok.Getter;
import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

@Getter
public enum WeaviateConnectorErrorCode implements SeaTunnelErrorCode {
    FAILED_TO_CONNECT_WEAVIATE("Weaviate-01", "Failed to connect to Weaviate instance"),
    RESPONSE_ERROR("Weaviate-02", "Error in response from Weaviate"),
    AUTHENTICATION_ERROR("Weaviate-03", "Authentication failed for Weaviate instance"),
    UNSUPPORTED_DATA_TYPE("Weaviate-04", "Unsupported data type encountered")
    ;

    private final String code;
    private final String description;

    WeaviateConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }
}
