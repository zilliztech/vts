package org.apache.seatunnel.connectors.s3.vector.exception;

import lombok.Getter;
import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

@Getter
public enum S3VectorConnectorErrorCode implements SeaTunnelErrorCode {
    UNSUPPORTED_DATA_TYPE("S3Vector-01", "Unsupported data type"),
    RESPONSE_ERROR("S3Vector-02", "Error in response from S3 Vector service"),
    ;

    private final String code;
    private final String description;

    S3VectorConnectorErrorCode(String s, String s1) {
        this.code = s;
        this.description = s1;
    }
}
