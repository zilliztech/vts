package org.apache.seatunnel.connectors.seatunnel.qdrant.exception;

import lombok.Getter;
import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

@Getter
public enum QdrantConnectionErrorCode implements SeaTunnelErrorCode {
    FAILED_CONNECT_QDRANT("QDRANT-01", "Failed to connect to Qdrant"),
    EMPTY_COLLECTION("QDRANT-02", "No data in collection" );

    private final String code;
    private final String description;

    QdrantConnectionErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }
}
