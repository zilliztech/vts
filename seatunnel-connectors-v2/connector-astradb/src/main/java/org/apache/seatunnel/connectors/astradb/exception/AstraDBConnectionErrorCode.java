package org.apache.seatunnel.connectors.astradb.exception;

import lombok.Getter;
import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

@Getter
public enum AstraDBConnectionErrorCode implements SeaTunnelErrorCode {
    SOURCE_TABLE_SCHEMA_IS_NULL("PINECONE-01", "Source table schema is null"),
    READ_DATA_FAIL("PINECONE-02", "Read data fail");

    private final String code;
    private final String description;

    AstraDBConnectionErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }
}
