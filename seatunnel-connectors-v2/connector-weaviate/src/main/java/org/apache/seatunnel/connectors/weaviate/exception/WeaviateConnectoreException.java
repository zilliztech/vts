package org.apache.seatunnel.connectors.weaviate.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

public class WeaviateConnectoreException extends SeaTunnelRuntimeException {
    public WeaviateConnectoreException(SeaTunnelErrorCode errorCode, String message) {
        super(errorCode, message);
    }
}
