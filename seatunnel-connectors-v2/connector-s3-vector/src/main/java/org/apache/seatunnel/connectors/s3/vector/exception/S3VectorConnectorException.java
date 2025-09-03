package org.apache.seatunnel.connectors.s3.vector.exception;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

public class S3VectorConnectorException extends SeaTunnelRuntimeException {
    public S3VectorConnectorException(S3VectorConnectorErrorCode code, String message) {
        super(code, message);
    }
}
