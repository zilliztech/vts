package org.apache.seatunnel.connectors.astradb.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

public class AstraDBConnectorException extends SeaTunnelRuntimeException {
    public AstraDBConnectorException(SeaTunnelErrorCode seaTunnelErrorCode) {
        super(seaTunnelErrorCode, seaTunnelErrorCode.getErrorMessage());
    }

    public AstraDBConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, Throwable cause) {
        super(seaTunnelErrorCode, seaTunnelErrorCode.getErrorMessage(), cause);
    }
}
