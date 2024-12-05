package org.apache.seatunnel.connectors.shopify.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

public class ShopifyConnectorException extends SeaTunnelRuntimeException {
    public ShopifyConnectorException(SeaTunnelErrorCode seaTunnelErrorCode) {
        super(seaTunnelErrorCode, seaTunnelErrorCode.getErrorMessage());
    }

    public ShopifyConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, Throwable cause) {
        super(seaTunnelErrorCode, seaTunnelErrorCode.getErrorMessage(), cause);
    }
}
