package org.apache.seatunnel.connectors.shopify.exception;

import lombok.Getter;
import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

@Getter
public enum ShopifyConnectionErrorCode implements SeaTunnelErrorCode {
    SOURCE_TABLE_SCHEMA_IS_NULL("Shopify-01", "Source table schema is null"),
    READ_DATA_FAIL("Shopify-02", "Read data fail");

    private final String code;
    private final String description;

    ShopifyConnectionErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }
}
