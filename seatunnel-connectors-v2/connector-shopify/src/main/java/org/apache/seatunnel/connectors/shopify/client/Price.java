package org.apache.seatunnel.connectors.shopify.client;

import lombok.Data;

// Price.java
@Data
public class Price {
    private String amount;
    private String currencyCode;
}
