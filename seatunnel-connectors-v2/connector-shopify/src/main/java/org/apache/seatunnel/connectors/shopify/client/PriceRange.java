package org.apache.seatunnel.connectors.shopify.client;

import lombok.Data;

// PriceRange.java
@Data
public class PriceRange {
    private Price minVariantPrice;
    private Price maxVariantPrice;
}
