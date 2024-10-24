package org.apache.seatunnel.connectors.shopify.client;

import lombok.Data;

import java.util.List;

// Variant.java
@Data
public class Variant {
    private String id;
    private String title;
    private String sku;
    private String price;
    private String compareAtPrice;
    private Integer inventoryQuantity;
    private List<SelectedOption> selectedOptions;
}
