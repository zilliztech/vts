package org.apache.seatunnel.connectors.shopify.client.resp;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.seatunnel.connectors.shopify.client.Product;

import java.util.List;

@Data
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
public class ListProductResp {
    private List<Product> productList;
    private String afterCursor;
}
