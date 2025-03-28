// Product.java
package org.apache.seatunnel.connectors.shopify.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.seatunnel.connectors.shopify.client.resp.ListProductResp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShopifyGraphQLClient {
    private final String shopUrl;
    private final String accessToken;
    private final OkHttpClient client;
    private final ObjectMapper objectMapper;

    public ShopifyGraphQLClient(String shopUrl, String accessToken) {
        this.shopUrl = shopUrl.replaceAll("/$", "");
        this.accessToken = accessToken;
        this.client = new OkHttpClient();
        this.objectMapper = new ObjectMapper();
    }

    private static final String PRODUCTS_QUERY =
            "query GetProducts($first: Int!, $after: String) {" +
                    "  products(first: $first, after: $after) {" +
                    "    pageInfo {" +
                    "      hasNextPage" +
                    "      endCursor" +
                    "    }" +
                    "    edges {" +
                    "      node {" +
                    "        id" +
                    "        title" +
                    "        handle" +
                    "        description" +
                    "        productType" +
                    "        vendor" +
                    "        status" +
                    "        createdAt" +
                    "        updatedAt" +
                    "        publishedAt" +
                    "        tags" +
                    "        priceRangeV2 {" +
                    "          minVariantPrice {" +
                    "            amount" +
                    "            currencyCode" +
                    "          }" +
                    "          maxVariantPrice {" +
                    "            amount" +
                    "            currencyCode" +
                    "          }" +
                    "        }" +
                    "        variants(first: 10) {" +
                    "          edges {" +
                    "            node {" +
                    "              id" +
                    "              title" +
                    "              sku" +
                    "              price" +
                    "              compareAtPrice" +
                    "              inventoryQuantity" +
                    "              selectedOptions {" +
                    "                name" +
                    "                value" +
                    "              }" +
                    "            }" +
                    "          }" +
                    "        }" +
                    "        images(first: 10) {" +
                    "          edges {" +
                    "            node {" +
                    "              id" +
                    "              url" +
                    "              altText" +
                    "            }" +
                    "          }" +
                    "        }" +
                    "      }" +
                    "    }" +
                    "  }" +
                    "}";

    public ListProductResp listProducts(String afterCursor, Integer limit) throws IOException {
        List<Product> productList = new ArrayList<>();

        JsonNode response = executeQuery(PRODUCTS_QUERY, limit, afterCursor);
        JsonNode products = response.path("data").path("products");

        for (JsonNode edge : products.path("edges")) {
            JsonNode node = edge.path("node");
            Product product = parseProduct(node);
            productList.add(product);
        }

        JsonNode pageInfo = products.path("pageInfo");
        afterCursor = pageInfo.path("hasNextPage").asBoolean() ? pageInfo.path("endCursor").asText() : null;


        return ListProductResp.builder()
                .productList(productList)
                .afterCursor(afterCursor)
                .build();
    }

    private JsonNode executeQuery(String query, Integer limit, String afterCursor) throws IOException {
        // Create variables object using ObjectMapper for proper JSON formatting
        ObjectNode variables = objectMapper.createObjectNode();
        variables.put("first", limit);
        variables.put("after", afterCursor);

        // Create the complete request body
        ObjectNode requestBody = objectMapper.createObjectNode();
        requestBody.put("query", query);
        requestBody.set("variables", variables);

        // Convert to JSON string
        String jsonBody = objectMapper.writeValueAsString(requestBody);

        // Create request body
        RequestBody body = RequestBody.create(
                jsonBody,
                MediaType.get("application/json")
        );

        // Build and execute request
        Request request = new Request.Builder()
                .url(String.format("https://%s/admin/api/2024-01/graphql.json", shopUrl))
                .addHeader("X-Shopify-Access-Token", accessToken)
                .post(body)
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected response " + response);
            }
            String responseBody = response.body().string();
            return objectMapper.readTree(responseBody);
        }
    }

    private Product parseProduct(JsonNode node) {
        Product product = new Product();
        product.setId(node.path("id").asText());
        product.setTitle(node.path("title").asText());
        product.setHandle(node.path("handle").asText());
        product.setDescription(node.path("description").asText());
        product.setProductType(node.path("productType").asText());
        product.setVendor(node.path("vendor").asText());
        product.setStatus(node.path("status").asText());

        // Parse variants
        List<Variant> variants = new ArrayList<>();
        node.path("variants").path("edges").forEach(variantEdge -> {
            JsonNode variantNode = variantEdge.path("node");
            Variant variant = new Variant();
            variant.setId(variantNode.path("id").asText());
            variant.setTitle(variantNode.path("title").asText());
            variant.setSku(variantNode.path("sku").asText());
            variant.setPrice(variantNode.path("price").asText());
            variant.setCompareAtPrice(variantNode.path("compareAtPrice").asText());
            variant.setInventoryQuantity(Integer.valueOf(variantNode.path("inventoryQuantity").asInt()));

            List<SelectedOption> options = new ArrayList<>();
            variantNode.path("selectedOptions").forEach(optionNode -> {
                SelectedOption option = new SelectedOption();
                option.setName(optionNode.path("name").asText());
                option.setValue(optionNode.path("value").asText());
                options.add(option);
            });
            variant.setSelectedOptions(options);
            variants.add(variant);
        });
        product.setVariants(variants);

        // Parse images
        List<Image> images = new ArrayList<>();
        node.path("images").path("edges").forEach(imageEdge -> {
            JsonNode imageNode = imageEdge.path("node");
            Image image = new Image();
            image.setId(imageNode.path("id").asText());
            image.setUrl(imageNode.path("url").asText());
            image.setAltText(imageNode.path("altText").asText());
            images.add(image);
        });
        product.setImages(images);

        // Parse price range
        PriceRange priceRange = new PriceRange();
        JsonNode priceRangeNode = node.path("priceRangeV2");

        Price minPrice = new Price();
        minPrice.setAmount(priceRangeNode.path("minVariantPrice").path("amount").asText());
        minPrice.setCurrencyCode(priceRangeNode.path("minVariantPrice").path("currencyCode").asText());

        Price maxPrice = new Price();
        maxPrice.setAmount(priceRangeNode.path("maxVariantPrice").path("amount").asText());
        maxPrice.setCurrencyCode(priceRangeNode.path("maxVariantPrice").path("currencyCode").asText());

        priceRange.setMinVariantPrice(minPrice);
        priceRange.setMaxVariantPrice(maxPrice);
        product.setPriceRange(priceRange);

        return product;
    }
}