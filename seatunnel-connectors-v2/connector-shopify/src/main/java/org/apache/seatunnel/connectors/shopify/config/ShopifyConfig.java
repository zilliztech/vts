package org.apache.seatunnel.connectors.shopify.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class ShopifyConfig {
    public static final String CONNECTOR_IDENTITY = "Shopify";

    public static final Option<String> ACCESS_TOKEN =
            Options.key("access_token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("token for authentication");

    public static final Option<String> SHOP_URL =
            Options.key("shop_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Shopify store URL");

    public static final Option<String> API_KEY_OPENAI =
            Options.key("api_key_openai")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Open AI key for embedding");
}
