package org.apache.seatunnel.connectors.weaviate.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class WeaviateConfig {
    public static final String CONNECTOR_IDENTITY = "Weaviate";
    public static final Option<String> SCHEME =
            Options.key("scheme")
                    .stringType()
                    .defaultValue("http")
                    .withDescription("Weaviate connection scheme, http or https");

    public static final Option<String> HOST =
            Options.key("host")
                    .stringType()
                    .defaultValue("localhost:8080")
                    .withDescription("Weaviate host and port in the format host:port");

    public static final Option<String> API_KEY =
            Options.key("api_key")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Weaviate API key for authentication, if required");

    public static final Option<String> CLASS_NAME =
            Options.key("class_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Weaviate class name to connect to, must be specified");
}

