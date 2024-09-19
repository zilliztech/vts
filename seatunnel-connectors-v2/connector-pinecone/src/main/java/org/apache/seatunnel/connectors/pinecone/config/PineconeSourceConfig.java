package org.apache.seatunnel.connectors.pinecone.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class PineconeSourceConfig {
    public static final String CONNECTOR_IDENTITY = "Pinecone";

    public static final Option<String> API_KEY =
            Options.key("api_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Pinecone token for authentication");

    public static final Option<String> INDEX =
            Options.key("index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Pinecone index name");
}
