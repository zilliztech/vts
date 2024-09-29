package org.apache.seatunnel.connectors.astradb.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class AstraDBSourceConfig {
    public static final String CONNECTOR_IDENTITY = "AstraDB";

    public static final Option<String> API_KEY =
            Options.key("api_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Pinecone token for authentication");

    public static final Option<String> API_ENDPOINT =
            Options.key("api_endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Pinecone token for authentication");

    public static final Option<String> INDEX =
            Options.key("index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Pinecone index name");
}
