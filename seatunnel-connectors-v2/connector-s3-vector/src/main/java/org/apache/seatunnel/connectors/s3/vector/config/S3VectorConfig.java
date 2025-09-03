package org.apache.seatunnel.connectors.s3.vector.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class S3VectorConfig {
    public static final String CONNECTOR_IDENTITY = "S3Vector";

    public static final Option<String> REGION =
            Options.key("region")
                    .stringType()
                    .defaultValue("us-west-2")
                    .withDescription("The AWS region of the S3 Vector service, default is us-west-2");

    public static final Option<String> AK =
            Options.key("ak")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Access Key ID of the AWS account, must be specified");

    public static final Option<String> SK =
            Options.key("sk")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Secret Access Key of the AWS account, must be specified");

    public static final Option<String> INDEX_NAME =
            Options.key("index_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("S3 Vector index name, must be specified");

    public static final Option<String> VECTOR_BUCKET_NAME =
            Options.key("vector_bucket_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("S3 bucket name where vectors are stored, must be specified");
}
