package org.apache.seatunnel.connectors.seatunnel.milvus.sink.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusCommonConfig;

public class MilvusBucketConfig extends MilvusCommonConfig {
    public static final Option<String> INSTANCE_ID = Options.key("instance_id")
            .stringType()
            .noDefaultValue()
            .withDescription("instance_id");
    public static final Option<String> API_KEY = Options.key("api_key")
            .stringType()
            .noDefaultValue()
            .withDescription("api_key");
    public static final Option<String> MINIO_URL =
            Options.key("minio_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("minio_url");
    public static final Option<String> ACCESS_KEY = Options.key("access_key")
            .stringType()
            .noDefaultValue()
            .withDescription("access_key");;
    public static final Option<String> SECRET_KEY = Options.key("secret_key")
            .stringType()
            .noDefaultValue()
            .withDescription("secret_key");;
    public static final Option<String> BUCKET_NAME = Options.key("bucket_name")
            .stringType()
            .noDefaultValue()
            .withDescription("bucket_name");
    public static final Option<String> PREFIX = Options.key("prefix")
            .stringType()
            .defaultValue("vts")
            .withDescription("path to store data");
}
