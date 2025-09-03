package org.apache.seatunnel.connectors.s3.vector.config;

import lombok.Data;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3vectors.S3VectorsClient;

import java.io.Serializable;

@Data
public class S3VectorParameters implements Serializable {
    private String region;

    private String ak;
    private String sk;

    private String indexName;
    private String vectorBucketName;

    public S3VectorParameters(ReadonlyConfig config) {
        this.region = config.get(S3VectorConfig.REGION);

        this.ak = config.get(S3VectorConfig.AK);
        this.sk = config.get(S3VectorConfig.SK);

        this.indexName = config.get(S3VectorConfig.INDEX_NAME);
        this.vectorBucketName = config.get(S3VectorConfig.VECTOR_BUCKET_NAME);
    }

    public S3VectorsClient buildS3VectorClient() {
        AwsBasicCredentials credentials = AwsBasicCredentials.create(ak, sk);
        AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(credentials);
        return S3VectorsClient.builder().region(Region.of(region)).credentialsProvider(credentialsProvider).build();
    }

}
