package org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils;

import io.milvus.bulkwriter.connect.GcpMetadataServerCredentialsProvider;
import io.milvus.bulkwriter.connect.S3ConnectParam;
import io.milvus.bulkwriter.connect.StorageConnectParam;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.external.dto.StageBucket;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WorkloadIdentityCredentialsTest {

    @Test
    public void sessionDurationDefaultsToOneHour() {
        Assertions.assertEquals(3600,
                WorkloadIdentityCredentials.parseSessionDurationSeconds(null));
        Assertions.assertEquals(3600,
                WorkloadIdentityCredentials.parseSessionDurationSeconds("  "));
    }

    @Test
    public void sessionDurationAcceptsPositiveInteger() {
        Assertions.assertEquals(7200, WorkloadIdentityCredentials.parseSessionDurationSeconds("7200"));
    }

    @Test
    public void sessionDurationRejectsGarbage() {
        Assertions.assertThrows(MilvusConnectorException.class,
                () -> WorkloadIdentityCredentials.parseSessionDurationSeconds("abc"));
        Assertions.assertThrows(MilvusConnectorException.class,
                () -> WorkloadIdentityCredentials.parseSessionDurationSeconds("0"));
        Assertions.assertThrows(MilvusConnectorException.class,
                () -> WorkloadIdentityCredentials.parseSessionDurationSeconds("-100"));
    }

    @Test
    public void gcpWorkloadIdentityUsesMetadataServerProvider() {
        StageBucket stageBucket = StageBucket.builder()
                .cloudId("gcp")
                .regionId("us-west1")
                .bucketName("bucket")
                .minioUrl("storage.googleapis.com")
                .useWorkloadIdentity(true)
                .build();
        StorageConnectParam param = StorageConnectParamFactory.create(stageBucket);
        Assertions.assertTrue(param instanceof S3ConnectParam);
        Assertions.assertTrue(((S3ConnectParam) param).getCredentialsProvider()
                instanceof GcpMetadataServerCredentialsProvider);
    }

    @Test
    public void awsWorkloadIdentityFailsFastWithoutIrsaEnv() {
        // no AWS_ROLE_ARN/AWS_WEB_IDENTITY_TOKEN_FILE in the test environment
        StageBucket stageBucket = StageBucket.builder()
                .cloudId("aws")
                .regionId("us-west-2")
                .bucketName("bucket")
                .minioUrl("s3.us-west-2.amazonaws.com")
                .useWorkloadIdentity(true)
                .build();
        Assertions.assertThrows(MilvusConnectorException.class,
                () -> StorageConnectParamFactory.create(stageBucket));
    }

    @Test
    public void staticKeysStillWork() {
        StageBucket stageBucket = StageBucket.builder()
                .cloudId("aws")
                .regionId("us-west-2")
                .bucketName("bucket")
                .minioUrl("s3.us-west-2.amazonaws.com")
                .accessKey("ak")
                .secretKey("sk")
                .build();
        S3ConnectParam param = (S3ConnectParam) StorageConnectParamFactory.create(stageBucket);
        Assertions.assertNull(param.getCredentialsProvider());
        Assertions.assertEquals("ak", param.getAccessKey());
        Assertions.assertEquals("sk", param.getSecretKey());
    }
}
