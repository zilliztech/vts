package org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils;

import com.azure.identity.DefaultAzureCredentialBuilder;
import io.milvus.bulkwriter.connect.AzureConnectParam;
import io.milvus.bulkwriter.connect.GcpMetadataServerCredentialsProvider;
import io.milvus.bulkwriter.connect.S3ConnectParam;
import io.milvus.bulkwriter.connect.StorageConnectParam;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.external.dto.StageBucket;

import java.util.Objects;

/**
 * Builds the milvus bulk writer's {@link StorageConnectParam} from a {@link StageBucket}.
 * Two authentication modes are supported:
 *
 * <ul>
 *   <li>static credentials: access_key/secret_key (S3-style) or account name/key
 *       (Azure connection string), as provided in the stage bucket config;</li>
 *   <li>workload identity: {@code use_workload_identity} is set and no static credentials
 *       are provided, so credentials are minted from the identity of the workload running
 *       the writer (see {@link WorkloadIdentityCredentials}).</li>
 * </ul>
 */
public class StorageConnectParamFactory {

    public static StorageConnectParam create(StageBucket stageBucket) {
        if (isAzure(stageBucket.getCloudId())) {
            return useWorkloadIdentity(stageBucket)
                    ? azureWithWorkloadIdentity(stageBucket)
                    : azureWithAccountKey(stageBucket);
        }
        return useWorkloadIdentity(stageBucket)
                ? s3WithWorkloadIdentity(stageBucket)
                : s3WithStaticKeys(stageBucket);
    }

    private static boolean useWorkloadIdentity(StageBucket stageBucket) {
        return Boolean.TRUE.equals(stageBucket.getUseWorkloadIdentity());
    }

    private static boolean isAzure(String cloudId) {
        return Objects.equals(cloudId, "az") || Objects.equals(cloudId, "azure");
    }

    private static StorageConnectParam azureWithAccountKey(StageBucket stageBucket) {
        String connectionStr = "DefaultEndpointsProtocol=https;AccountName=" + stageBucket.getAccessKey()
                + ";AccountKey=" + stageBucket.getSecretKey() + ";EndpointSuffix=core.windows.net";
        return AzureConnectParam.newBuilder()
                .withConnStr(connectionStr)
                .withContainerName(stageBucket.getBucketName())
                .build();
    }

    private static StorageConnectParam azureWithWorkloadIdentity(StageBucket stageBucket) {
        return AzureConnectParam.newBuilder()
                .withAccountUrl(azureAccountUrl(stageBucket))
                .withCredential(new DefaultAzureCredentialBuilder().build())
                .withContainerName(stageBucket.getBucketName())
                .build();
    }

    // the storage account name is not a secret and travels in access_key; when it is
    // absent, minio_url is expected to carry the full account url instead
    private static String azureAccountUrl(StageBucket stageBucket) {
        if (stageBucket.getAccessKey() != null && !stageBucket.getAccessKey().isEmpty()) {
            return "https://" + stageBucket.getAccessKey() + ".blob.core.windows.net";
        }
        if (stageBucket.getMinioUrl() != null && stageBucket.getMinioUrl().startsWith("http")) {
            return stageBucket.getMinioUrl();
        }
        throw new MilvusConnectorException(MilvusConnectionErrorCode.INIT_WRITER_ERROR,
                "use_workload_identity is set but the storage account is unknown: "
                        + "access_key (account name) and minio_url (account url) are both empty");
    }

    private static StorageConnectParam s3WithStaticKeys(StageBucket stageBucket) {
        return s3Builder(stageBucket)
                .withAccessKey(stageBucket.getAccessKey())
                .withSecretKey(stageBucket.getSecretKey())
                .build();
    }

    private static StorageConnectParam s3WithWorkloadIdentity(StageBucket stageBucket) {
        S3ConnectParam.Builder builder = s3Builder(stageBucket);
        // self-refreshing providers: a long-running writer never touches an expired
        // credential, and no fixed session duration has to be negotiated with the
        // customer role
        if (Objects.equals(stageBucket.getCloudId(), "gcp")) {
            // the bulk writer sends the token as a Bearer credential against the GCS XML API
            return builder.withCredentialsProvider(new GcpMetadataServerCredentialsProvider()).build();
        }
        return builder.withCredentialsProvider(
                WorkloadIdentityCredentials.awsWebIdentityProvider(stageBucket.getRegionId())).build();
    }

    private static S3ConnectParam.Builder s3Builder(StageBucket stageBucket) {
        return S3ConnectParam.newBuilder()
                .withEndpoint(stageBucket.getMinioUrl())
                .withRegion(stageBucket.getRegionId())
                .withBucketName(stageBucket.getBucketName())
                .withCloudName(stageBucket.getCloudId());
    }
}
