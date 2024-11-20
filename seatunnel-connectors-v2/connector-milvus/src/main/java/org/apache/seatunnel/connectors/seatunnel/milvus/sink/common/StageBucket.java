package org.apache.seatunnel.connectors.seatunnel.milvus.sink.common;

import com.google.gson.annotations.SerializedName;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class StageBucket {
    @SerializedName("minio_url")
    private String minioUrl;
    @SerializedName("access_key")
    private String accessKey;
    @SerializedName("secret_key")
    private String secretKey;
    @SerializedName("bucket_name")
    private String bucketName;
    @Builder.Default
    private String prefix = "";

    //config for import
    @SerializedName("instance_id")
    private String instanceId;
    @SerializedName("api_key")
    private String apiKey;
    @SerializedName("auto_import")
    @Builder.Default
    private Boolean autoImport = false;
}
