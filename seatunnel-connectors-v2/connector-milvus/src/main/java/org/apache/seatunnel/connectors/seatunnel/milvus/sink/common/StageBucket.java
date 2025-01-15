package org.apache.seatunnel.connectors.seatunnel.milvus.sink.common;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
public class StageBucket {
    @SerializedName("cloud_id")
    @Builder.Default
    private String cloudId = "aws";
    @SerializedName("minio_url")
    private String minioUrl;
    @SerializedName("region_id")
    private String regionId;
    @SerializedName("access_key")
    private String accessKey;
    @SerializedName("secret_key")
    private String secretKey;
    @SerializedName("bucket_name")
    private String bucketName;
    @SerializedName("chunk_size")
    @Builder.Default
    // 512 MB in default
    private Integer chunkSize = 512;
    @Builder.Default
    private String prefix = "";

    //config for import
    @SerializedName("instance_id")
    private String instanceId;
    @SerializedName("api_key")
    private String apiKey;
    @Builder.Default
    @SerializedName("auto_import")
    private Boolean autoImport = true;
    @Builder.Default
    @SerializedName("inner_call")
    private Boolean innerCall = false;
}
