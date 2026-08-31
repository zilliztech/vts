package org.apache.seatunnel.connectors.seatunnel.milvus.external.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
public class InnerImportRequest {
    private String objectUrl;
    private String accessKey;
    private String secretKey;
    // workload identity token, request-scoped credential minted at import time: a gcp
    // bearer token on its own, or the session token of the ak/sk/tokens triple on aws
    private String token;
    private String clusterId;
    private String apiKey;
    private String dbName;
    private String collectionName;
    private String partitionName;
    private Boolean innerCall;
}