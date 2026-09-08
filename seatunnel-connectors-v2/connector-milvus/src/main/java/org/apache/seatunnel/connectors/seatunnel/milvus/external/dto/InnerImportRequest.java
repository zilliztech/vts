package org.apache.seatunnel.connectors.seatunnel.milvus.external.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
public class InnerImportRequest {
    private String objectUrl;
    // storage credentials; excluded from toString because the import request is logged
    @ToString.Exclude
    private String accessKey;
    @ToString.Exclude
    private String secretKey;
    // workload identity token, request-scoped credential minted at import time: a gcp
    // bearer token on its own, or the session token of the ak/sk/tokens triple on aws
    @ToString.Exclude
    private String token;
    private String clusterId;
    // the caller's api key is also a credential
    @ToString.Exclude
    private String apiKey;
    private String dbName;
    private String collectionName;
    private String partitionName;
    private Boolean innerCall;
}
