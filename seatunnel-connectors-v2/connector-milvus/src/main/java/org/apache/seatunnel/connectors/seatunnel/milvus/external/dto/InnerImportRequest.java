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
    // workload identity token (gcp access token / aws session token), request-scoped
    // credential for the server-side storage prevalidation when ak/sk are absent
    private String token;
    private String clusterId;
    private String apiKey;
    private String dbName;
    private String collectionName;
    private String partitionName;
    private Boolean innerCall;
}