package org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer;

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
    private String clusterId;
    private String apiKey;
    private String dbName;
    private String collectionName;
    private String partitionName;
    private Boolean innerCall;
}
