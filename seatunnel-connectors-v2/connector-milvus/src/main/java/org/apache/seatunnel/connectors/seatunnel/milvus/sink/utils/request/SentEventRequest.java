package org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.request;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class SentEventRequest {
    private String jobId;
    private String collectionName;
    private Integer eventType;
    private String eventData;
    private String message;
}
