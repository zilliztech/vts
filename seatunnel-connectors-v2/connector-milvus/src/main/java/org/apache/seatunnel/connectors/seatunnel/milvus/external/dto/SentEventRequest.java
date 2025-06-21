package org.apache.seatunnel.connectors.seatunnel.milvus.external.dto;

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
    private Integer eventType;
    private String eventData;
    private String message;
}