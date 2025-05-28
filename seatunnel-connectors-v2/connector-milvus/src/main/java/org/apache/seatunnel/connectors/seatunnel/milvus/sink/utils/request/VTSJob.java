package org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class VTSJob {
    private List<String> skippedData;
    private String dataPath;
}
