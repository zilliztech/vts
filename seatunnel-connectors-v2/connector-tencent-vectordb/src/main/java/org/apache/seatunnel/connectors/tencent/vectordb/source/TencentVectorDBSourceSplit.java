package org.apache.seatunnel.connectors.tencent.vectordb.source;

import lombok.Data;
import lombok.experimental.SuperBuilder;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.TablePath;

@Data
@SuperBuilder
public class TencentVectorDBSourceSplit implements SourceSplit {
    private TablePath tablePath;
    private String splitId;
    private String partitionName;
    /**
     * Get the split id of this source split.
     *
     * @return id of this source split.
     */
    @Override
    public String splitId() {
        return splitId;
    }
}
