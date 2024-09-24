package org.apache.seatunnel.connectors.pinecone.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.seatunnel.api.table.catalog.TablePath;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class PineconeSourceState implements Serializable {
    private List<TablePath> pendingTables;
    private Map<Integer, List<PineconeSourceSplit>> pendingSplits;
}
