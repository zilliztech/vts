package org.apache.seatunnel.connectors.astradb.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.seatunnel.api.table.catalog.TablePath;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class AstraDBSourceState implements Serializable {
    private List<TablePath> pendingTables;
    private Map<Integer, List<AstraDBSourceSplit>> pendingSplits;
}
