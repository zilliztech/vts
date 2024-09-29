package org.apache.seatunnel.connectors.astradb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.astradb.config.AstraDBSourceConfig;
import org.apache.seatunnel.connectors.astradb.utils.AstraDBUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AstraDBSource implements SeaTunnelSource<SeaTunnelRow, AstraDBSourceSplit, AstraDBSourceState>,
        SupportParallelism,
        SupportColumnProjection {
    private final ReadonlyConfig config;
    private final Map<TablePath, CatalogTable> sourceTables;

    public AstraDBSource(ReadonlyConfig config) {
        this.config = config;
        AstraDBUtils astraDBUtils = new AstraDBUtils(config);
        this.sourceTables = astraDBUtils.getSourceTables();
    }

    /**
     * Get the boundedness of this source.
     *
     * @return the boundedness of this source.
     */
    @Override
    public Boundedness getBoundedness() {
        return null;
    }

    /**
     * Create source reader, used to produce data.
     *
     * @param readerContext reader context.
     * @return source reader.
     * @throws Exception when create reader failed.
     */
    @Override
    public SourceReader<SeaTunnelRow, AstraDBSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new AstraDBSourceReader(readerContext, config, sourceTables);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return new ArrayList<>(sourceTables.values());
    }

    /**
     * Create source split enumerator, used to generate splits. This method will be called only once
     * when start a source.
     *
     * @param enumeratorContext enumerator context.
     * @return source split enumerator.
     * @throws Exception when create enumerator failed.
     */
    @Override
    public SourceSplitEnumerator<AstraDBSourceSplit, AstraDBSourceState> createEnumerator(SourceSplitEnumerator.Context<AstraDBSourceSplit> enumeratorContext) throws Exception {
        return new AstraDBSourceSplitEnumertor(enumeratorContext, config, sourceTables, null);
    }

    /**
     * Create source split enumerator, used to generate splits. This method will be called when
     * restore from checkpoint.
     *
     * @param enumeratorContext enumerator context.
     * @param checkpointState   checkpoint state.
     * @return source split enumerator.
     * @throws Exception when create enumerator failed.
     */
    @Override
    public SourceSplitEnumerator<AstraDBSourceSplit, AstraDBSourceState> restoreEnumerator(SourceSplitEnumerator.Context<AstraDBSourceSplit> enumeratorContext, AstraDBSourceState checkpointState) throws Exception {
        return new AstraDBSourceSplitEnumertor(enumeratorContext, config, sourceTables, checkpointState);
    }

    /**
     * Returns a unique identifier among same factory interfaces.
     *
     * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code
     * kafka}). If multiple factories exist for different versions, a version should be appended
     * using "-" (e.g. {@code elasticsearch-7}).
     */
    @Override
    public String getPluginName() {
        return AstraDBSourceConfig.CONNECTOR_IDENTITY;
    }
}
