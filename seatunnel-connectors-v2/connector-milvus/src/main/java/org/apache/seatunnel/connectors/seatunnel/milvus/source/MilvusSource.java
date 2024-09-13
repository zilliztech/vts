package org.apache.seatunnel.connectors.seatunnel.milvus.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.utils.MilvusConvertUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MilvusSource
        implements SeaTunnelSource<SeaTunnelRow, MilvusSourceSplit, MilvusSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private final ReadonlyConfig config;
    private final Map<TablePath, CatalogTable> sourceTables;

    public MilvusSource(ReadonlyConfig sourceConfing) {
        this.config = sourceConfing;
        MilvusConvertUtils milvusConvertUtils = new MilvusConvertUtils(sourceConfing);
        this.sourceTables = milvusConvertUtils.getSourceTables();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    public List<CatalogTable> getProducedCatalogTables() {
        return new ArrayList<>(sourceTables.values());
    }

    @Override
    public SourceReader<SeaTunnelRow, MilvusSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new MilvusSourceReader(readerContext, config, sourceTables);
    }

    @Override
    public SourceSplitEnumerator<MilvusSourceSplit, MilvusSourceState> createEnumerator(
            SourceSplitEnumerator.Context<MilvusSourceSplit> context) throws Exception {
        return new MilvusSourceSplitEnumertor(context, config, sourceTables, null);
    }

    @Override
    public SourceSplitEnumerator<MilvusSourceSplit, MilvusSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<MilvusSourceSplit> context,
            MilvusSourceState checkpointState)
            throws Exception {
        return new MilvusSourceSplitEnumertor(context, config, sourceTables, checkpointState);
    }

    @Override
    public String getPluginName() {
        return "Milvus";
    }
}
