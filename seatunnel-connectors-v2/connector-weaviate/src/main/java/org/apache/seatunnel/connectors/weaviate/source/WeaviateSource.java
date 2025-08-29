package org.apache.seatunnel.connectors.weaviate.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.weaviate.config.WeaviateConfig;
import org.apache.seatunnel.connectors.weaviate.config.WeaviateParameters;
import org.apache.seatunnel.connectors.weaviate.utils.ConnectorUtils;

import java.util.Collections;
import java.util.List;

public class WeaviateSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    private final WeaviateParameters parameters;

    private final CatalogTable catalogTable;

    WeaviateSource(ReadonlyConfig readonlyConfig) {
        this.parameters = new WeaviateParameters(readonlyConfig);
        if (readonlyConfig.get(TableSchemaOptions.SCHEMA) == null) {
            this.catalogTable = ConnectorUtils.buildCatalogTable(parameters);
        } else {
            this.catalogTable = CatalogTableUtil.buildWithConfig(readonlyConfig);
        }
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) {
        return new WeaviateSourceReader(parameters, readerContext, catalogTable.getTableSchema());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public String getPluginName() {
        return WeaviateConfig.CONNECTOR_IDENTITY;
    }
}
