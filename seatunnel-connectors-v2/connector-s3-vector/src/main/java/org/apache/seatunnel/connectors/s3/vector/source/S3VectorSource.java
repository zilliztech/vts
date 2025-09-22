package org.apache.seatunnel.connectors.s3.vector.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.s3.vector.config.S3VectorConfig;
import org.apache.seatunnel.connectors.s3.vector.config.S3VectorParameters;
import org.apache.seatunnel.connectors.s3.vector.utils.ConnectorUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import java.util.Collections;
import java.util.List;

public class S3VectorSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    private final S3VectorParameters parameters;

    private final CatalogTable catalogTable;

    S3VectorSource(ReadonlyConfig readonlyConfig) {
        this.parameters = new S3VectorParameters(readonlyConfig);
        if (readonlyConfig.get(TableSchemaOptions.SCHEMA) == null) {
            this.catalogTable = ConnectorUtils.buildCatalogTable(parameters);
        } else {
            this.catalogTable = CatalogTableUtil.buildWithConfig(readonlyConfig);
        }
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) {
        return new S3VectorSourceReader(parameters, readerContext, catalogTable.getTableSchema());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public String getPluginName() {
        return S3VectorConfig.CONNECTOR_IDENTITY;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }
}
