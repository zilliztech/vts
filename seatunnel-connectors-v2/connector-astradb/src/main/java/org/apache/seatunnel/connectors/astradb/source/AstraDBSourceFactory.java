package org.apache.seatunnel.connectors.astradb.source;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.astradb.config.AstraDBSourceConfig;

import java.io.Serializable;

@Slf4j
@AutoService(Factory.class)
public class AstraDBSourceFactory implements TableSourceFactory {

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new AstraDBSource(context.getOptions());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(AstraDBSourceConfig.API_KEY)
                .optional()
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AstraDBSource.class;
    }

    @Override
    public String factoryIdentifier() {
        return AstraDBSourceConfig.CONNECTOR_IDENTITY;
    }
}
