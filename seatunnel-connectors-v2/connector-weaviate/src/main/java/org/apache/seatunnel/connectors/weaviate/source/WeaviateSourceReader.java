package org.apache.seatunnel.connectors.weaviate.source;

import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.data.api.ObjectsGetter;
import io.weaviate.client.v1.data.model.WeaviateObject;
import io.weaviate.client.v1.misc.api.LiveChecker;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.weaviate.config.WeaviateParameters;
import org.apache.seatunnel.connectors.weaviate.exception.WeaviateConnectorErrorCode;
import org.apache.seatunnel.connectors.weaviate.exception.WeaviateConnectoreException;
import org.apache.seatunnel.connectors.weaviate.utils.ConverterUtils;

import java.util.List;

public class WeaviateSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final WeaviateParameters parameters;
    private final SingleSplitReaderContext context;
    private final TableSchema tableSchema;

    private WeaviateClient client;

    public WeaviateSourceReader(WeaviateParameters parameters, SingleSplitReaderContext context, TableSchema tableSchema) {
        this.parameters = parameters;
        this.context = context;
        this.tableSchema = tableSchema;
    }

    @Override
    public void open() {
        client = parameters.buildWeaviateClient();
        LiveChecker checker = client.misc().liveChecker();
        Result<Boolean> response = checker.run();
        if (response.hasErrors()) {
            throw new WeaviateConnectoreException(
                    WeaviateConnectorErrorCode.FAILED_TO_CONNECT_WEAVIATE, response.getError().toString());
        }
    }

    @Override
    public void close() {
    }


    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) {
        int BATCH_SIZE = 100;
        String lastId = null;
        while (true) {
            ObjectsGetter getter = client.data().objectsGetter()
                    .withAfter(lastId)
                    .withClassName(parameters.getClassName())
                    .withVector()
                    .withLimit(BATCH_SIZE);
            Result<List<WeaviateObject>> response = getter.run();
            if (response.hasErrors()) {
                throw new WeaviateConnectoreException(WeaviateConnectorErrorCode.RESPONSE_ERROR, response.getError().toString());
            }

            List<WeaviateObject> objects = response.getResult();
            if (objects.isEmpty()) {
                break; // No more objects to fetch
            }

            for (WeaviateObject object : objects) {
                SeaTunnelRow row = ConverterUtils.convertToSeaTunnelRow(tableSchema, object);
                output.collect(row);
            }
            lastId = objects.get(objects.size() - 1).getId();
            if (objects.size() < BATCH_SIZE) {
                break; // If the last batch is smaller than BATCH_SIZE, we are done
            }
        }

        context.signalNoMoreElement();
    }
}
