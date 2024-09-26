package org.apache.seatunnel.connectors.astradb.source;

import com.datastax.astra.client.Collection;
import com.datastax.astra.client.DataAPIClient;
import com.datastax.astra.client.Database;
import com.datastax.astra.client.model.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.astradb.exception.AstraDBConnectionErrorCode;
import org.apache.seatunnel.connectors.astradb.exception.AstraDBConnectorException;

import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.apache.seatunnel.connectors.astradb.config.AstraDBSourceConfig.*;

@Slf4j
public class AstraDBSourceReader implements SourceReader<SeaTunnelRow, AstraDBSourceSplit> {
    private final Deque<AstraDBSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();
    private final ReadonlyConfig config;
    private final Context context;
    private Map<TablePath, CatalogTable> sourceTables;
    private String pageState;
    private DataAPIClient client;

    private volatile boolean noMoreSplit;
    public AstraDBSourceReader(Context readerContext, ReadonlyConfig config, Map<TablePath, CatalogTable> sourceTables) {
        this.context = readerContext;
        this.config = config;
        this.sourceTables = sourceTables;
    }

    /**
     * Open the source reader.
     */
    @Override
    public void open() throws Exception {
        client = new DataAPIClient(config.get(API_KEY));
    }

    /**
     * Called to close the reader, in case it holds on to any resources, like threads or network
     * connections.
     */
    @Override
    public void close() throws IOException {
    }

    /**
     * Generate the next batch of records.
     *
     * @param output output collector.
     * @throws Exception if error occurs.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            AstraDBSourceSplit split = pendingSplits.poll();
            if (null != split) {
                try {
                    log.info("Begin to read data from split: " + split);
                    TablePath tablePath = split.getTablePath();
                    TableSchema tableSchema = sourceTables.get(tablePath).getTableSchema();
                    log.info("begin to read data from AstraDB, table schema: " + tableSchema);
                    if (null == tableSchema) {
                        throw new AstraDBConnectorException(
                                AstraDBConnectionErrorCode.SOURCE_TABLE_SCHEMA_IS_NULL);
                    }
                    Database db = client.getDatabase(config.get(API_ENDPOINT));

                    Collection<Document> collection =  db.getCollection(config.get(INDEX));

                    // Define find options including pageSize and pageState for pagination
                    // Building a filter
                    Filter filter = Filters.and();

                    // Find Options
                    FindOptions options = new FindOptions()
                            .limit(10) // stop after 10 items (max records)
                            .pageState(pageState) // used for pagination
                            .includeSimilarity(); // include similarity

                    do {
                        // Find a page of documents
                        Page<Document> pageResponse = collection.findPage(filter, options);

                        // Process the documents in the current page
                        pageResponse.getResults().forEach(doc -> {
                            // Assuming vector data is stored in a field named "vector"
                            List<Float> vector = doc.get("vector", List.class); // Cast to List<Float>
                            System.out.println("Vector: " + vector);

                            // Process the vector data...
                        });

                        // Get the next pageState for pagination
                        pageState = pageResponse.getPageState().get();

                    } while (pageState != null); // Continue until there are no more pages

                } catch (Exception e) {
                    log.error("Read data from split: " + split + " failed", e);
                    throw new AstraDBConnectorException(
                            AstraDBConnectionErrorCode.READ_DATA_FAIL, e);
                }
            } else {
                if (!noMoreSplit) {
                    log.info("AstraDB source wait split!");
                }
            }
        }
        if (noMoreSplit
                && pendingSplits.isEmpty()
                && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded pinecone source");
            context.signalNoMoreElement();
        }
        Thread.sleep(1000L);
    }

    /**
     * Get the current split checkpoint state by checkpointId.
     *
     * <p>If the source is bounded, checkpoint is not triggered.
     *
     * @param checkpointId checkpoint Id.
     * @return split checkpoint state.
     * @throws Exception if error occurs.
     */
    @Override
    public List<AstraDBSourceSplit> snapshotState(long checkpointId) throws Exception {
        return null;
    }

    /**
     * Add the split checkpoint state to reader.
     *
     * @param splits split checkpoint state.
     */
    @Override
    public void addSplits(List<AstraDBSourceSplit> splits) {
        log.info("Adding pinecone splits to reader: " + splits);
        pendingSplits.addAll(splits);
    }

    /**
     * This method is called when the reader is notified that it will not receive any further
     * splits.
     *
     * <p>It is triggered when the enumerator calls {@link
     * SourceSplitEnumerator.Context#signalNoMoreSplits(int)} with the reader's parallel subtask.
     */
    @Override
    public void handleNoMoreSplits() {
        log.info("receive no more splits message, this milvus reader will not add new split.");
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
