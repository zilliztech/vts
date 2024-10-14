package org.apache.seatunnel.connectors.seatunnel.qdrant.utils;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Collections;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.*;
import org.apache.seatunnel.connectors.seatunnel.qdrant.config.QdrantConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.seatunnel.api.table.type.BasicType.*;
import static org.apache.seatunnel.api.table.type.VectorType.VECTOR_FLOAT_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.qdrant.config.QdrantConfig.*;

public class ConnectorUtils {

    public static CatalogTable buildCatalogTable(ReadonlyConfig config) {
        QdrantClient client = new QdrantClient(QdrantGrpcClient.
                newBuilder(config.get(QdrantConfig.HOST), config.get(QdrantConfig.PORT), config.get(QdrantConfig.USE_TLS))
                .withApiKey(config.get(QdrantConfig.API_KEY))
                .build());

        String collectionName = config.get(COLLECTION_NAME);

        Collections.CollectionInfo collectionInfo = null;
        try {
            collectionInfo = client.getCollectionInfoAsync(collectionName).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        TablePath tablePath = TablePath.of("default", config.get(COLLECTION_NAME));

        List<Column> columns = new ArrayList<>();

        PhysicalColumn idColumn = PhysicalColumn.builder()
                .name("id")
                .dataType(LONG_TYPE)
                .build();
        columns.add(idColumn);

        Collections.VectorsConfig vectorsConfig = collectionInfo.getConfig().getParams().getVectorsConfig();
        if(vectorsConfig.getParamsMap().getMapMap().isEmpty()) {
            // single vector
            long dimension = vectorsConfig.getParams().getSize();
            PhysicalColumn vectorColumn = PhysicalColumn.builder()
                    .name("vector")
                    .dataType(VECTOR_FLOAT_TYPE)
                    .scale(Math.toIntExact(dimension))
                    .build();
            columns.add(vectorColumn);
        } else if (!vectorsConfig.getParamsMap().getMapMap().isEmpty()) {
            // multiple vectors
            for (Map.Entry<String, Collections.VectorParams> entry : vectorsConfig.getParamsMap().getMapMap().entrySet()) {
                Collections.VectorParams params = entry.getValue();
                long dimension = params.getSize();
                PhysicalColumn vectorColumn = PhysicalColumn.builder()
                        .name(entry.getKey())
                        .dataType(VECTOR_FLOAT_TYPE)
                        .scale(Math.toIntExact(dimension))
                        .build();
                columns.add(vectorColumn);
            }
        }
        Map<String, Object> options = new HashMap<>();
        options.put("isDynamicField", true);
        // dynamic column
        PhysicalColumn dynamicColumn = PhysicalColumn.builder()
                .name("meta")
                .dataType(JSON_TYPE)
                .options(options)
                .build();

        columns.add(dynamicColumn);

        TableSchema tableSchema = TableSchema.builder()
                .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                .columns(columns)
                .build();
        Map<TablePath, CatalogTable> sourceTables = new HashMap<>();
        CatalogTable catalogTable = CatalogTable.of(TableIdentifier.of("qdrant", tablePath),
                tableSchema, new HashMap<>(), new ArrayList<>(), "");
        return catalogTable;
    }
}
