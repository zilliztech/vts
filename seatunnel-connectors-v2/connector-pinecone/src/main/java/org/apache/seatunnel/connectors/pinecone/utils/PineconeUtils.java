package org.apache.seatunnel.connectors.pinecone.utils;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.pinecone.clients.Index;
import io.pinecone.clients.Pinecone;
import io.pinecone.proto.Vector;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.*;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.openapitools.control.client.model.IndexModel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.BasicType.JSON_TYPE;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.VectorType.VECTOR_FLOAT_TYPE;
import static org.apache.seatunnel.connectors.pinecone.config.PineconeSourceConfig.API_KEY;
import static org.apache.seatunnel.connectors.pinecone.config.PineconeSourceConfig.INDEX;

public class PineconeUtils {
    private ReadonlyConfig config;
    Map<TablePath, CatalogTable> sourceTables;

    public PineconeUtils(ReadonlyConfig config) {
        this.config = config;
        this.sourceTables = new HashMap<>();
    }

    public Map<TablePath, CatalogTable> getSourceTables() {
        Pinecone pinecone = new Pinecone.Builder(config.get(API_KEY)).build();
        String indexName = config.get(INDEX);
        IndexModel indexMetadata = pinecone.describeIndex(indexName);
        TablePath tablePath = TablePath.of("default", indexName);

        List<Column> columns = new ArrayList<>();

        PhysicalColumn idColumn = PhysicalColumn.builder()
                .name("id")
                .dataType(STRING_TYPE)
                .build();
        PhysicalColumn vectorColumn = PhysicalColumn.builder()
                .name("vector")
                .dataType(VECTOR_FLOAT_TYPE)
                .scale(indexMetadata.getDimension())
                .build();
        Map<String, Object> options = new HashMap<>();
        options.put("isDynamicField", true);
        PhysicalColumn dynamicColumn = PhysicalColumn.builder()
                .name("meta")
                .dataType(JSON_TYPE)
                .options(options)
                .build();
        columns.add(idColumn);
        columns.add(vectorColumn);
        columns.add(dynamicColumn);

        TableSchema tableSchema = TableSchema.builder()
                .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                .columns(columns)
                .build();
        Map<TablePath, CatalogTable> sourceTables = new HashMap<>();
        CatalogTable catalogTable = CatalogTable.of(TableIdentifier.of("pinecone", tablePath),
                tableSchema, new HashMap<>(), new ArrayList<>(), "");
        sourceTables.put(tablePath, catalogTable);
        return sourceTables;
    }
}

