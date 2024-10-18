package org.apache.seatunnel.connectors.astradb.utils;

import com.google.common.collect.Lists;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.VectorType.VECTOR_FLOAT_TYPE;
import static org.apache.seatunnel.connectors.astradb.config.AstraDBSourceConfig.INDEX;

public class AstraDBUtils {
    private ReadonlyConfig config;
    Map<TablePath, CatalogTable> sourceTables;

    public AstraDBUtils(ReadonlyConfig config) {
        this.config = config;
        this.sourceTables = new HashMap<>();
    }

    public Map<TablePath, CatalogTable> getSourceTables() {

        String indexName = config.get(INDEX);
        TablePath tablePath = TablePath.of("default", indexName);

        List<Column> columns = new ArrayList<>();

        PhysicalColumn idColumn = PhysicalColumn.builder()
                .name("id")
                .dataType(STRING_TYPE)
                .build();
        PhysicalColumn vectorColumn = PhysicalColumn.builder()
                .name("vector")
                .dataType(VECTOR_FLOAT_TYPE)
                .scale(1536)
                .build();
        Map<String, Object> options = new HashMap<>();
        options.put("isDynamicField", true);
        PhysicalColumn dynamicColumn = PhysicalColumn.builder()
                .name("meta")
                .dataType(STRING_TYPE)
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
        CatalogTable catalogTable = CatalogTable.of(TableIdentifier.of("astradb", tablePath),
                tableSchema, new HashMap<>(), new ArrayList<>(), "");
        sourceTables.put(tablePath, catalogTable);
        return sourceTables;
    }
}

