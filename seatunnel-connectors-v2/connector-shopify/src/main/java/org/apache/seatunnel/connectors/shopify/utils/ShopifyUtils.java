package org.apache.seatunnel.connectors.shopify.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.constants.CommonOptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShopifyUtils {
    private ReadonlyConfig config;

    public ShopifyUtils(ReadonlyConfig config) {
        this.config = config;
    }

    public CatalogTable getSourceTables() {
        TablePath tablePath = TablePath.of("default", "products");

        List<Column> columns = new ArrayList<>();
        PhysicalColumn idColumn = PhysicalColumn.builder()
                .name("id")
                .dataType(STRING_TYPE)
                .build();
        PhysicalColumn titleColumn = PhysicalColumn.builder()
                .name("title")
                .dataType(STRING_TYPE)
                .build();
        PhysicalColumn vectorColumn = PhysicalColumn.builder()
                .name("title_vector")
                .dataType(VectorType.VECTOR_FLOAT_TYPE)
                .scale(1536)
                .build();

        Map<String, Object> options = new HashMap<>();
        options.put(CommonOptions.METADATA.getName(), true);
        PhysicalColumn meteColumn = PhysicalColumn.builder()
                .name(CommonOptions.METADATA.getName())
                .dataType(STRING_TYPE)
                .options(options)
                .build();
        columns.add(idColumn);
        columns.add(titleColumn);
        columns.add(vectorColumn);
        columns.add(meteColumn);
        TableSchema tableSchema = TableSchema.builder()
                .primaryKey(PrimaryKey.of("id", Collections.singletonList("id")))
                .columns(columns)
                .build();

        return CatalogTable.of(TableIdentifier.of("Shopify", tablePath),
                tableSchema, new HashMap<>(), new ArrayList<>(), "");
    }
}

