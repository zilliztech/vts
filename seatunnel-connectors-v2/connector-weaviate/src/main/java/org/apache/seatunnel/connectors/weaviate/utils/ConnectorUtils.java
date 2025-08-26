package org.apache.seatunnel.connectors.weaviate.utils;

import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.data.model.WeaviateObject;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.weaviate.config.WeaviateParameters;
import org.apache.seatunnel.connectors.weaviate.exception.WeaviateConnectorErrorCode;
import org.apache.seatunnel.connectors.weaviate.exception.WeaviateConnectoreException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ConnectorUtils {
    public static CatalogTable buildCatalogTable(WeaviateParameters parameters) {
        WeaviateClient client = parameters.buildWeaviateClient();
        Result<List<WeaviateObject>> response = client.data()
                .objectsGetter()
                .withClassName(parameters.getClassName())
                .withVector()
                .withLimit(1)
                .run();

        if (response.hasErrors()) {
            throw new WeaviateConnectoreException(WeaviateConnectorErrorCode.FAILED_TO_CONNECT_WEAVIATE, response.getError().toString());
        }
        if (CollectionUtils.isEmpty(response.getResult())) {
            throw new WeaviateConnectoreException(WeaviateConnectorErrorCode.RESPONSE_ERROR, "The specified class is empty, please insert data first.");
        }

        WeaviateObject object = response.getResult().get(0);

        List<Column> columns = new ArrayList<>();

        Column id = PhysicalColumn.builder()
                .name("id")
                .dataType(BasicType.STRING_TYPE)
                .build();
        columns.add(id);

        if (object.getVector() != null) {
            long dim = object.getVector().length;
            Column vector = PhysicalColumn.builder()
                    .name(ConverterUtils.VECTOR_KEY)
                    .dataType(VectorType.VECTOR_FLOAT_TYPE)
                    .scale(Math.toIntExact(dim))
                    .build();
            columns.add(vector);
        }

        if (MapUtils.isNotEmpty(object.getProperties())) {
            object.getProperties().forEach((key, value) -> {
                if (value instanceof String) {
                    columns.add(PhysicalColumn.builder()
                            .name(key)
                            .dataType(BasicType.STRING_TYPE)
                            .build());
                } else if (value instanceof Integer) {
                    columns.add(PhysicalColumn.builder()
                            .name(key)
                            .dataType(BasicType.INT_TYPE)
                            .build());
                } else if (value instanceof Float) {
                    columns.add(PhysicalColumn.builder()
                            .name(key)
                            .dataType(BasicType.FLOAT_TYPE)
                            .build());
                } else if (value instanceof Double) {
                    columns.add(PhysicalColumn.builder()
                            .name(key)
                            .dataType(BasicType.DOUBLE_TYPE)
                            .build());
                } else if (value instanceof Boolean) {
                    columns.add(PhysicalColumn.builder()
                            .name(key)
                            .dataType(BasicType.BOOLEAN_TYPE)
                            .build());
                } else if (value instanceof Long) {
                    columns.add(PhysicalColumn.builder()
                            .name(key)
                            .dataType(BasicType.LONG_TYPE)
                            .build());
                } else {
                    throw new WeaviateConnectoreException(
                            WeaviateConnectorErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unsupported data type for field: " + key);
                }
            });
        }

        TableSchema tableSchema = TableSchema.builder()
                .primaryKey(PrimaryKey.of("id", new ArrayList<String>() {{ add("id"); }}))
                .columns(columns)
                .build();

        TablePath tablePath = TablePath.of("default", parameters.getClassName());
        return CatalogTable.of(TableIdentifier.of("", tablePath),
                tableSchema, new HashMap<>(), new ArrayList<>(), "");
    }
}
