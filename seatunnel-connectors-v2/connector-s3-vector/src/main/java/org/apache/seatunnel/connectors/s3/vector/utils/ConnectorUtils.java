package org.apache.seatunnel.connectors.s3.vector.utils;

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
import org.apache.seatunnel.connectors.s3.vector.config.S3VectorParameters;
import org.apache.seatunnel.connectors.s3.vector.exception.S3VectorConnectorErrorCode;
import org.apache.seatunnel.connectors.s3.vector.exception.S3VectorConnectorException;
import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.services.s3vectors.S3VectorsClient;
import software.amazon.awssdk.services.s3vectors.model.ListVectorsRequest;
import software.amazon.awssdk.services.s3vectors.model.ListVectorsResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectorUtils {
    public static CatalogTable buildCatalogTable(S3VectorParameters parameters) {
        S3VectorsClient client = parameters.buildS3VectorClient();
        ListVectorsRequest request = ListVectorsRequest.builder()
                .indexName(parameters.getIndexName())
                .vectorBucketName(parameters.getVectorBucketName())
                .maxResults(1)
                .returnData(true)
                .returnMetadata(true)
                .build();
        ListVectorsResponse response = client.listVectors(request);

        if (!response.sdkHttpResponse().isSuccessful()) {
            String msg = "Failed to list vectors from S3 Vector Service. " +
                    "HTTP Status Code: " + response.sdkHttpResponse().statusCode() +
                    ", HTTP Status Text: " + response.sdkHttpResponse().statusText();

            throw new RuntimeException(msg);
        }

        List<Column> columns = new ArrayList<>();
        Column id = PhysicalColumn.builder()
                .name("key")
                .dataType(BasicType.STRING_TYPE)
                .build();
        columns.add(id);

        if (CollectionUtils.isEmpty(response.vectors())) {
            throw new S3VectorConnectorException(S3VectorConnectorErrorCode.RESPONSE_ERROR, "No vectors found in the specified index and bucket.");
        }

        int dim = response.vectors().get(0).data().float32().size();
        Column vector = PhysicalColumn.builder()
                .name(ConverterUtils.VECTOR_KEY)
                .dataType(VectorType.VECTOR_FLOAT_TYPE)
                .scale(dim)
                .build();
        columns.add(vector);

        Map<String, Document> metadata = response.vectors().get(0).metadata().asMap();
        if (MapUtils.isNotEmpty(metadata)) {
            for (Map.Entry<String, Document> entry : metadata.entrySet()) {
                String columnName = entry.getKey();
                Document value = entry.getValue();
                if (value.isBoolean()) {
                    columns.add(PhysicalColumn.builder()
                            .name(columnName)
                            .dataType(BasicType.BOOLEAN_TYPE)
                            .build());
                } else if (value.isString()) {
                    columns.add(PhysicalColumn.builder()
                            .name(columnName)
                            .dataType(BasicType.STRING_TYPE)
                            .build());
                } else if (value.isNumber()) {
                    columns.add(PhysicalColumn.builder()
                            .name(columnName)
                            .dataType(BasicType.DOUBLE_TYPE)
                            .build());
                } else {
                    throw new S3VectorConnectorException(S3VectorConnectorErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unsupported metadata type for column: " + columnName);
                }
            }

        }

        TableSchema tableSchema = TableSchema.builder()
                .primaryKey(PrimaryKey.of("key", new ArrayList<String>() {{ add("key"); }}))
                .columns(columns)
                .build();

        TablePath tablePath = TablePath.of("default", parameters.getIndexName());
        return CatalogTable.of(TableIdentifier.of("", tablePath),
                tableSchema, new HashMap<>(), new ArrayList<>(), "");
    }
}
