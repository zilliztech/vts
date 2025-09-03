package org.apache.seatunnel.connectors.s3.vector.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.s3.vector.config.S3VectorParameters;
import org.apache.seatunnel.connectors.s3.vector.exception.S3VectorConnectorErrorCode;
import org.apache.seatunnel.connectors.s3.vector.exception.S3VectorConnectorException;
import org.apache.seatunnel.connectors.s3.vector.utils.ConverterUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import software.amazon.awssdk.services.s3vectors.S3VectorsClient;
import software.amazon.awssdk.services.s3vectors.model.ListVectorsRequest;
import software.amazon.awssdk.services.s3vectors.model.ListVectorsResponse;

public class S3VectorSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final S3VectorParameters parameters;
    private final SingleSplitReaderContext context;
    private final TableSchema tableSchema;

    private S3VectorsClient client;

    public S3VectorSourceReader(S3VectorParameters parameters, SingleSplitReaderContext context, TableSchema tableSchema) {
        this.parameters = parameters;
        this.context = context;
        this.tableSchema = tableSchema;
    }

    @Override
    public void open() {
        client = parameters.buildS3VectorClient();
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) {
        int BATCH_SIZE = 100;
        String nextToken = null;
        while (true) {
            ListVectorsRequest request = ListVectorsRequest.builder()
                    .vectorBucketName(parameters.getVectorBucketName())
                    .indexName(parameters.getIndexName())
                    .maxResults(BATCH_SIZE)
                    .nextToken(nextToken)
                    .returnMetadata(true)
                    .returnData(true)
                    .build();

            ListVectorsResponse response = client.listVectors(request);
            if (!response.sdkHttpResponse().isSuccessful()) {
                String msg = "Failed to list vectors from S3 Vector Service. " +
                        "HTTP Status Code: " + response.sdkHttpResponse().statusCode() +
                        ", HTTP Status Text: " + response.sdkHttpResponse().statusText();

                throw new S3VectorConnectorException(S3VectorConnectorErrorCode.RESPONSE_ERROR, msg);
            }

            if (response.vectors().isEmpty()) {
                break;
            }

            response.vectors().forEach(vector -> {
                SeaTunnelRow row = ConverterUtils.convertToSeaTunnelRow(tableSchema, vector);
                output.collect(row);
            });

            if (response.nextToken() == null) {
                break;
            }
            nextToken = response.nextToken();

            if (response.vectors().size() < BATCH_SIZE) {
                break;
            }
        }

        context.signalNoMoreElement();
    }
}
