/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.qdrant.source;

import io.qdrant.client.QdrantClient;
import static io.qdrant.client.WithPayloadSelectorFactory.enable;
import io.qdrant.client.WithVectorsSelectorFactory;
import io.qdrant.client.grpc.Points;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.qdrant.config.QdrantParameters;
import org.apache.seatunnel.connectors.seatunnel.qdrant.utils.ConverterUtils;

import java.util.List;
import java.util.Objects;


public class QdrantSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final QdrantParameters qdrantParameters;
    private final SingleSplitReaderContext context;
    private final TableSchema tableSchema;
    private final TablePath tablePath;
    private QdrantClient qdrantClient;

    public QdrantSourceReader(
            QdrantParameters qdrantParameters,
            SingleSplitReaderContext context,
            CatalogTable catalogTable) {
        this.qdrantParameters = qdrantParameters;
        this.context = context;
        this.tableSchema = catalogTable.getTableSchema();
        this.tablePath = catalogTable.getTablePath();
    }

    @Override
    public void open() throws Exception {
        qdrantClient = qdrantParameters.buildQdrantClient();
        qdrantClient.healthCheckAsync().get();
    }

    @Override
    public void close() {
        if (Objects.nonNull(qdrantClient)) {
            qdrantClient.close();
        }
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        int SCROLL_SIZE = 64;
        Points.ScrollPoints request =
                Points.ScrollPoints.newBuilder()
                        .setCollectionName(qdrantParameters.getCollectionName())
                        .setLimit(SCROLL_SIZE)
                        .setWithPayload(enable(true))
                        .setWithVectors(WithVectorsSelectorFactory.enable(true))
                        .build();

        while (true) {
            Points.ScrollResponse response = qdrantClient.scrollAsync(request).get();
            List<Points.RetrievedPoint> points = response.getResultList();

            for (Points.RetrievedPoint point : points) {
                SeaTunnelRow seaTunnelRow = ConverterUtils.convertToSeaTunnelRowWithMeta(tableSchema, point);
                output.collect(seaTunnelRow);
            }

            Points.PointId offset = response.getNextPageOffset();

            if (!offset.hasNum() && !offset.hasUuid()) break;

            request = request.toBuilder().setOffset(offset).build();
        }

        context.signalNoMoreElement();
    }


}
