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

package org.apache.seatunnel.connectors.seatunnel.milvus.source;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.source.config.MilvusSourceConfig;
import static org.apache.seatunnel.connectors.seatunnel.milvus.source.config.MilvusSourceConfig.BATCH_SIZE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public class MilvusSourceReader implements SourceReader<SeaTunnelRow, MilvusSourceSplit> {

    private final Deque<MilvusSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();
    private final ReadonlyConfig config;
    private final Context context;
    private final Map<TablePath, CatalogTable> sourceTables;

    private MilvusClientV2 client;

    private volatile boolean noMoreSplit;

    public MilvusSourceReader(
            Context readerContext,
            ReadonlyConfig config,
            Map<TablePath, CatalogTable> sourceTables) {
        this.context = readerContext;
        this.config = config;
        this.sourceTables = sourceTables;
    }

    @Override
    public void open() throws Exception {
        client =
                new MilvusClientV2(
                        ConnectConfig.builder()
                                .uri(config.get(MilvusSourceConfig.URL))
                                .token(config.get(MilvusSourceConfig.TOKEN))
                                .dbName(config.get(MilvusSourceConfig.DATABASE))
                                .build());

    }

    @Override
    public void close() throws IOException {
        log.info("Close milvus source reader");
        client.close();
        log.info("Close milvus source reader success");
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            MilvusSourceSplit split = pendingSplits.poll();
            if (null != split) {
                try {
                    log.info("Begin to read data from split: " + split);
                    TableSchema tableSchema = sourceTables.get(split.getTablePath()).getTableSchema();
                    MilvusBufferReader milvusBufferReader = new MilvusBufferReader(split, output, client, tableSchema);
                    milvusBufferReader.pollData(config.get(BATCH_SIZE));
                } catch (Exception e) {
                    log.error("Read data from split: " + split + " failed", e);
                    throw new MilvusConnectorException(MilvusConnectionErrorCode.READ_DATA_FAIL, e);
                }
            } else {
                if (!noMoreSplit) {
                    log.info("Milvus source wait split!");
                }
            }
        }
        if (noMoreSplit
                && pendingSplits.isEmpty()
                && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded milvus source");
            context.signalNoMoreElement();
        }
    }

    @Override
    public List<MilvusSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<MilvusSourceSplit> splits) {
        log.info("Adding milvus splits to reader: " + splits);
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("receive no more splits message, this milvus reader will not add new split.");
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
