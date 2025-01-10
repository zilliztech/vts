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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer;

import com.google.gson.JsonObject;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.UpsertReq;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.BATCH_SIZE;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusConnectorUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusSinkConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class MilvusBufferBatchWriter implements MilvusWriter {

    private final CatalogTable catalogTable;
    private final ReadonlyConfig config;
    private final String collectionName;
    private final String partitionName;
    private final Boolean hasPartitionKey;

    private final MilvusClientV2 milvusClient;
    private final MilvusSinkConverter milvusSinkConverter;
    private int batchSize;
    private volatile List<JsonObject> milvusDataCache;
    private final AtomicLong writeCache = new AtomicLong();
    private final AtomicLong writeCount = new AtomicLong();

    private final List<String> jsonFieldNames;
    private final String dynamicFieldName;

    public MilvusBufferBatchWriter (CatalogTable catalogTable, ReadonlyConfig config, MilvusClientV2 milvusClient, String partitionName)
            throws SeaTunnelException {
        this.catalogTable = catalogTable;
        this.config = config;
        this.batchSize = config.get(BATCH_SIZE);
        this.collectionName = catalogTable.getTablePath().getTableName();
        this.partitionName = partitionName;

        this.milvusDataCache = new ArrayList<>();
        this.milvusSinkConverter = new MilvusSinkConverter();

        this.dynamicFieldName = MilvusConnectorUtils.getDynamicField(catalogTable);
        this.jsonFieldNames = MilvusConnectorUtils.getJsonField(catalogTable);
        this.hasPartitionKey = MilvusConnectorUtils.hasPartitionKey(milvusClient, collectionName);
        this.milvusClient = milvusClient;
    }

    @Override
    public void write(SeaTunnelRow element) {
        // put data to cache by partition
        JsonObject data =
                milvusSinkConverter.buildMilvusData(
                        catalogTable, config, jsonFieldNames, dynamicFieldName, element);
        milvusDataCache.add(data);
        writeCache.incrementAndGet();
        writeCount.incrementAndGet();
        if(needCommit()){
            try {
                commit();
            } catch (Exception e) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                        "write data failed",
                        e);
            }
        }
    }

    @Override
    public boolean needCommit() {
        return this.writeCache.get() >= this.batchSize;
    }

    @Override
    public void commit() throws Exception {
        // Flush the batch writer
        // Get the number of records completed
        if (this.milvusDataCache.isEmpty()) {
            return;
        }

        // default to use upsertReq, but upsert only works when autoID is disabled
        upsertWrite(partitionName, milvusDataCache);

        writeCount.addAndGet(this.writeCache.get());
        this.writeCache.set(0L);
        this.milvusDataCache.clear();
    }

    @Override
    public void close() throws Exception {
        commit();
        this.milvusClient.close(10);
    }

    @Override
    public long getWriteCache() {
        return writeCache.get();
    }

    private void upsertWrite(String partitionName, List<JsonObject> data)
            throws InterruptedException {
        UpsertReq upsertReq = UpsertReq.builder()
                        .collectionName(this.collectionName)
                        .data(data)
                        .build();

        if (StringUtils.isNotEmpty(partitionName) && !partitionName.equals("_default") &&  !this.hasPartitionKey) {
            upsertReq.setPartitionName(partitionName);
        }

        try {
            milvusClient.upsert(upsertReq);
        } catch (Exception e) {
            if (e.getMessage().contains("rate limit exceeded")
                    || e.getMessage().contains("received message larger than max")) {
                if (data.size() > 2) {
                    log.warn("upsert data failed, retry in smaller chunks: {} ", data.size() / 2);
                    this.batchSize = this.batchSize / 2;
                    log.info("sleep 1 minute to avoid rate limit");
                    // sleep 1 minute to avoid rate limit
                    Thread.sleep(60000);
                    log.info("sleep 1 minute success");
                    // Split the data and retry in smaller chunks
                    List<JsonObject> firstHalf = data.subList(0, data.size() / 2);
                    List<JsonObject> secondHalf = data.subList(data.size() / 2, data.size());
                    upsertWrite(partitionName, firstHalf);
                    upsertWrite(partitionName, secondHalf);
                } else {
                    // If the data size is 10, throw the exception to avoid infinite recursion
                    throw new MilvusConnectorException(
                            MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                            "upsert data failed," + " size down to 10, break",
                            e);
                }
            } else {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.WRITE_DATA_FAIL,
                        "upsert data failed with unknown exception",
                        e);
            }
        }
    }
}
