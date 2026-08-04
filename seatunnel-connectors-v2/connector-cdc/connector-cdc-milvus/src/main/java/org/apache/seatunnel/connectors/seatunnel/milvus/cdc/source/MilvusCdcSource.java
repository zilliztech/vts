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

package org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.config.MilvusCdcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.client.MilvusCdcClientFactory;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.schema.MilvusCdcCollectionSchemaRegistry;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.schema.MilvusCdcSchemaDiscovery;

import java.util.List;

public class MilvusCdcSource
        implements SeaTunnelSource<SeaTunnelRow, MilvusCdcSplit, MilvusCdcSourceState>,
                SupportParallelism {

    private final ReadonlyConfig config;
    private final List<CatalogTable> catalogTables;
    private final MilvusCdcCollectionSchemaRegistry schemaRegistry;

    public MilvusCdcSource(ReadonlyConfig config) {
        this.config = config;
        List<MilvusCdcSourceTable> sourceTables =
                MilvusCdcSourceConfigParser.parseDatabaseCollections(config);
        this.schemaRegistry = new MilvusCdcSchemaDiscovery().discover(config, sourceTables);
        this.catalogTables = schemaRegistry.catalogTables();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.UNBOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return catalogTables;
    }

    @Override
    public SourceReader<SeaTunnelRow, MilvusCdcSplit> createReader(
            SourceReader.Context readerContext) {
        return new MilvusCdcSourceReader(
                readerContext, config, MilvusCdcClientFactory.fromConfig(config), schemaRegistry);
    }

    @Override
    public SourceSplitEnumerator<MilvusCdcSplit, MilvusCdcSourceState> createEnumerator(
            SourceSplitEnumerator.Context<MilvusCdcSplit> enumeratorContext) {
        return new MilvusCdcSplitEnumerator(enumeratorContext, config, null);
    }

    @Override
    public SourceSplitEnumerator<MilvusCdcSplit, MilvusCdcSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<MilvusCdcSplit> enumeratorContext,
            MilvusCdcSourceState checkpointState) {
        return new MilvusCdcSplitEnumerator(enumeratorContext, config, checkpointState);
    }

    @Override
    public String getPluginName() {
        return MilvusCdcSourceConfig.CONNECTOR_IDENTITY;
    }
}
