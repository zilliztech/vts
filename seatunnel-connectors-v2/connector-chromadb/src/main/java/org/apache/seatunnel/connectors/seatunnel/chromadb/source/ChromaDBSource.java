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

package org.apache.seatunnel.connectors.seatunnel.chromadb.source;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.chromadb.client.ChromaDBClient;
import org.apache.seatunnel.connectors.seatunnel.chromadb.config.ChromaDBSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.chromadb.exception.ChromaDBConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.chromadb.exception.ChromaDBConnectorException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE;
import static org.apache.seatunnel.api.table.type.VectorType.VECTOR_FLOAT_TYPE;

@Slf4j
public class ChromaDBSource
        implements SeaTunnelSource<SeaTunnelRow, ChromaDBSourceSplit, ChromaDBSourceState>,
                SupportParallelism {

    private final ReadonlyConfig config;
    private final LinkedHashMap<String, CatalogTable> collectionTables;

    public ChromaDBSource(ReadonlyConfig config) {
        this.config = config;
        this.collectionTables = new LinkedHashMap<>();

        try (ChromaDBClient client = createClient(config)) {
            List<ChromaDBClient.CollectionInfo> collections = resolveCollections(client, config);

            if (collections.isEmpty()) {
                throw new ChromaDBConnectorException(
                        ChromaDBConnectorErrorCode.COLLECTION_NOT_FOUND,
                        "No collections found to migrate");
            }

            for (ChromaDBClient.CollectionInfo info : collections) {
                CatalogTable table = buildCatalogTable(info);
                collectionTables.put(info.getId(), table);
                log.info(
                        "Resolved collection '{}' (id={}), {} columns",
                        info.getName(),
                        info.getId(),
                        table.getTableSchema().getColumns().size());
            }

            log.info("Total collections to migrate: {}", collectionTables.size());
        } catch (ChromaDBConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new ChromaDBConnectorException(
                    ChromaDBConnectorErrorCode.CONNECT_FAILED,
                    "Failed to initialize ChromaDB source",
                    e);
        }
    }

    @Override
    public String getPluginName() {
        return ChromaDBSourceConfig.CONNECTOR_IDENTITY;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return new ArrayList<>(collectionTables.values());
    }

    @Override
    public SourceReader<SeaTunnelRow, ChromaDBSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new ChromaDBSourceReader(config, readerContext, collectionTables);
    }

    @Override
    public SourceSplitEnumerator<ChromaDBSourceSplit, ChromaDBSourceState> createEnumerator(
            SourceSplitEnumerator.Context<ChromaDBSourceSplit> context) {
        return new ChromaDBSplitEnumerator(context, collectionTables, null);
    }

    @Override
    public SourceSplitEnumerator<ChromaDBSourceSplit, ChromaDBSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<ChromaDBSourceSplit> context,
            ChromaDBSourceState state) {
        return new ChromaDBSplitEnumerator(context, collectionTables, state);
    }

    static ChromaDBClient createClient(ReadonlyConfig config) {
        return new ChromaDBClient(config);
    }

    private List<ChromaDBClient.CollectionInfo> resolveCollections(
            ChromaDBClient client, ReadonlyConfig config) {
        List<String> configuredNames = config.get(ChromaDBSourceConfig.COLLECTIONS);

        if (configuredNames == null || configuredNames.isEmpty()) {
            throw new ChromaDBConnectorException(
                    ChromaDBConnectorErrorCode.COLLECTION_NOT_FOUND,
                    "collections is required and must not be empty");
        }

        LinkedHashSet<String> uniqueNames = new LinkedHashSet<>(configuredNames);
        if (uniqueNames.size() < configuredNames.size()) {
            log.warn(
                    "Duplicate collection names detected in config, duplicates will be ignored");
        }
        List<ChromaDBClient.CollectionInfo> result = new ArrayList<>();
        for (String name : uniqueNames) {
            ChromaDBClient.CollectionInfo info = client.getCollection(name);
            if (info == null) {
                throw new ChromaDBConnectorException(
                        ChromaDBConnectorErrorCode.COLLECTION_NOT_FOUND,
                        "Received null response when fetching collection '"
                                + name
                                + "'. This may indicate a proxy or load balancer issue.");
            }
            result.add(info);
        }
        return result;
    }

    private CatalogTable buildCatalogTable(ChromaDBClient.CollectionInfo collectionInfo) {
        String collectionName = collectionInfo.getName();
        List<Column> columns = new ArrayList<>();

        columns.add(
                PhysicalColumn.builder()
                        .name(ChromaDBSourceConfig.FIELD_ID)
                        .dataType(STRING_TYPE)
                        .build());

        Integer dimension = collectionInfo.getDimension();
        if (dimension == null) {
            throw new ChromaDBConnectorException(
                    ChromaDBConnectorErrorCode.COLLECTION_NOT_FOUND,
                    "Collection '"
                            + collectionName
                            + "' has unknown dimension (empty collection, no records inserted yet). "
                            + "Cannot migrate a collection without vector data.");
        }

        columns.add(
                PhysicalColumn.builder()
                        .name(ChromaDBSourceConfig.FIELD_EMBEDDING)
                        .dataType(VECTOR_FLOAT_TYPE)
                        .scale(dimension)
                        .build());

        columns.add(
                PhysicalColumn.builder()
                        .name(ChromaDBSourceConfig.FIELD_DOCUMENT)
                        .dataType(STRING_TYPE)
                        .nullable(true)
                        .build());

        columns.add(
                PhysicalColumn.builder()
                        .name(ChromaDBSourceConfig.FIELD_URI)
                        .dataType(STRING_TYPE)
                        .nullable(true)
                        .build());

        Map<String, Object> metadataOptions = new HashMap<>();
        metadataOptions.put(CommonOptions.METADATA.getName(), true);
        columns.add(
                PhysicalColumn.builder()
                        .name(CommonOptions.METADATA.getName())
                        .dataType(STRING_TYPE)
                        .options(metadataOptions)
                        .build());

        TablePath tablePath =
                TablePath.of(config.get(ChromaDBSourceConfig.DATABASE), collectionName);
        TableSchema tableSchema =
                TableSchema.builder()
                        .primaryKey(
                                PrimaryKey.of(
                                        ChromaDBSourceConfig.FIELD_ID,
                                        Lists.newArrayList(ChromaDBSourceConfig.FIELD_ID)))
                        .columns(columns)
                        .build();

        return CatalogTable.of(
                TableIdentifier.of(ChromaDBSourceConfig.CONNECTOR_IDENTITY, tablePath),
                tableSchema,
                new HashMap<>(),
                new ArrayList<>(),
                "");
    }
}
