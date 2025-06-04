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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog;

import static com.google.common.base.Preconditions.checkNotNull;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.collection.response.ListCollectionsResp;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DropDatabaseReq;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.InfoPreviewResult;
import org.apache.seatunnel.api.table.catalog.PreviewResult;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusConnectorUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class MilvusCatalog implements Catalog {

    private final String catalogName;
    private final ReadonlyConfig config;

    private MilvusClientV2 client;

    private CatalogUtils catalogUtils;

    public MilvusCatalog(String catalogName, ReadonlyConfig config) {
        this.catalogName = catalogName;
        this.config = config;
    }

    @Override
    public void open() throws CatalogException {
        try {
            this.client = new MilvusClientV2(MilvusConnectorUtils.getConnectConfig(config));
            catalogUtils = new CatalogUtils(client, config);
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed to open catalog %s", catalogName), e);
        }
    }

    @Override
    public void close() throws CatalogException {
        this.client.close();
    }

    @Override
    public String name() {
        return catalogName;
    }

    /**
     * Get the name of the default database for this catalog. The default database will be the
     * current database for the catalog when user's session doesn't specify a current database. The
     * value probably comes from configuration, will not change for the life time of the catalog
     * instance.
     *
     * @return the name of the current database
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public String getDefaultDatabase() throws CatalogException {
        return "default";
    }

    /**
     * Check if a database exists in this catalog.
     *
     * @param databaseName Name of the database
     * @return true if the given database exists in the catalog false otherwise
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        // no need to check db existence, db will be check in ConnectConfig
        return true;
    }

    /**
     * Get the names of all databases in this catalog.
     *
     * @return a list of the names of all databases
     * @throws CatalogException in case of any runtime exception
     */
    @Override
    public List<String> listDatabases() throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public PreviewResult previewAction(
            ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable) {
        if (actionType == ActionType.CREATE_TABLE) {
            return new InfoPreviewResult("create collection " + tablePath.getTableName());
        } else if (actionType == ActionType.DROP_TABLE) {
            return new InfoPreviewResult("drop collection " + tablePath.getTableName());
        } else if (actionType == ActionType.CREATE_DATABASE) {
            return new InfoPreviewResult("create database " + tablePath.getDatabaseName());
        } else if (actionType == ActionType.DROP_DATABASE) {
            return new InfoPreviewResult("drop database " + tablePath.getDatabaseName());
        } else {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        ListCollectionsResp listCollectionsResp = client.listCollections();

        return listCollectionsResp.getCollectionNames();
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {

        return this.client.hasCollection(HasCollectionReq.builder()
                        .collectionName(tablePath.getTableName())
                        .build());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        return null;
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable catalogTable, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        log.info("create table: {}", tablePath.getTableName());
        checkNotNull(tablePath, "Table path cannot be null");
        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                log.info("table: {} already exists, skip create table", tablePath.getTableName());
                return;
            }
            throw new TableAlreadyExistException(catalogName, tablePath);
        }

        checkNotNull(catalogTable, "catalogTable must not be null");
        TableSchema tableSchema = catalogTable.getTableSchema();
        checkNotNull(tableSchema, "tableSchema must not be null");
        catalogUtils.createTableInternal(tablePath, catalogTable);
        if(config.get(MilvusSinkConfig.CREATE_INDEX).booleanValue()) {
            log.info("create index is enabled, create index for table: {}", tablePath.getTableName());
            catalogUtils.createIndex(tablePath, tableSchema);
            log.info("create index for table: {} success", tablePath.getTableName());
        }else{
            log.info("create index is disabled, skip create index");
        }
        log.info("create table: {} success", tablePath.getTableName());
    }



    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(catalogName, tablePath);
            }
            return;
        }
        this.client.dropCollection(
                DropCollectionReq.builder().collectionName(tablePath.getTableName()).build());
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        if (databaseExists(tablePath.getDatabaseName())) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(catalogName, tablePath.getDatabaseName());
            }
            return;
        }
        this.client.createDatabase(CreateDatabaseReq.builder().databaseName(tablePath.getDatabaseName()).build());

    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
            }
            return;
        }
        this.client.dropDatabase(
                DropDatabaseReq.builder()
                        .databaseName(tablePath.getDatabaseName())
                        .build());
    }
}
