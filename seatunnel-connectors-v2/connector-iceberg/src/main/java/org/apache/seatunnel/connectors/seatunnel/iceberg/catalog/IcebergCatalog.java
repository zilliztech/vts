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

package org.apache.seatunnel.connectors.seatunnel.iceberg.catalog;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.InfoPreviewResult;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PreviewResult;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergCatalogLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.utils.SchemaUtils;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.utils.SchemaUtils.toIcebergTableIdentifier;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.utils.SchemaUtils.toTablePath;

@Slf4j
public class IcebergCatalog implements Catalog {
    private final String catalogName;
    private final ReadonlyConfig readonlyConfig;
    private final IcebergCatalogLoader icebergCatalogLoader;
    private org.apache.iceberg.catalog.Catalog catalog;

    public IcebergCatalog(String catalogName, ReadonlyConfig readonlyConfig) {
        this.readonlyConfig = readonlyConfig;
        this.catalogName = catalogName;
        this.icebergCatalogLoader = new IcebergCatalogLoader(new CommonConfig(readonlyConfig));
    }

    @Override
    public String name() {
        return this.catalogName;
    }

    @Override
    public void open() throws CatalogException {
        this.catalog = icebergCatalogLoader.loadCatalog();
    }

    @Override
    public void close() throws CatalogException {
        if (catalog != null && catalog instanceof Closeable) {
            try {
                ((Closeable) catalog).close();
            } catch (IOException e) {
                log.error("Error while closing IcebergCatalog.", e);
                throw new CatalogException(e);
            }
        }
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return "default";
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        if (catalog instanceof SupportsNamespaces) {
            boolean exists =
                    ((SupportsNamespaces) catalog).namespaceExists(Namespace.of(databaseName));
            log.info("Database {} existence status: {}", databaseName, exists);
            return exists;
        } else {
            throw new UnsupportedOperationException(
                    "catalog not implements SupportsNamespaces so can't check database exists");
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        if (catalog instanceof SupportsNamespaces) {
            List<String> databases =
                    ((SupportsNamespaces) catalog)
                            .listNamespaces().stream()
                                    .map(Namespace::toString)
                                    .collect(Collectors.toList());
            log.info("Fetched {} namespaces.", databases.size());
            return databases;
        } else {
            throw new UnsupportedOperationException(
                    "catalog not implements SupportsNamespaces so can't list databases");
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        List<String> tables =
                catalog.listTables(Namespace.of(databaseName)).stream()
                        .map(tableIdentifier -> toTablePath(tableIdentifier).getTableName())
                        .collect(Collectors.toList());
        log.info("Fetched {} tables.", tables.size());
        return tables;
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        return catalog.tableExists(toIcebergTableIdentifier(tablePath));
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        TableIdentifier icebergTableIdentifier = toIcebergTableIdentifier(tablePath);
        try {
            CatalogTable catalogTable =
                    toCatalogTable(catalog.loadTable(icebergTableIdentifier), tablePath);
            log.info("Fetched table details for: {}", tablePath);
            return catalogTable;
        } catch (NoSuchTableException e) {
            throw new TableNotExistException("Table not exist", tablePath, e);
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        log.info("Creating table at path: {}", tablePath);
        SchemaUtils.autoCreateTable(catalog, tablePath, table, readonlyConfig);
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (ignoreIfNotExists) {
            if (!tableExists(tablePath)) {
                log.info(
                        "Attempted to drop table at path: {}. The table does not exist, but proceeding as 'ignoreIfNotExists' is set to true.",
                        tablePath);
                return;
            }
        }
        catalog.dropTable(toIcebergTableIdentifier(tablePath), true);
        log.info("Dropped table at path: {}", tablePath);
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        // Do nothing
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        // Do nothing
    }

    @Override
    public boolean isExistsData(TablePath tablePath) {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException("table not exist", tablePath);
        }
        TableIdentifier icebergTableIdentifier = toIcebergTableIdentifier(tablePath);
        Snapshot snapshot = catalog.loadTable(icebergTableIdentifier).currentSnapshot();
        if (snapshot != null) {
            String total = snapshot.summary().getOrDefault("total-records", null);
            return total != null && !total.equals("0");
        }
        return false;
    }

    @Override
    public void executeSql(TablePath tablePath, String sql) {
        throw new UnsupportedOperationException("Does not support executing custom SQL");
    }

    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException("table not exist", tablePath);
        }
        TableIdentifier icebergTableIdentifier = toIcebergTableIdentifier(tablePath);
        catalog.loadTable(icebergTableIdentifier)
                .newDelete()
                .deleteFromRowFilter(org.apache.iceberg.expressions.Expressions.alwaysTrue())
                .commit();
        log.info("Truncated table at path: {}", tablePath);
    }

    public CatalogTable toCatalogTable(Table icebergTable, TablePath tablePath) {
        Schema schema = icebergTable.schema();
        List<Types.NestedField> columns = schema.columns();
        TableSchema.Builder builder = TableSchema.builder();

        columns.forEach(
                nestedField -> {
                    String name = nestedField.name();
                    SeaTunnelDataType<?> seaTunnelType =
                            SchemaUtils.toSeaTunnelType(name, nestedField.type());
                    Integer scale = null;
                    if(seaTunnelType.equals(ArrayType.FLOAT_ARRAY_TYPE)){
                        seaTunnelType = VectorType.VECTOR_FLOAT_TYPE;
                        scale = getScale(icebergTable, name);
                    }
                    PhysicalColumn physicalColumn =
                            PhysicalColumn.of(
                                    name,
                                    seaTunnelType,
                                    (Long) null,
                                    scale,
                                    nestedField.isOptional(),
                                    null,
                                    nestedField.doc());
                    builder.column(physicalColumn);
                });
        if(StringUtils.isNotEmpty(readonlyConfig.get(SourceConfig.PRIMARY_KEY))){
            String primaryKey = readonlyConfig.get(SourceConfig.PRIMARY_KEY);
            builder.primaryKey(
                    PrimaryKey.of(
                            tablePath.getTableName() + "_pk", Collections.singletonList(primaryKey)));
        }
        List<String> partitionKeys =
                icebergTable.spec().fields().stream()
                        .map(PartitionField::name)
                        .collect(Collectors.toList());

        return CatalogTable.of(
                org.apache.seatunnel.api.table.catalog.TableIdentifier.of(
                        catalogName, tablePath.getDatabaseName(), tablePath.getTableName()),
                builder.build(),
                icebergTable.properties(),
                partitionKeys,
                null,
                catalogName);
    }

    private Integer getScale(Table icebergTable, String name) {
        CloseableIterable<Record> result = IcebergGenerics.read(icebergTable)
                .build();
        if (result.iterator().hasNext()) {
            Record record = result.iterator().next();
            List<Object> vector = (List<Object>) record.getField(name);
            return vector.size();
        }
        return null;
    }

    @Override
    public PreviewResult previewAction(
            ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable) {
        if (actionType == ActionType.CREATE_TABLE) {
            checkArgument(catalogTable.isPresent(), "CatalogTable cannot be null");
            return new InfoPreviewResult("create table " + toIcebergTableIdentifier(tablePath));
        } else if (actionType == ActionType.DROP_TABLE) {
            return new InfoPreviewResult("drop table " + toIcebergTableIdentifier(tablePath));
        } else if (actionType == ActionType.TRUNCATE_TABLE) {
            return new InfoPreviewResult("truncate table " + toIcebergTableIdentifier(tablePath));
        } else if (actionType == ActionType.CREATE_DATABASE) {
            return new InfoPreviewResult("do nothing");
        } else if (actionType == ActionType.DROP_DATABASE) {
            return new InfoPreviewResult("do nothing");
        } else {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        }
    }
}
