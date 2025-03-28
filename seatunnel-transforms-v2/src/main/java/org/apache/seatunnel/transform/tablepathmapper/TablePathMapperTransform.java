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

package org.apache.seatunnel.transform.tablepathmapper;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import java.util.Map;

/**
 * now TableMapper only support change table name, if you have other requirements, can extension it.
 * TablePathMapper{ table_mapper = { test-vector2 = test_vector2 } ... database_mapper,
 * schema_mapper .... }
 */
@Slf4j
public class TablePathMapperTransform extends AbstractCatalogSupportTransform {
    public static String PLUGIN_NAME = "TablePathMapper";
    private final TablePathMapperTransformConfig config;

    public TablePathMapperTransform(
            @NonNull TablePathMapperTransformConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.config = config;
        checkTableMapper(config.getTableMapper(), catalogTable);
        checkDatabaseMapper(config.getDatabaseMapper(), catalogTable);
        checkSchemaMapper(config.getSchemaMapper(), catalogTable);
    }

    private void checkTableMapper(Map<String, String> tableMapper, CatalogTable catalogTable) {
        if (MapUtils.isEmpty(tableMapper)) {
            return;
        }
        if (!tableMapper.containsKey(catalogTable.getTableId().getTableName())) {
            throw TransformCommonError.cannotFindInputTableNameError(
                    getPluginName(), tableMapper.keySet());
        }
    }

    private void checkDatabaseMapper(
            Map<String, String> databaseMapper, CatalogTable catalogTable) {
        if (MapUtils.isEmpty(databaseMapper)) {
            return;
        }
        if (!databaseMapper.containsKey(catalogTable.getTableId().getDatabaseName())) {
            throw TransformCommonError.cannotFindInputDatabaseNameError(
                    getPluginName(), databaseMapper.keySet());
        }
    }

    private void checkSchemaMapper(Map<String, String> schemaMapper, CatalogTable catalogTable) {
        if (MapUtils.isEmpty(schemaMapper)) {
            return;
        }
        if (!schemaMapper.containsKey(catalogTable.getTableId().getSchemaName())) {
            throw TransformCommonError.cannotFindInputSchemaNameError(
                    getPluginName(), schemaMapper.keySet());
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        return inputRow;
    }

    @Override
    protected TableSchema transformTableSchema() {
        return inputCatalogTable.getTableSchema();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        TableIdentifier tableId = inputCatalogTable.getTableId();

        Map<String, String> tableMapper = config.getTableMapper();
        String tableName =
                MapUtils.isEmpty(tableMapper)
                        ? tableId.getTableName()
                        : tableMapper.get(tableId.getTableName());

        Map<String, String> databaseMapper = config.getDatabaseMapper();
        String database =
                MapUtils.isEmpty(databaseMapper)
                        ? tableId.getDatabaseName()
                        : databaseMapper.get(tableId.getDatabaseName());

        Map<String, String> schemaMapper = config.getSchemaMapper();
        String schema =
                MapUtils.isEmpty(schemaMapper)
                        ? tableId.getSchemaName()
                        : schemaMapper.get(tableId.getSchemaName());
        return TableIdentifier.of(tableId.getCatalogName(), database, schema, tableName);
    }
}
