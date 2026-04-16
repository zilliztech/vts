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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink;

import com.google.auto.service.AutoService;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog.MilvusFieldSchema;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig;

import java.time.ZoneId;
import java.util.List;

@AutoService(Factory.class)
public class MilvusSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "Milvus";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(MilvusSinkConfig.URL, MilvusSinkConfig.TOKEN)
                .optional(
                        MilvusSinkConfig.ENABLE_DYNAMIC_FIELD,
                        MilvusSinkConfig.SCHEMA_SAVE_MODE,
                        MilvusSinkConfig.DATA_SAVE_MODE,
                        MilvusSinkConfig.FIELD_SCHEMA)
                .build();
    }

    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        validateFieldTimezones(config);
        CatalogTable catalogTable = renameCatalogTable(config, context.getCatalogTable());
        return () -> new MilvusSink(config, catalogTable);
    }

    private void validateFieldTimezones(ReadonlyConfig config) {
        List<Object> rawFieldSchema = config.get(MilvusSinkConfig.FIELD_SCHEMA);
        if (rawFieldSchema == null || rawFieldSchema.isEmpty()) {
            return;
        }
        Gson gson = new Gson();
        List<MilvusFieldSchema> fieldSchemaList = gson.fromJson(
                gson.toJson(rawFieldSchema),
                new TypeToken<List<MilvusFieldSchema>>() {}.getType());
        for (MilvusFieldSchema fs : fieldSchemaList) {
            if (fs.getTimezone() != null && !fs.getTimezone().isEmpty()) {
                try {
                    ZoneId.of(fs.getTimezone());
                } catch (Exception e) {
                    throw new IllegalArgumentException(
                            "Invalid timezone '" + fs.getTimezone()
                                    + "' for field '" + fs.getEffectiveFieldName()
                                    + "'. Use IANA zone ID (e.g. 'Asia/Shanghai') "
                                    + "or UTC offset (e.g. '+08:00').", e);
                }
            }
        }
    }

    private CatalogTable renameCatalogTable(
            ReadonlyConfig config, CatalogTable sourceCatalogTable) {
        TableIdentifier sourceTableId = sourceCatalogTable.getTableId();
        String databaseName;
        if (StringUtils.isNotEmpty(config.get(MilvusSinkConfig.DATABASE))) {
            databaseName = config.get(MilvusSinkConfig.DATABASE);
        } else {
            databaseName = sourceTableId.getDatabaseName();
        }
        String tableName = sourceTableId.getTableName();
        if(config.get(MilvusSinkConfig.COLLECTION_RENAME).containsKey(sourceTableId.getTableName())){
            tableName = config.get(MilvusSinkConfig.COLLECTION_RENAME).get(sourceTableId.getTableName());
        };
        tableName = tableName.replace("-", "_");
        TableIdentifier newTableId =
                TableIdentifier.of(
                        sourceTableId.getCatalogName(),
                        databaseName,
                        sourceTableId.getSchemaName(),
                        tableName);

        return CatalogTable.of(newTableId, sourceCatalogTable);
    }
}
