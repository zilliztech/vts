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

package org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.schema;

import org.apache.seatunnel.api.table.catalog.CatalogTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MilvusCdcCollectionSchemaRegistry implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, Map<String, MilvusCdcCollectionSchema>>
            schemasBySourceDatabaseAndCollection;
    private final List<CatalogTable> catalogTables;

    public MilvusCdcCollectionSchemaRegistry(List<MilvusCdcCollectionSchema> schemas) {
        Map<String, Map<String, MilvusCdcCollectionSchema>> schemasBySource = new HashMap<>();
        List<CatalogTable> tables = new ArrayList<>();
        for (MilvusCdcCollectionSchema schema : schemas) {
            String sourceDatabase = requireNonBlank(schema.getSourceDatabase(), "sourceDatabase");
            String sourceCollection =
                    requireNonBlank(schema.getSourceCollection(), "sourceCollection");
            MilvusCdcCollectionSchema previous =
                    schemasBySource
                            .computeIfAbsent(sourceDatabase, ignored -> new HashMap<>())
                            .put(sourceCollection, schema);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "Duplicate Milvus CDC source schema for "
                                + sourceDatabase
                                + "."
                                + sourceCollection);
            }
            tables.add(schema.getCatalogTable());
        }
        Map<String, Map<String, MilvusCdcCollectionSchema>> immutableSchemas = new HashMap<>();
        for (Map.Entry<String, Map<String, MilvusCdcCollectionSchema>> entry :
                schemasBySource.entrySet()) {
            immutableSchemas.put(entry.getKey(), Collections.unmodifiableMap(entry.getValue()));
        }
        this.schemasBySourceDatabaseAndCollection = Collections.unmodifiableMap(immutableSchemas);
        this.catalogTables = Collections.unmodifiableList(tables);
    }

    public Optional<MilvusCdcCollectionSchema> schemaForSourceCollection(
            String sourceDatabase, String sourceCollection) {
        if (sourceDatabase == null || sourceDatabase.trim().isEmpty()) {
            return Optional.empty();
        }
        if (sourceCollection == null || sourceCollection.trim().isEmpty()) {
            return Optional.empty();
        }
        Map<String, MilvusCdcCollectionSchema> schemasByCollection =
                schemasBySourceDatabaseAndCollection.get(sourceDatabase.trim());
        if (schemasByCollection == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(schemasByCollection.get(sourceCollection.trim()));
    }

    public List<CatalogTable> catalogTables() {
        return catalogTables;
    }

    private static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Milvus CDC schema " + fieldName + " must not be empty.");
        }
        return value.trim();
    }
}
