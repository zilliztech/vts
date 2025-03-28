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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class XuguCreateTableSqlBuilderTest {

    private static final PrintStream CONSOLE = System.out;

    @Test
    public void testBuild() {
        String dataBaseName = "test_database";
        String tableName = "test_table";
        TablePath tablePath = TablePath.of(dataBaseName, tableName);
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, "id"))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 128, false, null, "name"))
                        .column(
                                PhysicalColumn.of(
                                        "age", BasicType.INT_TYPE, (Long) null, true, null, "age"))
                        .column(
                                PhysicalColumn.of(
                                        "blob_v",
                                        PrimitiveByteArrayType.INSTANCE,
                                        Long.MAX_VALUE,
                                        true,
                                        null,
                                        "blob_v"))
                        .column(
                                PhysicalColumn.of(
                                        "createTime",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        3,
                                        true,
                                        null,
                                        "createTime"))
                        .column(
                                PhysicalColumn.of(
                                        "lastUpdateTime",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        3,
                                        true,
                                        null,
                                        "lastUpdateTime"))
                        .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                        .constraintKey(
                                Arrays.asList(
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.INDEX_KEY,
                                                "name",
                                                Lists.newArrayList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "name", null))),
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.INDEX_KEY,
                                                "blob_v",
                                                Lists.newArrayList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "blob_v", null)))))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", dataBaseName, tableName),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "User table");

        XuguCreateTableSqlBuilder xuguCreateTableSqlBuilder =
                new XuguCreateTableSqlBuilder(catalogTable, true);
        String createTableSql = xuguCreateTableSqlBuilder.build(tablePath);
        // create table sql is change; The old unit tests are no longer applicable
        String expect =
                "CREATE TABLE \"test_table\" (\n"
                        + "\"id\" BIGINT NOT NULL,\n"
                        + "\"name\" VARCHAR(128) NOT NULL,\n"
                        + "\"age\" INTEGER,\n"
                        + "\"blob_v\" BLOB,\n"
                        + "\"createTime\" TIMESTAMP,\n"
                        + "\"lastUpdateTime\" TIMESTAMP,\n"
                        + "CONSTRAINT id_88a3 PRIMARY KEY (\"id\")\n"
                        + ");\n"
                        + "COMMENT ON COLUMN \"test_table\".\"id\" IS 'id';\n"
                        + "COMMENT ON COLUMN \"test_table\".\"name\" IS 'name';\n"
                        + "COMMENT ON COLUMN \"test_table\".\"age\" IS 'age';\n"
                        + "COMMENT ON COLUMN \"test_table\".\"blob_v\" IS 'blob_v';\n"
                        + "COMMENT ON COLUMN \"test_table\".\"createTime\" IS 'createTime';\n"
                        + "COMMENT ON COLUMN \"test_table\".\"lastUpdateTime\" IS 'lastUpdateTime'";

        // replace "CONSTRAINT id_xxxx" because it's dynamically generated(random)
        String regex = "id_\\w+";
        String replacedStr1 = createTableSql.replaceAll(regex, "id_");
        String replacedStr2 = expect.replaceAll(regex, "id_");
        CONSOLE.println(replacedStr2);
        Assertions.assertEquals(replacedStr2, replacedStr1);

        // skip index
        XuguCreateTableSqlBuilder xuguCreateTableSqlBuilderSkipIndex =
                new XuguCreateTableSqlBuilder(catalogTable, false);
        String createTableSqlSkipIndex = xuguCreateTableSqlBuilderSkipIndex.build(tablePath);
        String expectSkipIndex =
                "CREATE TABLE \"test_table\" (\n"
                        + "\"id\" BIGINT NOT NULL,\n"
                        + "\"name\" VARCHAR(128) NOT NULL,\n"
                        + "\"age\" INTEGER,\n"
                        + "\"blob_v\" BLOB,\n"
                        + "\"createTime\" TIMESTAMP,\n"
                        + "\"lastUpdateTime\" TIMESTAMP\n"
                        + ");\n"
                        + "COMMENT ON COLUMN \"test_table\".\"id\" IS 'id';\n"
                        + "COMMENT ON COLUMN \"test_table\".\"name\" IS 'name';\n"
                        + "COMMENT ON COLUMN \"test_table\".\"age\" IS 'age';\n"
                        + "COMMENT ON COLUMN \"test_table\".\"blob_v\" IS 'blob_v';\n"
                        + "COMMENT ON COLUMN \"test_table\".\"createTime\" IS 'createTime';\n"
                        + "COMMENT ON COLUMN \"test_table\".\"lastUpdateTime\" IS 'lastUpdateTime'";
        CONSOLE.println(expectSkipIndex);
        Assertions.assertEquals(expectSkipIndex, createTableSqlSkipIndex);
    }
}
