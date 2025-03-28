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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql;

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
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class MysqlCreateTableSqlBuilderTest {

    private static final PrintStream CONSOLE = System.out;

    @Test
    public void testBuild() {
        // todo
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

        String createTableSql =
                MysqlCreateTableSqlBuilder.builder(
                                tablePath, catalogTable, MySqlTypeConverter.DEFAULT_INSTANCE, true)
                        .build(DatabaseIdentifier.MYSQL);
        // create table sql is change; The old unit tests are no longer applicable
        String expect =
                "CREATE TABLE `test_table` (\n"
                        + "\t`id` BIGINT NOT NULL COMMENT 'id', \n"
                        + "\t`name` VARCHAR(128) NOT NULL COMMENT 'name', \n"
                        + "\t`age` INT NULL COMMENT 'age', \n"
                        + "\t`blob_v` LONGBLOB NULL COMMENT 'blob_v', \n"
                        + "\t`createTime` DATETIME NULL COMMENT 'createTime', \n"
                        + "\t`lastUpdateTime` DATETIME NULL COMMENT 'lastUpdateTime', \n"
                        + "\tPRIMARY KEY (`id`), \n"
                        + "\tKEY `name` (`name`), \n"
                        + "\tKEY `blob_v` (`blob_v`(255))\n"
                        + ") COMMENT = 'User table';";
        CONSOLE.println(expect);
        Assertions.assertEquals(expect, createTableSql);

        // skip index
        String createTableSqlSkipIndex =
                MysqlCreateTableSqlBuilder.builder(
                                tablePath, catalogTable, MySqlTypeConverter.DEFAULT_INSTANCE, false)
                        .build(DatabaseIdentifier.MYSQL);
        String expectSkipIndex =
                "CREATE TABLE `test_table` (\n"
                        + "\t`id` BIGINT NOT NULL COMMENT 'id', \n"
                        + "\t`name` VARCHAR(128) NOT NULL COMMENT 'name', \n"
                        + "\t`age` INT NULL COMMENT 'age', \n"
                        + "\t`blob_v` LONGBLOB NULL COMMENT 'blob_v', \n"
                        + "\t`createTime` DATETIME NULL COMMENT 'createTime', \n"
                        + "\t`lastUpdateTime` DATETIME NULL COMMENT 'lastUpdateTime'\n"
                        + ") COMMENT = 'User table';";
        CONSOLE.println(expectSkipIndex);
        Assertions.assertEquals(expectSkipIndex, createTableSqlSkipIndex);
    }
}
