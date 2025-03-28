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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.opengauss;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialect;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class OpenGaussDialect extends PostgresDialect {

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String updateClause =
                Arrays.stream(fieldNames)
                        .filter(fieldName -> !Arrays.asList(uniqueKeyFields).contains(fieldName))
                        .map(
                                fieldName ->
                                        quoteIdentifier(fieldName)
                                                + "=EXCLUDED."
                                                + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));
        if (updateClause.isEmpty()) {
            return Optional.empty();
        }
        String upsertSQL =
                String.format(
                        "%s ON DUPLICATE KEY UPDATE %s",
                        getInsertIntoStatement(database, tableName, fieldNames), updateClause);
        return Optional.of(upsertSQL);
    }
}
