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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkWriteMode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class MilvusWriterFactoryTest {

    @Test
    void validatesRowFieldCountBeforeWriting() {
        MilvusWriterFactory factory = createFactory();

        assertDoesNotThrow(() -> factory.validateRow(new SeaTunnelRow(new Object[] {1L, "alice"})));

        MilvusConnectorException exception =
                assertThrows(
                        MilvusConnectorException.class,
                        () -> factory.validateRow(new SeaTunnelRow(new Object[] {1L})));
        assertTrue(exception.getMessage().contains("expected 2 fields, but received 1"));
    }

    @Test
    void cdcWriteModeRejectsAutoIdCollection() {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put("write_mode", MilvusSinkWriteMode.CDC);
        DescribeCollectionResp describeCollectionResp =
                DescribeCollectionResp.builder().autoID(true).build();

        MilvusConnectorException exception =
                assertThrows(
                        MilvusConnectorException.class,
                        () ->
                                new MilvusWriterFactory(
                                        catalogTable(),
                                        ReadonlyConfig.fromMap(configMap),
                                        mock(MilvusClientV2.class),
                                        describeCollectionResp));

        assertTrue(exception.getMessage().contains("does not support autoID"));
    }

    private static MilvusWriterFactory createFactory() {
        return new MilvusWriterFactory(
                catalogTable(),
                ReadonlyConfig.fromMap(new HashMap<>()),
                mock(MilvusClientV2.class),
                mock(DescribeCollectionResp.class));
    }

    private static CatalogTable catalogTable() {
        List<Column> columns =
                Arrays.asList(
                        PhysicalColumn.builder().name("id").dataType(BasicType.LONG_TYPE).build(),
                        PhysicalColumn.builder()
                                .name("name")
                                .dataType(BasicType.STRING_TYPE)
                                .build());

        return CatalogTable.of(
                TableIdentifier.of("test", TablePath.of("default", "test_collection")),
                TableSchema.builder().columns(columns).build(),
                new HashMap<>(),
                Collections.emptyList(),
                "");
    }
}
