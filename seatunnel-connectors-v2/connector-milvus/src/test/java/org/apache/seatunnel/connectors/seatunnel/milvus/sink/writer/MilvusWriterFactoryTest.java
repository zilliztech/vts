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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.partition.request.HasPartitionReq;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class MilvusWriterFactoryTest {

    @Test
    void cdcReusesOneCollectionWriterWithoutPartitionAliases() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusWriterFactory factory = routingFactory("CDC", false, client);
        assertTrue(factory.getWriters().isEmpty());
        MilvusWriter writer = factory.getOrCreateWriter(partitionRow("p1"));
        assertSame(writer, factory.getOrCreateWriter(partitionRow("p2")));
        assertSame(writer, factory.getOrCreateWriter(partitionRow(null)));
        assertSame(writer, factory.getOrCreateWriter(partitionRow("_default")));
        assertEquals(Collections.singletonList(writer), factory.getWriters());
        MilvusWriterFactory otherSink = routingFactory("CDC", false, client);
        assertNotSame(writer, otherSink.getOrCreateWriter(partitionRow("p1")));
        verifyNoInteractions(client);
    }

    @Test
    void appendCachesDistinctWritersByNormalizedPartition() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        when(client.hasPartition(any(HasPartitionReq.class))).thenReturn(true);
        MilvusWriterFactory factory = routingFactory("APPEND", false, client);
        MilvusWriter first = factory.getOrCreateWriter(partitionRow("p-1"));
        assertSame(first, factory.getOrCreateWriter(partitionRow("p_1")));
        MilvusWriter second = factory.getOrCreateWriter(partitionRow("p2"));
        assertNotSame(first, second);
        MilvusWriter defaultWriter = factory.getOrCreateWriter(partitionRow(null));
        assertSame(defaultWriter, factory.getOrCreateWriter(partitionRow("")));
        assertSame(defaultWriter, factory.getOrCreateWriter(partitionRow("_default")));
        assertEquals(Arrays.asList(first, second, defaultWriter), factory.getWriters());
        factory.getWriters().clear();
        assertEquals(3, factory.getWriters().size());
    }

    @Test
    void appendPartitionKeyRoutesPhysicalPartitionsToDefaultWriter() {
        MilvusClientV2 client = mock(MilvusClientV2.class);
        MilvusWriterFactory factory = routingFactory("APPEND", true, client);
        MilvusWriter writer = factory.getOrCreateWriter(partitionRow("p1"));
        assertSame(writer, factory.getOrCreateWriter(partitionRow("p2")));
        assertEquals(Collections.singletonList(writer), factory.getWriters());
        verifyNoInteractions(client);
    }

    private static SeaTunnelRow partitionRow(String partition) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1L, "alice"});
        row.setPartitionName(partition);
        return row;
    }

    private static MilvusWriterFactory routingFactory(
            String mode, boolean partitionKey, MilvusClientV2 client) {
        CreateCollectionReq.CollectionSchema schema =
                CreateCollectionReq.CollectionSchema.builder()
                        .fieldSchemaList(
                                Arrays.asList(
                                        CreateCollectionReq.FieldSchema.builder()
                                                .name("id")
                                                .dataType(DataType.Int64)
                                                .isPrimaryKey(true)
                                                .build(),
                                        CreateCollectionReq.FieldSchema.builder()
                                                .name("name")
                                                .dataType(DataType.VarChar)
                                                .isPartitionKey(partitionKey)
                                                .build()))
                        .functionList(Collections.emptyList())
                        .build();
        DescribeCollectionResp description =
                DescribeCollectionResp.builder()
                        .collectionName("test_collection")
                        .primaryFieldName("id")
                        .autoID(false)
                        .collectionSchema(schema)
                        .build();
        return new MilvusWriterFactory(
                catalogTable(),
                ReadonlyConfig.fromMap(Collections.singletonMap("write_mode", mode)),
                client,
                description);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2})
    void cdcRejectsNonSingleReplica(int replicas) {
        HashMap<String, Object> config = new HashMap<>();
        config.put("write_mode", "CDC");
        config.put("multi_table_sink_replica", replicas);
        MilvusConnectorException error =
                assertThrows(
                        MilvusConnectorException.class,
                        () ->
                                new MilvusWriterFactory(
                                        catalogTable(),
                                        ReadonlyConfig.fromMap(config),
                                        mock(MilvusClientV2.class),
                                        DescribeCollectionResp.builder()
                                                .collectionSchema(
                                                        CreateCollectionReq.CollectionSchema
                                                                .builder()
                                                                .fieldSchemaList(
                                                                        Collections.emptyList())
                                                                .build())
                                                .build()));
        assertTrue(error.getMessage().contains("multi_table_sink_replica = 1"));
    }

    @Test
    void appendAllowsMultipleReplicas() {
        HashMap<String, Object> config = new HashMap<>();
        config.put("write_mode", "APPEND");
        config.put("multi_table_sink_replica", 2);
        assertDoesNotThrow(
                () ->
                        new MilvusWriterFactory(
                                catalogTable(),
                                ReadonlyConfig.fromMap(config),
                                mock(MilvusClientV2.class),
                                DescribeCollectionResp.builder()
                                        .collectionSchema(
                                                CreateCollectionReq.CollectionSchema.builder()
                                                        .fieldSchemaList(Collections.emptyList())
                                                        .build())
                                        .build()));
    }

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

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"false", "", "invalid"})
    void cdcWriteModeRejectsAutoIdWithoutPermission(String property) {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put("write_mode", MilvusSinkWriteMode.CDC);
        configMap.put("cdc_overlap_upsert_cutoff_ms", 100L);
        configMap.put("enable_upsert", true);
        DescribeCollectionResp describeCollectionResp =
                DescribeCollectionResp.builder()
                        .autoID(true)
                        .properties(
                                property == null
                                        ? null
                                        : Collections.singletonMap(
                                                "allow_insert_auto_id", property))
                        .build();

        MilvusConnectorException exception =
                assertThrows(
                        MilvusConnectorException.class,
                        () ->
                                new MilvusWriterFactory(
                                        catalogTable(),
                                        ReadonlyConfig.fromMap(configMap),
                                        mock(MilvusClientV2.class),
                                        describeCollectionResp));

        assertTrue(exception.getMessage().contains("allow_insert_auto_id"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "True", "TRUE"})
    void cdcWriteModeAllowsAutoIdWithExplicitIdPermission(String property) {
        HashMap<String, Object> config = new HashMap<>();
        config.put("write_mode", MilvusSinkWriteMode.CDC);
        DescribeCollectionResp description =
                DescribeCollectionResp.builder()
                        .autoID(true)
                        .properties(Collections.singletonMap("allow_insert_auto_id", property))
                        .build();
        assertDoesNotThrow(
                () ->
                        new MilvusWriterFactory(
                                catalogTable(),
                                ReadonlyConfig.fromMap(config),
                                mock(MilvusClientV2.class),
                                description));
    }

    @Test
    void cdcWriteModeRejectsAutoIdWhenPropertyIsMissing() {
        HashMap<String, Object> config = new HashMap<>();
        config.put("write_mode", MilvusSinkWriteMode.CDC);
        DescribeCollectionResp description =
                DescribeCollectionResp.builder()
                        .autoID(true)
                        .properties(Collections.singletonMap("collection.ttl.seconds", "60"))
                        .build();
        assertThrows(
                MilvusConnectorException.class,
                () ->
                        new MilvusWriterFactory(
                                catalogTable(),
                                ReadonlyConfig.fromMap(config),
                                mock(MilvusClientV2.class),
                                description));
    }

    private static MilvusWriterFactory createFactory() {
        return new MilvusWriterFactory(
                catalogTable(),
                ReadonlyConfig.fromMap(new HashMap<>()),
                mock(MilvusClientV2.class),
                DescribeCollectionResp.builder()
                        .collectionSchema(
                                CreateCollectionReq.CollectionSchema.builder()
                                        .fieldSchemaList(Collections.emptyList())
                                        .build())
                        .build());
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
