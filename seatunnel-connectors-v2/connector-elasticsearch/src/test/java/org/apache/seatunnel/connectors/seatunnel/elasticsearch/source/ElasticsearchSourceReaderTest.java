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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SearchApiTypeEnum;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.PointInTimeResult;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ScrollResult;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ElasticsearchSourceReaderTest {

    private static final String INDEX = "idx";
    private static final String FIELD = "name";
    private static final int SHARD_ID = 0;
    private static final String NODE_ID = "node-1";
    private static final String PREFERENCE = "_shards:" + SHARD_ID + "|_prefer_nodes:" + NODE_ID;
    private static final String SCROLL_TIME = "1m";
    private static final String SCROLL_ID = "scroll-1";
    private static final String INITIAL_PIT_ID = "pit-1";
    private static final String ROTATED_PIT_ID = "pit-2";
    private static final int BATCH_SIZE = 100;
    private static final long PIT_KEEP_ALIVE = 3600000L;

    private final Object checkpointLock = new Object();

    @Test
    public void scrollSearchReadsAllBatchesAndClearsLastScrollId() throws Exception {
        EsRestClient client = mock(EsRestClient.class);
        ScrollResult first = scrollResult("scroll-1", 2, doc("alice"));
        ScrollResult second = scrollResult("scroll-2", -1, doc("bob"));
        ScrollResult empty = scrollResult("scroll-3", -1);
        when(client.searchByScroll(
                        eq(INDEX),
                        eq(Collections.singletonList(FIELD)),
                        any(),
                        eq(SCROLL_TIME),
                        eq(BATCH_SIZE),
                        eq(PREFERENCE)))
                .thenReturn(first);
        when(client.searchWithScrollId(eq("scroll-1"), eq(SCROLL_TIME))).thenReturn(second);
        when(client.searchWithScrollId(eq("scroll-2"), eq(SCROLL_TIME))).thenReturn(empty);

        Collector<SeaTunnelRow> collector = collector();
        pollOnce(client, SearchApiTypeEnum.SCROLL, collector);

        verify(collector, times(2)).collect(any(SeaTunnelRow.class));
        verify(client).clearScroll("scroll-3");
    }

    @Test
    public void scrollSearchFailsWhenFirstResponseLacksTotalHits() throws Exception {
        EsRestClient client = mock(EsRestClient.class);
        ScrollResult result = new ScrollResult();
        result.setScrollId(SCROLL_ID);
        result.setDocs(Collections.emptyList());
        result.setTotalHits(-1);
        when(client.searchByScroll(
                        eq(INDEX),
                        eq(Collections.singletonList(FIELD)),
                        any(),
                        eq(SCROLL_TIME),
                        eq(BATCH_SIZE),
                        eq(PREFERENCE)))
                .thenReturn(result);

        ElasticsearchConnectorException exception =
                Assertions.assertThrows(
                        ElasticsearchConnectorException.class,
                        () -> pollOnce(client, SearchApiTypeEnum.SCROLL));

        Assertions.assertTrue(exception.getMessage().contains("Refusing scrollSearch without hits.total"));
        verify(client).clearScroll(SCROLL_ID);
    }

    @Test
    public void scrollSearchFailsWhenProcessedCountDoesNotMatchTotalHits() throws Exception {
        EsRestClient client = mock(EsRestClient.class);
        ScrollResult result = scrollResult(SCROLL_ID, 1);
        when(client.searchByScroll(
                        eq(INDEX),
                        eq(Collections.singletonList(FIELD)),
                        any(),
                        eq(SCROLL_TIME),
                        eq(BATCH_SIZE),
                        eq(PREFERENCE)))
                .thenReturn(result);

        ElasticsearchConnectorException exception =
                Assertions.assertThrows(
                        ElasticsearchConnectorException.class,
                        () -> pollOnce(client, SearchApiTypeEnum.SCROLL));

        Assertions.assertTrue(exception.getMessage().contains("scrollSearch data inconsistency"));
        verify(client).clearScroll(SCROLL_ID);
    }

    @Test
    public void pitSearchReadsBatchesWithSearchAfterAndDeletesLatestPit() throws Exception {
        EsRestClient client = mock(EsRestClient.class);
        when(client.createPointInTime(eq(INDEX), eq(PIT_KEEP_ALIVE), eq(PREFERENCE)))
                .thenReturn(INITIAL_PIT_ID);
        when(client.doSearchWithPointInTime(
                        eq(Collections.singletonList(FIELD)),
                        any(),
                        eq(BATCH_SIZE),
                        eq(INITIAL_PIT_ID),
                        isNull(),
                        eq(PIT_KEEP_ALIVE),
                        eq(true)))
                .thenReturn(
                        PointInTimeResult.builder()
                                .pitId(ROTATED_PIT_ID)
                                .docs(Collections.singletonList(doc("alice")))
                                .totalHits(1)
                                .searchAfter(new Object[] {10L})
                                .build());
        when(client.doSearchWithPointInTime(
                        eq(Collections.singletonList(FIELD)),
                        any(),
                        eq(BATCH_SIZE),
                        eq(ROTATED_PIT_ID),
                        argThat(searchAfter -> Arrays.equals(searchAfter, new Object[] {10L})),
                        eq(PIT_KEEP_ALIVE),
                        eq(false)))
                .thenReturn(
                        PointInTimeResult.builder()
                                .pitId(ROTATED_PIT_ID)
                                .docs(Collections.emptyList())
                                .totalHits(-1)
                                .build());

        Collector<SeaTunnelRow> collector = collector();
        pollOnce(client, SearchApiTypeEnum.PIT, collector);

        verify(collector).collect(any(SeaTunnelRow.class));
        verify(client).deletePointInTime(ROTATED_PIT_ID);
    }

    @Test
    public void pitSearchFailsWhenFirstResponseLacksTotalHits() throws Exception {
        EsRestClient client = mock(EsRestClient.class);
        when(client.createPointInTime(eq(INDEX), eq(PIT_KEEP_ALIVE), eq(PREFERENCE)))
                .thenReturn(INITIAL_PIT_ID);
        when(client.doSearchWithPointInTime(
                        eq(Collections.singletonList(FIELD)),
                        any(),
                        eq(BATCH_SIZE),
                        eq(INITIAL_PIT_ID),
                        isNull(),
                        eq(PIT_KEEP_ALIVE),
                        eq(true)))
                .thenReturn(
                        PointInTimeResult.builder()
                                .pitId(ROTATED_PIT_ID)
                                .docs(Collections.emptyList())
                                .totalHits(-1)
                                .build());

        ElasticsearchConnectorException exception =
                Assertions.assertThrows(
                        ElasticsearchConnectorException.class,
                        () -> pollOnce(client, SearchApiTypeEnum.PIT));

        Assertions.assertTrue(exception.getMessage().contains("Refusing pitSearch without hits.total"));
        verify(client).deletePointInTime(ROTATED_PIT_ID);
    }

    @Test
    public void pitSearchFailsWhenNonEmptyBatchLacksSearchAfter() throws Exception {
        EsRestClient client = mock(EsRestClient.class);
        when(client.createPointInTime(eq(INDEX), eq(PIT_KEEP_ALIVE), eq(PREFERENCE)))
                .thenReturn(INITIAL_PIT_ID);
        when(client.doSearchWithPointInTime(
                        eq(Collections.singletonList(FIELD)),
                        any(),
                        eq(BATCH_SIZE),
                        eq(INITIAL_PIT_ID),
                        isNull(),
                        eq(PIT_KEEP_ALIVE),
                        eq(true)))
                .thenReturn(
                        PointInTimeResult.builder()
                                .pitId(ROTATED_PIT_ID)
                                .docs(Collections.singletonList(doc("alice")))
                                .totalHits(1)
                                .build());

        ElasticsearchConnectorException exception =
                Assertions.assertThrows(
                        ElasticsearchConnectorException.class,
                        () -> pollOnce(client, SearchApiTypeEnum.PIT));

        Assertions.assertTrue(exception.getMessage().contains("no sort values"));
        verify(client).deletePointInTime(ROTATED_PIT_ID);
    }

    @Test
    public void pitSearchFailsWhenProcessedCountDoesNotMatchTotalHits() throws Exception {
        EsRestClient client = mock(EsRestClient.class);
        when(client.createPointInTime(eq(INDEX), eq(PIT_KEEP_ALIVE), eq(PREFERENCE)))
                .thenReturn(INITIAL_PIT_ID);
        when(client.doSearchWithPointInTime(
                        eq(Collections.singletonList(FIELD)),
                        any(),
                        eq(BATCH_SIZE),
                        eq(INITIAL_PIT_ID),
                        isNull(),
                        eq(PIT_KEEP_ALIVE),
                        eq(true)))
                .thenReturn(
                        PointInTimeResult.builder()
                                .pitId(ROTATED_PIT_ID)
                                .docs(Collections.emptyList())
                                .totalHits(1)
                                .build());

        ElasticsearchConnectorException exception =
                Assertions.assertThrows(
                        ElasticsearchConnectorException.class,
                        () -> pollOnce(client, SearchApiTypeEnum.PIT));

        Assertions.assertTrue(exception.getMessage().contains("pitSearch data inconsistency"));
        verify(client).deletePointInTime(ROTATED_PIT_ID);
    }

    private void pollOnce(EsRestClient client, SearchApiTypeEnum searchApiType) throws Exception {
        pollOnce(client, searchApiType, collector());
    }

    private void pollOnce(
            EsRestClient client,
            SearchApiTypeEnum searchApiType,
            Collector<SeaTunnelRow> collector)
            throws Exception {
        ElasticsearchSourceReader reader =
                new ElasticsearchSourceReader(mock(SourceReader.Context.class), readerConfig());
        Field clientField = ElasticsearchSourceReader.class.getDeclaredField("esRestClient");
        clientField.setAccessible(true);
        clientField.set(reader, client);
        reader.addSplits(
                Collections.singletonList(
                        new ElasticsearchSourceSplit(
                                sourceConfig(searchApiType),
                                INDEX,
                                SHARD_ID,
                                NODE_ID,
                                null)));
        reader.pollNext(collector);
    }

    private ReadonlyConfig readerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("hosts", Collections.singletonList("http://localhost:9200"));
        config.put("wan_only", true);
        return ReadonlyConfig.fromMap(config);
    }

    private SourceConfig sourceConfig(SearchApiTypeEnum searchApiType) {
        SourceConfig config = new SourceConfig();
        config.setIndex(INDEX);
        config.setSource(Collections.singletonList(FIELD));
        config.setQuery(Collections.singletonMap("match_all", Collections.emptyMap()));
        config.setScrollTime(SCROLL_TIME);
        config.setScrollSize(BATCH_SIZE);
        config.setSearchApiType(searchApiType);
        config.setPitKeepAlive(PIT_KEEP_ALIVE);
        config.setPitBatchSize(BATCH_SIZE);
        config.setCatalogTable(catalogTable());
        return config;
    }

    private CatalogTable catalogTable() {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name(FIELD)
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("elasticsearch", TablePath.of("default", INDEX)),
                schema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }

    private Collector<SeaTunnelRow> collector() {
        @SuppressWarnings("unchecked")
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(checkpointLock);
        return collector;
    }

    private static ScrollResult scrollResult(
            String scrollId, long totalHits, Map<String, Object>... docs) {
        ScrollResult result = new ScrollResult();
        result.setScrollId(scrollId);
        result.setDocs(Arrays.asList(docs));
        result.setTotalHits(totalHits);
        return result;
    }

    private static Map<String, Object> doc(String name) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("_index", INDEX);
        doc.put("_id", name);
        doc.put(FIELD, name);
        return doc;
    }
}
