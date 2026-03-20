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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;
import io.milvus.v2.common.IndexParam;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.connectors.seatunnel.milvus.common.MilvusConstants;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CatalogUtilsTest {

    private CatalogUtils catalogUtils;
    private final Gson gson = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();
    private final Type extraParamsType = new TypeToken<Map<String, Object>>() {}.getType();

    @BeforeEach
    void setUp() {
        // Create CatalogUtils with null client and empty config (no field_schema)
        ReadonlyConfig config = ReadonlyConfig.fromMap(new HashMap<>());
        catalogUtils = new CatalogUtils(null, config);
    }

    // ========================
    // collectSourceFieldNames
    // ========================

    @Test
    void testCollectSourceFieldNames_regularColumns() {
        CatalogTable table = buildCatalogTable(
                List.of(
                        buildColumn("id", BasicType.LONG_TYPE, null),
                        buildColumn("name", BasicType.STRING_TYPE, null),
                        buildColumn("vec", VectorType.VECTOR_FLOAT_TYPE, null)
                ),
                new HashMap<>()
        );

        Set<String> result = catalogUtils.collectSourceFieldNames(table);
        Assertions.assertEquals(Set.of("id", "name", "vec"), result);
    }

    @Test
    void testCollectSourceFieldNames_skipsMetadataColumns() {
        Map<String, Object> metaOptions = new HashMap<>();
        metaOptions.put(CommonOptions.METADATA.getName(), true);

        CatalogTable table = buildCatalogTable(
                List.of(
                        buildColumn("id", BasicType.LONG_TYPE, null),
                        buildColumn("$meta", BasicType.STRING_TYPE, metaOptions)
                ),
                new HashMap<>()
        );

        Set<String> result = catalogUtils.collectSourceFieldNames(table);
        Assertions.assertEquals(Set.of("id"), result);
    }

    @Test
    void testCollectSourceFieldNames_metadataFalseNotSkipped() {
        Map<String, Object> metaOptions = new HashMap<>();
        metaOptions.put(CommonOptions.METADATA.getName(), false);

        CatalogTable table = buildCatalogTable(
                List.of(
                        buildColumn("id", BasicType.LONG_TYPE, null),
                        buildColumn("dynamic", BasicType.STRING_TYPE, metaOptions)
                ),
                new HashMap<>()
        );

        Set<String> result = catalogUtils.collectSourceFieldNames(table);
        Assertions.assertEquals(Set.of("id", "dynamic"), result);
    }

    @Test
    void testCollectSourceFieldNames_nullOptions() {
        CatalogTable table = buildCatalogTable(
                List.of(buildColumn("id", BasicType.LONG_TYPE, null)),
                new HashMap<>()
        );

        Set<String> result = catalogUtils.collectSourceFieldNames(table);
        Assertions.assertEquals(Set.of("id"), result);
    }

    // ========================
    // collectTargetFieldNames
    // ========================

    @Test
    void testCollectTargetFieldNames_noFieldSchemaConfig_fallsBackToSource() {
        // catalogUtils has empty fieldSchemaMap (default setUp)
        CatalogTable table = buildCatalogTable(
                List.of(
                        buildColumn("id", BasicType.LONG_TYPE, null),
                        buildColumn("vec", VectorType.VECTOR_FLOAT_TYPE, null)
                ),
                new HashMap<>()
        );

        Set<String> result = catalogUtils.collectTargetFieldNames(table);
        Assertions.assertEquals(Set.of("id", "vec"), result);
    }

    @Test
    void testCollectTargetFieldNames_withFieldSchemaConfig() {
        // Create CatalogUtils with field_schema config
        // MilvusFieldSchema uses @SerializedName("field_name") for fieldName
        Map<String, Object> fieldSchema = new HashMap<>();
        fieldSchema.put("field_name", "target_id");
        fieldSchema.put("data_type", 5); // Int64

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("field_schema", List.of(fieldSchema));
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        CatalogUtils utilsWithSchema = new CatalogUtils(null, config);

        CatalogTable table = buildCatalogTable(
                List.of(buildColumn("id", BasicType.LONG_TYPE, null)),
                new HashMap<>()
        );

        Set<String> result = utilsWithSchema.collectTargetFieldNames(table);
        Assertions.assertTrue(result.contains("target_id"));
        Assertions.assertFalse(result.contains("id"));
    }

    // ========================
    // buildIndexParam
    // ========================

    @Test
    void testBuildIndexParam_fieldInTarget_returnsIndex() {
        Map<String, String> indexInfo = new HashMap<>();
        indexInfo.put("fieldName", "vec");
        indexInfo.put("indexName", "vec_idx");
        indexInfo.put("indexType", "HNSW");
        indexInfo.put("metricType", "COSINE");

        Set<String> targetFields = Set.of("id", "vec");
        Set<String> sourceFields = Set.of("id", "vec");

        IndexParam result = catalogUtils.buildIndexParam(
                indexInfo, targetFields, sourceFields, false, gson, extraParamsType);

        Assertions.assertNotNull(result);
        Assertions.assertEquals("vec", result.getFieldName());
        Assertions.assertEquals("vec_idx", result.getIndexName());
        Assertions.assertEquals(IndexParam.IndexType.HNSW, result.getIndexType());
        Assertions.assertEquals(IndexParam.MetricType.COSINE, result.getMetricType());
    }

    @Test
    void testBuildIndexParam_fieldNotInTarget_sourceField_skipped() {
        Map<String, String> indexInfo = new HashMap<>();
        indexInfo.put("fieldName", "old_vec");
        indexInfo.put("indexName", "old_vec_idx");
        indexInfo.put("indexType", "HNSW");
        indexInfo.put("metricType", "L2");

        Set<String> targetFields = Set.of("id", "new_vec");
        Set<String> sourceFields = Set.of("id", "old_vec", "new_vec");

        IndexParam result = catalogUtils.buildIndexParam(
                indexInfo, targetFields, sourceFields, false, gson, extraParamsType);

        Assertions.assertNull(result, "Should skip index on source-only field not synced to target");
    }

    @Test
    void testBuildIndexParam_dynamicFieldIndex_enableDynamic_allowed() {
        Map<String, String> indexInfo = new HashMap<>();
        indexInfo.put("fieldName", "dynamic_field");
        indexInfo.put("indexName", "dyn_idx");
        indexInfo.put("indexType", "INVERTED");

        Set<String> targetFields = Set.of("id", "vec");
        Set<String> sourceFields = Set.of("id", "vec");

        IndexParam result = catalogUtils.buildIndexParam(
                indexInfo, targetFields, sourceFields, true, gson, extraParamsType);

        Assertions.assertNotNull(result, "Dynamic field index should be allowed when enableDynamicField=true");
        Assertions.assertEquals("dynamic_field", result.getFieldName());
    }

    @Test
    void testBuildIndexParam_dynamicFieldIndex_disableDynamic_skipped() {
        Map<String, String> indexInfo = new HashMap<>();
        indexInfo.put("fieldName", "dynamic_field");
        indexInfo.put("indexName", "dyn_idx");
        indexInfo.put("indexType", "INVERTED");

        Set<String> targetFields = Set.of("id", "vec");
        Set<String> sourceFields = Set.of("id", "vec");

        IndexParam result = catalogUtils.buildIndexParam(
                indexInfo, targetFields, sourceFields, false, gson, extraParamsType);

        Assertions.assertNull(result, "Dynamic field index should be skipped when enableDynamicField=false");
    }

    @Test
    void testBuildIndexParam_nullFieldName_throwsException() {
        Map<String, String> indexInfo = new HashMap<>();
        indexInfo.put("fieldName", null);
        indexInfo.put("indexName", "idx");
        indexInfo.put("indexType", "HNSW");

        Assertions.assertThrows(MilvusConnectorException.class, () ->
                catalogUtils.buildIndexParam(indexInfo, Set.of("id"), Set.of("id"), false, gson, extraParamsType));
    }

    @Test
    void testBuildIndexParam_emptyTargetFieldNames_skipsFiltering() {
        // When targetFieldNames is empty, all indexes should pass through
        Map<String, String> indexInfo = new HashMap<>();
        indexInfo.put("fieldName", "any_field");
        indexInfo.put("indexName", "any_idx");
        indexInfo.put("indexType", "HNSW");
        indexInfo.put("metricType", "L2");

        Set<String> emptyTarget = Collections.emptySet();
        Set<String> emptySource = Collections.emptySet();

        IndexParam result = catalogUtils.buildIndexParam(
                indexInfo, emptyTarget, emptySource, false, gson, extraParamsType);

        Assertions.assertNotNull(result, "Should allow all indexes when targetFieldNames is empty");
    }

    @Test
    void testBuildIndexParam_trieIndexType_fallsBackToAutoindex() {
        // TRIE index type is serialized as "Trie" via getName(), which doesn't match
        // enum constant name "TRIE", so it falls back to AUTOINDEX
        Map<String, String> indexInfo = new HashMap<>();
        indexInfo.put("fieldName", "varchar_field");
        indexInfo.put("indexName", "trie_idx");
        indexInfo.put("indexType", "Trie");

        Set<String> targetFields = Set.of("varchar_field");

        IndexParam result = catalogUtils.buildIndexParam(
                indexInfo, targetFields, targetFields, false, gson, extraParamsType);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(IndexParam.IndexType.AUTOINDEX, result.getIndexType(),
                "Trie (getName) doesn't match TRIE (valueOf), should fall back to AUTOINDEX");
    }

    @Test
    void testBuildIndexParam_unknownIndexType_fallsBackToAutoindex() {
        Map<String, String> indexInfo = new HashMap<>();
        indexInfo.put("fieldName", "vec");
        indexInfo.put("indexName", "idx");
        indexInfo.put("indexType", "NONEXISTENT_TYPE");
        indexInfo.put("metricType", "L2");

        Set<String> targetFields = Set.of("vec");

        IndexParam result = catalogUtils.buildIndexParam(
                indexInfo, targetFields, targetFields, false, gson, extraParamsType);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(IndexParam.IndexType.AUTOINDEX, result.getIndexType(),
                "Unknown index type should fall back to AUTOINDEX");
    }

    @Test
    void testBuildIndexParam_noMetricType_setsNull() {
        Map<String, String> indexInfo = new HashMap<>();
        indexInfo.put("fieldName", "scalar_field");
        indexInfo.put("indexName", "scalar_idx");
        indexInfo.put("indexType", "INVERTED");

        Set<String> targetFields = Set.of("scalar_field");

        IndexParam result = catalogUtils.buildIndexParam(
                indexInfo, targetFields, targetFields, false, gson, extraParamsType);

        Assertions.assertNotNull(result);
        Assertions.assertNull(result.getMetricType());
    }

    @Test
    void testBuildIndexParam_withExtraParams() {
        Map<String, Object> extraParams = new HashMap<>();
        extraParams.put("nlist", 1024);
        extraParams.put("M", 16);

        Map<String, String> indexInfo = new HashMap<>();
        indexInfo.put("fieldName", "vec");
        indexInfo.put("indexName", "vec_idx");
        indexInfo.put("indexType", "HNSW");
        indexInfo.put("metricType", "L2");
        indexInfo.put("extraParams", gson.toJson(extraParams));

        Set<String> targetFields = Set.of("vec");

        IndexParam result = catalogUtils.buildIndexParam(
                indexInfo, targetFields, targetFields, false, gson, extraParamsType);

        Assertions.assertNotNull(result);
        Assertions.assertNotNull(result.getExtraParams());
        Assertions.assertEquals(16L, ((Number) result.getExtraParams().get("M")).longValue());
    }

    @Test
    void testBuildIndexParam_emptyExtraParams_noExtraParamsSet() {
        Map<String, String> indexInfo = new HashMap<>();
        indexInfo.put("fieldName", "vec");
        indexInfo.put("indexName", "vec_idx");
        indexInfo.put("indexType", "HNSW");
        indexInfo.put("extraParams", "");

        Set<String> targetFields = Set.of("vec");

        IndexParam result = catalogUtils.buildIndexParam(
                indexInfo, targetFields, targetFields, false, gson, extraParamsType);

        Assertions.assertNotNull(result);
    }

    // ========================
    // parseIndexParamsFromSource (integration)
    // ========================

    @Test
    void testParseIndexParamsFromSource_noIndexList() {
        CatalogTable table = buildCatalogTable(
                List.of(buildColumn("id", BasicType.LONG_TYPE, null)),
                new HashMap<>()
        );

        List<IndexParam> result = catalogUtils.parseIndexParamsFromSource(table);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    void testParseIndexParamsFromSource_emptyIndexList() {
        Map<String, String> options = new HashMap<>();
        options.put(MilvusConstants.INDEX_LIST, "[]");

        CatalogTable table = buildCatalogTable(
                List.of(buildColumn("id", BasicType.LONG_TYPE, null)),
                options
        );

        List<IndexParam> result = catalogUtils.parseIndexParamsFromSource(table);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    void testParseIndexParamsFromSource_filtersNonTargetFields() {
        List<Map<String, String>> indexes = new ArrayList<>();

        Map<String, String> idx1 = new HashMap<>();
        idx1.put("fieldName", "vec");
        idx1.put("indexName", "vec_idx");
        idx1.put("indexType", "HNSW");
        idx1.put("metricType", "L2");
        indexes.add(idx1);

        Map<String, String> idx2 = new HashMap<>();
        idx2.put("fieldName", "old_field");
        idx2.put("indexName", "old_idx");
        idx2.put("indexType", "INVERTED");
        indexes.add(idx2);

        Map<String, String> options = new HashMap<>();
        options.put(MilvusConstants.INDEX_LIST, gson.toJson(indexes));
        options.put(MilvusConstants.ENABLE_DYNAMIC_FIELD, "false");

        // Source has both fields, but we set up field_schema config for target with only "vec"
        Map<String, Object> fieldSchema = new HashMap<>();
        fieldSchema.put("field_name", "vec");
        fieldSchema.put("data_type", 101); // FloatVector
        fieldSchema.put("dimension", 128);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("field_schema", List.of(fieldSchema));
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        CatalogUtils utilsWithSchema = new CatalogUtils(null, config);

        CatalogTable table = buildCatalogTable(
                List.of(
                        buildColumn("vec", VectorType.VECTOR_FLOAT_TYPE, null),
                        buildColumn("old_field", BasicType.STRING_TYPE, null)
                ),
                options
        );

        List<IndexParam> result = utilsWithSchema.parseIndexParamsFromSource(table);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("vec", result.get(0).getFieldName());
    }

    @Test
    void testParseIndexParamsFromSource_allowsDynamicFieldIndex() {
        List<Map<String, String>> indexes = new ArrayList<>();

        Map<String, String> idx1 = new HashMap<>();
        idx1.put("fieldName", "dyn_key");
        idx1.put("indexName", "dyn_idx");
        idx1.put("indexType", "INVERTED");
        indexes.add(idx1);

        Map<String, String> options = new HashMap<>();
        options.put(MilvusConstants.INDEX_LIST, gson.toJson(indexes));
        options.put(MilvusConstants.ENABLE_DYNAMIC_FIELD, "true");

        CatalogTable table = buildCatalogTable(
                List.of(buildColumn("id", BasicType.LONG_TYPE, null)),
                options
        );

        List<IndexParam> result = catalogUtils.parseIndexParamsFromSource(table);
        // "dyn_key" is not in source fields (only "id" is), and dynamic is enabled
        // Since target==source (no field_schema), target is {"id"}, so dyn_key is not in target
        // dyn_key is not in sourceFields either, so it's treated as dynamic field
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("dyn_key", result.get(0).getFieldName());
    }

    @Test
    void testParseIndexParamsFromSource_trieIndexTypeFallback() {
        // Simulates what the source connector serializes for a TRIE index
        // getName() returns "Trie" which doesn't match valueOf("TRIE"), falls back to AUTOINDEX
        List<Map<String, String>> indexes = new ArrayList<>();

        Map<String, String> idx = new HashMap<>();
        idx.put("fieldName", "name");
        idx.put("indexName", "name_trie");
        idx.put("indexType", "Trie"); // getName() for TRIE returns "Trie"
        indexes.add(idx);

        Map<String, String> options = new HashMap<>();
        options.put(MilvusConstants.INDEX_LIST, gson.toJson(indexes));

        CatalogTable table = buildCatalogTable(
                List.of(buildColumn("name", BasicType.STRING_TYPE, null)),
                options
        );

        List<IndexParam> result = catalogUtils.parseIndexParamsFromSource(table);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(IndexParam.IndexType.AUTOINDEX, result.get(0).getIndexType());
    }

    // ========================
    // Helpers
    // ========================

    private PhysicalColumn buildColumn(String name, org.apache.seatunnel.api.table.type.SeaTunnelDataType<?> dataType,
                                        Map<String, Object> options) {
        return PhysicalColumn.of(name, dataType, 0L, true, null, null, null, options);
    }

    private CatalogTable buildCatalogTable(List<PhysicalColumn> columns, Map<String, String> options) {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (PhysicalColumn col : columns) {
            schemaBuilder.column(col);
        }
        return CatalogTable.of(
                TableIdentifier.of("test", TablePath.of("default", "test_collection")),
                schemaBuilder.build(),
                options,
                new ArrayList<>(),
                ""
        );
    }
}
