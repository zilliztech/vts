package org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog;

import com.google.gson.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.milvus.common.MilvusConstants;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.ENABLE_DYNAMIC_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig.FIELD_SCHEMA;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils.MilvusSchemaConverter;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class CatalogUtils {
    private final MilvusClientV2 client;
    private final ReadonlyConfig config;
    private final Map<String, MilvusFieldSchema> fieldSchemaMap;

    CatalogUtils(MilvusClientV2 client, ReadonlyConfig config) {
        this.client = client;
        this.config = config;
        this.fieldSchemaMap = parseFieldSchemaConfig();
    }

    private Map<String, MilvusFieldSchema> parseFieldSchemaConfig() {
        Map<String, MilvusFieldSchema> schemaMap = new java.util.HashMap<>();

        if (config.get(FIELD_SCHEMA) != null && !config.get(FIELD_SCHEMA).isEmpty()) {
            Gson gson = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();
            for (Object field : config.get(FIELD_SCHEMA)) {
                try {
                    Type type = new TypeToken<MilvusFieldSchema>() {
                    }.getType();
                    String json = gson.toJson(field);
                    MilvusFieldSchema fieldSchema = gson.fromJson(json, type);
                    if (fieldSchema != null) {
                        String key = fieldSchema.getEffectiveFieldName();
                        if (key != null) {
                            schemaMap.put(key, fieldSchema);
                        }
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse field schema: {}, error: {}", field, e.getMessage());
                }
            }
            log.info("Loaded field_schema config for {} fields", schemaMap.size());
        }

        return schemaMap;
    }

    void createIndex(TablePath tablePath, CatalogTable catalogTable) {
        Map<String, String> options = catalogTable.getOptions();

        List<IndexParam> indexParams = new ArrayList<>();

        // Check if there are existing indexes from source metadata in options
        if (options.containsKey(MilvusConstants.INDEX_LIST)) {
            String indexListStr = options.get(MilvusConstants.INDEX_LIST);
            if (StringUtils.isNotEmpty(indexListStr) && !indexListStr.equals("[]")) {
                try {
                    Gson gson = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();
                    Type indexListType = new TypeToken<List<Map<String, String>>>() {
                    }.getType();
                    List<Map<String, String>> indexes = gson.fromJson(indexListStr, indexListType);

                    if (indexes != null && !indexes.isEmpty()) {
                        for (Map<String, String> indexInfo : indexes) {

                            IndexParam.MetricType metricType = indexInfo.containsKey("metricType")
                                    ? IndexParam.MetricType.valueOf(indexInfo.get("metricType"))
                                    : null;

                            IndexParam.IndexType indexType;
                            try {
                                indexType = IndexParam.IndexType.valueOf(indexInfo.get("indexType"));
                            } catch (Exception e) {
                                log.warn("Unknown index type: {}, using default AUTOINDEX", indexInfo.get("indexType"));
                                indexType = IndexParam.IndexType.AUTOINDEX;
                            }
                            Map<String, Object> extraParams = new HashMap<>();
                            if (indexInfo.containsKey("extraParams")) {
                                String extraParamsStr = indexInfo.get("extraParams");
                                extraParams = gson.fromJson(extraParamsStr,
                                        new TypeToken<Map<String, Object>>(){}.getType());
                            }
                            IndexParam indexParam = IndexParam.builder()
                                    .fieldName(indexInfo.get("fieldName"))
                                    .metricType(metricType)
                                    .indexType(indexType)
                                    .indexName(indexInfo.get("indexName"))
                                    .extraParams(extraParams)
                                    .build();
                            indexParams.add(indexParam);
                            log.info("Using existing index from source: {}", indexInfo);
                        }
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse index list from options: {}, error: {}", indexListStr, e.getMessage());
                }
            }
        }

        log.info("indexParams: {}", indexParams);
        // create index
        CreateIndexReq createIndexReq = CreateIndexReq.builder()
                .collectionName(tablePath.getTableName())
                .indexParams(indexParams)
                .build();
        this.client.createIndex(createIndexReq);
    }

    void createTableInternal(TablePath tablePath, CatalogTable catalogTable) {
        // Delegate to specific method based on whether field_schema is configured
        if (!fieldSchemaMap.isEmpty()) {
            createTableFromFieldSchema(tablePath, catalogTable);
        } else {
            createTableFromSource(tablePath, catalogTable);
        }
    }

    /**
     * Create collection schema from field_schema config only.
     * Schema is built entirely from configuration.
     */
    private void createTableFromFieldSchema(TablePath tablePath, CatalogTable catalogTable) {
        log.info("Creating collection from field_schema config, {} fields defined", fieldSchemaMap.size());

        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();

        // Build all fields from config
        for (MilvusFieldSchema milvusFieldSchema : fieldSchemaMap.values()) {
            CreateCollectionReq.FieldSchema fieldSchema = createFieldSchemaFromConfig(milvusFieldSchema);
            fieldSchemaList.add(fieldSchema);
            log.info("Added field from config: {}", fieldSchema.getName());
        }

        // Extract struct fields from columns (if any) and merge with config
        List<CreateCollectionReq.StructFieldSchema> structFieldsList = getStructFieldsFromColumnsAndConfig(
                catalogTable, new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create());

        // Create collection with configured schema
        createCollection(tablePath, catalogTable, fieldSchemaList, structFieldsList);
    }

    /**
     * Create collection schema from source schema.
     * This is the default behavior when field_schema is not configured.
     */
    private void createTableFromSource(TablePath tablePath, CatalogTable catalogTable) {
        log.info("Creating collection from source schema");

        Map<String, String> options = catalogTable.getOptions();
        TableSchema tableSchema = catalogTable.getTableSchema();
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = new ArrayList<>();
        Gson gson = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

        if ((tableSchema.getPrimaryKey() == null || tableSchema.getPrimaryKey().getColumnNames().size() > 1)) {
            CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                    .name("Auto_id")
                    .isPrimaryKey(true)
                    .autoID(true)
                    .dataType(DataType.Int64)
                    .build();
            fieldSchemaList.add(fieldSchema);
        }

        // Convert all source columns to field schemas (skip struct fields)
        for (Column column : tableSchema.getColumns()) {
            if (column.getOptions() != null
                    && column.getOptions().containsKey(CommonOptions.METADATA.getName())
                    && (Boolean) column.getOptions().get(CommonOptions.METADATA.getName())) {
                // skip dynamic field
                continue;
            }

            // Check if this is an Array[Struct] field
            boolean isArrayStruct = column.getOptions() != null
                    && column.getOptions().containsKey(MilvusConstants.STRUCT_FIELDS);

            if (!isArrayStruct) {
                // Regular field - add to fieldSchemaList
                CreateCollectionReq.FieldSchema fieldSchema = MilvusSchemaConverter.convertToFieldType(
                        column,
                        tableSchema.getPrimaryKey());
                fieldSchemaList.add(fieldSchema);
            }
        }

        // Extract struct fields from columns and merge with config
        List<CreateCollectionReq.StructFieldSchema> structFieldsList = getStructFieldsFromColumnsAndConfig(
                catalogTable, gson);

        // Create collection with source schema
        createCollection(tablePath, catalogTable, fieldSchemaList, structFieldsList);
    }

    /**
     * Common logic to create a Milvus collection with the given field schema list
     * and struct fields.
     * Pattern: Use settings from source first, then override with sink config if
     * provided.
     */
    private void createCollection(TablePath tablePath, CatalogTable catalogTable,
            List<CreateCollectionReq.FieldSchema> fieldSchemaList,
            List<CreateCollectionReq.StructFieldSchema> structFields) {
        Map<String, String> options = catalogTable.getOptions();
        Gson gson = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

        // 1. Enable Dynamic Field - source first, then config override
        Boolean enableDynamicField = getEnableDynamicField(options);

        // 2. Consistency Level - source first, then config override
        ConsistencyLevel consistencyLevel = getConsistencyLevel(options);

        // 3. Collection Description - source first, then config override
        String collectionDescription = getCollectionDescription(tablePath, catalogTable);

        // 4. Timezone - source first, then config override
        Map<String, String> properties = new HashMap<>();
        String timezone = getTimezone(options);
        if (timezone != null) {
            properties.put("timezone", timezone);
        }

        // 5. Function List - source first, then merge/add from config
        List<CreateCollectionReq.Function> functionList = getFunctionList(options, gson);

        // 5. Struct Fields - passed from caller (already collected from source/config)
        log.info("Creating collection with {} struct fields", structFields.size());

        // 6. Shard Number - source first, then config override (handled later in
        // createCollectionReq)
        // Build collection schema
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .fieldSchemaList(fieldSchemaList)
                .enableDynamicField(enableDynamicField)
                .functionList(functionList)
                .structFields(structFields)
                .build();
        // Build create collection request
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionName(tablePath.getTableName())
                .description(collectionDescription)
                .collectionSchema(collectionSchema)
                .enableDynamicField(enableDynamicField)
                .consistencyLevel(consistencyLevel)
                .properties(properties)
                .build();

        // Set shard number - source first, then config override
        Integer shardNum = getShardNum(options);
        if (shardNum != null) {
            createCollectionReq.setNumShards(shardNum);
        }
        int retry = 5;
        while (retry > 0) {
            try {
                client.createCollection(createCollectionReq);
                TimeUnit.SECONDS.sleep(5);
                break;
            } catch (Exception e) {
                log.error("create collection failed, retry: {}", retry);
                retry--;
                if (retry == 0) {
                    throw new MilvusConnectorException(MilvusConnectionErrorCode.CREATE_COLLECTION_ERROR,
                            e.getMessage());
                }
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException interruptedException) {
                    log.error("sleep failed", interruptedException);
                }
            }
        }
    }

    private Object convertDefault(Integer dataType, Object defaultValue) {
        if (defaultValue == null || defaultValue.toString().isEmpty()) {
            return null;
        }
        DataType dataTypeEnum = DataType.forNumber(dataType);
        try {
            switch (dataTypeEnum) {
                case Int8:
                case Int16:
                    return Short.valueOf(defaultValue.toString());
                case Int32:
                    return Integer.valueOf(defaultValue.toString());
                case Int64:
                    return Long.valueOf(defaultValue.toString());
                case Bool:
                    return Boolean.valueOf(defaultValue.toString());
                case Float:
                    return Float.valueOf(defaultValue.toString());
                case Double:
                    return Double.valueOf(defaultValue.toString());
                case VarChar:
                case String:
                    return defaultValue.toString();
                case JSON:
                    return defaultValue.toString();
                case Struct:
                    // Struct type - handle as JSON string
                    if (defaultValue instanceof String) {
                        return defaultValue.toString();
                    } else {
                        // Convert object to JSON string
                        Gson gson = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();
                        return gson.toJson(defaultValue);
                    }
                case Geometry:
                    // Geometry type doesn't support default values in Milvus
                    log.warn("GEOMETRY type does not support default values, ignoring default value: {}", defaultValue);
                    return null;
                case Timestamptz:
                    // Timestamptz expects Long (Unix timestamp in microseconds)
                    if (defaultValue instanceof Long) {
                        return defaultValue;
                    } else {
                        try {
                            // Try to parse as timestamp string and convert to microseconds
                            java.sql.Timestamp ts = java.sql.Timestamp.valueOf(defaultValue.toString());
                            return ts.getTime() * 1000;
                        } catch (Exception e) {
                            log.error("Failed to convert TIMESTAMP default value: {}", defaultValue);
                            return null;
                        }
                    }
                default:
                    return defaultValue;
            }
        } catch (Exception e) {
            log.error("convert default value failed, dataType: {}, defaultValue: {}", dataTypeEnum, defaultValue);
            // if the default value is not valid, return null
            return null;
        }
    }

    /**
     * Create a field schema entirely from MilvusFieldSchema config.
     * This is used when field_schema is supplied - all field definitions come from
     * config.
     */
    private CreateCollectionReq.FieldSchema createFieldSchemaFromConfig(MilvusFieldSchema milvusFieldSchema) {
        // Validate that data_type is provided
        if (milvusFieldSchema.getDataType() == null) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.CREATE_COLLECTION_ERROR,
                    "data_type is required in field_schema for field: " + milvusFieldSchema.getEffectiveFieldName());
        }

        CreateCollectionReq.FieldSchema fieldSchema = CreateCollectionReq.FieldSchema.builder()
                .name(milvusFieldSchema.getEffectiveFieldName())
                .dataType(DataType.forNumber(milvusFieldSchema.getDataType()))
                .build();

        // Set element type for Array fields
        if (milvusFieldSchema.getElementType() != null) {
            fieldSchema.setElementType(DataType.forNumber(milvusFieldSchema.getElementType()));
        }

        // Set max capacity for Array fields
        if (milvusFieldSchema.getMaxCapacity() != null && milvusFieldSchema.getMaxCapacity() != 0) {
            fieldSchema.setMaxCapacity(milvusFieldSchema.getMaxCapacity());
        }

        // Set max length for VarChar/String fields
        if (milvusFieldSchema.getMaxLength() != null && milvusFieldSchema.getMaxLength() != 0) {
            fieldSchema.setMaxLength(milvusFieldSchema.getMaxLength());
        }

        // Set dimension for vector fields
        if (milvusFieldSchema.getDimension() != null && milvusFieldSchema.getDimension() != 0) {
            fieldSchema.setDimension(milvusFieldSchema.getDimension());
        }

        // Set nullable (default to true if not specified)
        if (milvusFieldSchema.getIsNullable() != null) {
            fieldSchema.setIsNullable(milvusFieldSchema.getIsNullable());
        }

        // Set default value if specified
        if (milvusFieldSchema.getDefaultValue() != null) {
            Object defaultValue = convertDefault(fieldSchema.getDataType().getCode(),
                    milvusFieldSchema.getDefaultValue());
            fieldSchema.setDefaultValue(defaultValue);
        }

        // Set primary key
        if (milvusFieldSchema.getIsPrimaryKey() != null && milvusFieldSchema.getIsPrimaryKey()) {
            fieldSchema.setIsPrimaryKey(true);
            // Set auto ID if specified
            if (milvusFieldSchema.getAutoId() != null) {
                fieldSchema.setAutoID(milvusFieldSchema.getAutoId());
            }
        }

        // Set partition key
        if (milvusFieldSchema.getIsPartitionKey() != null && milvusFieldSchema.getIsPartitionKey()) {
            fieldSchema.setIsPartitionKey(true);
        }

        // Set analyzer settings
        if (milvusFieldSchema.getEnableAnalyzer() != null && milvusFieldSchema.getEnableAnalyzer()) {
            fieldSchema.setEnableAnalyzer(true);
            if (milvusFieldSchema.getAnalyzerParams() != null) {
                fieldSchema.setAnalyzerParams(milvusFieldSchema.getAnalyzerParams());
            }
        }

        // Set match setting
        if (milvusFieldSchema.getEnableMatch() != null && milvusFieldSchema.getEnableMatch()) {
            fieldSchema.setEnableMatch(true);
        }

        // Note: struct fields are handled at CollectionSchema level, not individual
        // field level
        // See getStructFields() method

        return fieldSchema;
    }

    /**
     * Get enable_dynamic_field setting: source first, then config override
     */
    private Boolean getEnableDynamicField(Map<String, String> options) {
        // Default value
        Boolean enableDynamicField = true;

        // Use source value if available
        if (options.containsKey(MilvusConstants.ENABLE_DYNAMIC_FIELD)) {
            enableDynamicField = Boolean.valueOf(options.get(MilvusConstants.ENABLE_DYNAMIC_FIELD));
            log.debug("Using enable_dynamic_field from source: {}", enableDynamicField);
        }

        // Override with config value if provided
        if (config.get(ENABLE_DYNAMIC_FIELD) != null) {
            enableDynamicField = config.get(ENABLE_DYNAMIC_FIELD);
            log.debug("Overriding enable_dynamic_field with config: {}", enableDynamicField);
        }

        return enableDynamicField;
    }

    /**
     * Get consistency level: source first, then config override
     */
    private ConsistencyLevel getConsistencyLevel(Map<String, String> options) {
        // Default value
        ConsistencyLevel consistencyLevel = ConsistencyLevel.BOUNDED;

        // Use source value if available
        if (options.containsKey(MilvusConstants.CONSISTENCY_LEVEL)) {
            try {
                consistencyLevel = ConsistencyLevel
                        .valueOf(options.get(MilvusConstants.CONSISTENCY_LEVEL).toUpperCase());
                log.debug("Using consistency_level from source: {}", consistencyLevel);
            } catch (IllegalArgumentException e) {
                log.warn("Invalid consistency level from source: {}, using default BOUNDED",
                        options.get(MilvusConstants.CONSISTENCY_LEVEL));
            }
        }

        // Override with config value if provided
        if (config.get(MilvusSinkConfig.CONSISTENCY_LEVEL) != null) {
            consistencyLevel = config.get(MilvusSinkConfig.CONSISTENCY_LEVEL);
            log.debug("Overriding consistency_level with config: {}", consistencyLevel);
        }

        return consistencyLevel;
    }

    /**
     * Get collection description: source first, then config override
     */
    private String getCollectionDescription(TablePath tablePath, CatalogTable catalogTable) {
        // Default value
        String description = "";

        // Use source comment if available
        if (catalogTable.getComment() != null) {
            description = catalogTable.getComment();
            log.debug("Using description from source comment: {}", description);
        }

        // Override with config value if provided
        if (config.get(MilvusSinkConfig.COLLECTION_DESCRIPTION) != null
                && config.get(MilvusSinkConfig.COLLECTION_DESCRIPTION).containsKey(tablePath.getTableName())) {
            description = config.get(MilvusSinkConfig.COLLECTION_DESCRIPTION).get(tablePath.getTableName());
            log.debug("Overriding description with config: {}", description);
        }

        return description;
    }

    /**
     * Get function list: source first, then merge/add from config
     */
    private List<CreateCollectionReq.Function> getFunctionList(Map<String, String> options, Gson gson) {
        List<CreateCollectionReq.Function> functionList = new ArrayList<>();

        // Load functions from source if available
        if (options.containsKey(MilvusConstants.FUNCTION_LIST)) {
            String functionListStr = options.get(MilvusConstants.FUNCTION_LIST);
            if (StringUtils.isNotEmpty(functionListStr) && !functionListStr.equals("[]")) {
                try {
                    Type functionListType = new TypeToken<List<CreateCollectionReq.Function>>() {
                    }.getType();
                    List<CreateCollectionReq.Function> functionsFromSource = gson.fromJson(functionListStr,
                            functionListType);
                    if (functionsFromSource != null) {
                        functionList = functionsFromSource;
                        log.info("Loaded {} functions from source", functionsFromSource.size());
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse functionList from source: {}, error: {}", functionListStr,
                            e.getMessage());
                }
            }
        }

        // Merge/add functions from config (additive, not override)
        if (config.get(MilvusSinkConfig.functionList) != null && !config.get(MilvusSinkConfig.functionList).isEmpty()) {
            try {
                List<Object> functionsFromConfigRaw = config.get(MilvusSinkConfig.functionList);
                Type functionListType = new TypeToken<List<CreateCollectionReq.Function>>() {
                }.getType();
                String functionListStr = gson.toJson(functionsFromConfigRaw);
                List<CreateCollectionReq.Function> functionsFromConfig = gson.fromJson(functionListStr,
                        functionListType);
                if (functionsFromConfig != null) {
                    functionList = functionsFromConfig;
                    log.info("Added {} functions from config, total: {}", functionsFromConfig.size(),
                            functionList.size());
                }
            } catch (Exception e) {
                log.warn("Failed to parse functionList from config, error: {}", e.getMessage());
            }
        }

        return functionList;
    }

    /**
     * Extract struct fields from columns and merge with struct_fields config.
     * This is used for both source schema and field_schema config scenarios.
     */
    private List<CreateCollectionReq.StructFieldSchema> getStructFieldsFromColumnsAndConfig(
            CatalogTable catalogTable, Gson gson) {
        List<CreateCollectionReq.StructFieldSchema> structFieldsList = new ArrayList<>();

        // 1. Extract struct fields from columns (source metadata)
        TableSchema tableSchema = catalogTable.getTableSchema();
        for (Column column : tableSchema.getColumns()) {
            if (column.getOptions() != null && column.getOptions().containsKey(MilvusConstants.STRUCT_FIELDS)) {
                String structFieldsJson = (String) column.getOptions().get(MilvusConstants.STRUCT_FIELDS);
                if (StringUtils.isNotEmpty(structFieldsJson) && !structFieldsJson.equals("[]")) {
                    try {
                        // Parse the nested fields from the struct
                        Type nestedFieldsType = new TypeToken<List<CreateCollectionReq.FieldSchema>>() {
                        }.getType();
                        List<CreateCollectionReq.FieldSchema> nestedFields = gson.fromJson(structFieldsJson,
                                nestedFieldsType);

                        if (nestedFields != null && !nestedFields.isEmpty()) {
                            // Create a StructFieldSchema for this Array[Struct] field
                            CreateCollectionReq.StructFieldSchema structFieldSchema = CreateCollectionReq.StructFieldSchema
                                    .builder()
                                    .name(column.getName())
                                    .description(column.getComment() != null ? column.getComment() : "")
                                    .fields(nestedFields)
                                    .build();

                            // Set maxCapacity if available
                            if (column.getOptions().containsKey(MilvusConstants.MAX_CAPACITY)) {
                                Integer maxCapacity = (Integer) column.getOptions().get(MilvusConstants.MAX_CAPACITY);
                                structFieldSchema.setMaxCapacity(maxCapacity);
                            }

                            structFieldsList.add(structFieldSchema);
                            log.info("Extracted struct field from column: {} with {} nested fields",
                                    column.getName(), nestedFields.size());
                        }
                    } catch (Exception e) {
                        log.warn("Failed to parse struct fields from column {}: {}", column.getName(), e.getMessage());
                    }
                }
            }
        }

        // 2. Filter struct fields based on struct_fields config (if specified)
        List<String> allowedStructFields = config.get(MilvusSinkConfig.structFieldsList);
        if (allowedStructFields != null && !allowedStructFields.isEmpty()) {
            // Filter to only include specified struct field names
            List<CreateCollectionReq.StructFieldSchema> filteredList = new ArrayList<>();
            for (CreateCollectionReq.StructFieldSchema structField : structFieldsList) {
                if (allowedStructFields.contains(structField.getName())) {
                    filteredList.add(structField);
                    log.info("Including struct field from config filter: {}", structField.getName());
                } else {
                    log.info("Excluding struct field not in config filter: {}", structField.getName());
                }
            }
            return filteredList;
        }

        // If struct_fields config is empty, include all struct fields from source
        return structFieldsList;
    }

    /**
     * Get shard number: source first, then config override
     */
    private Integer getShardNum(Map<String, String> options) {
        Integer shardNum = null;

        // Use source value if available
        if (StringUtils.isNotEmpty(options.get(MilvusConstants.SHARDS_NUM))) {
            try {
                shardNum = Integer.parseInt(options.get(MilvusConstants.SHARDS_NUM));
                log.debug("Using shard_num from source: {}", shardNum);
            } catch (NumberFormatException e) {
                log.warn("Invalid shard_num from source: {}", options.get(MilvusConstants.SHARDS_NUM));
            }
        }

        // Override with config value if provided
        if (config.get(MilvusSinkConfig.SHARDS_NUM) != null) {
            shardNum = config.get(MilvusSinkConfig.SHARDS_NUM);
            log.debug("Overriding shard_num with config: {}", shardNum);
        }

        return shardNum;
    }

    private String getTimezone(Map<String, String> options) {
        String timezone = null;

        // Use source value if available
        if (options.containsKey(MilvusConstants.TIMEZONE)) {
            timezone = options.get(MilvusConstants.TIMEZONE);
            log.debug("Using timezone from source: {}", timezone);
        }

        // Override with config value if provided
        if (config.get(MilvusSinkConfig.TIMEZONE) != null) {
            timezone = config.get(MilvusSinkConfig.TIMEZONE);
            log.debug("Overriding timezone with config: {}", timezone);
        }

        return timezone;
    }
}
