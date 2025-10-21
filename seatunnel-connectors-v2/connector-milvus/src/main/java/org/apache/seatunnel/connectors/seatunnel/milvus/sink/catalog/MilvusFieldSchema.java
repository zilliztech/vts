package org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog;

import com.google.gson.annotations.SerializedName;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.Map;

/**
 * Unified field schema configuration for Milvus fields.
 * Combines functionality of extracting fields from dynamic schema and configuring field properties.
 */
@Data
@SuperBuilder
public class MilvusFieldSchema {
    // Field identification
    @SerializedName("field_name")
    private String fieldName;

    // For extracting from dynamic field
    @SerializedName("source_field_name")
    private String sourceFieldName;

    @SerializedName("data_type")
    private Integer dataType;

    @SerializedName("element_type")
    private Integer elementType;

    @SerializedName("max_capacity")
    @Builder.Default
    private Integer maxCapacity = 4096;

    @SerializedName("max_length")
    private Integer maxLength;

    @SerializedName("dimension")
    private Integer dimension;

    // Field properties
    @SerializedName("is_nullable")
    private Boolean isNullable;

    @SerializedName("default_value")
    private Object defaultValue;

    @SerializedName("is_primary_key")
    private Boolean isPrimaryKey;

    @SerializedName("auto_id")
    private Boolean autoId;

    @SerializedName("is_partition_key")
    private Boolean isPartitionKey;

    @SerializedName("enable_analyzer")
    private Boolean enableAnalyzer;

    @SerializedName("analyzer_params")
    private Map<String, Object> analyzerParams;

    @SerializedName("enable_match")
    private Boolean enableMatch;

    /**
     * Check if this is a dynamic field extraction (has source_field_name or data_type specified)
     */
    public boolean isDynamicExtraction() {
        return sourceFieldName != null || dataType != null;
    }

    /**
     * Get the effective field name (field_name if specified, otherwise source_field_name)
     */
    public String getEffectiveFieldName() {
        if (fieldName != null) {
            return fieldName;
        }
        return sourceFieldName;
    }
}
