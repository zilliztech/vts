package org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog;

import com.google.gson.annotations.SerializedName;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
public class MilvusField {
    @SerializedName("source_field_name")
    private String sourceFieldName;
    @SerializedName("target_field_name")
    private String targetFieldName;
    @SerializedName("data_type")
    private Integer dataType;
    @SerializedName("element_type")
    private Integer elementType;
    @SerializedName("max_capacity")
    @Builder.Default
    private Integer maxCapacity = 4096;
    @SerializedName("max_length")
    private Integer maxLength;
    @SerializedName("is_nullable")
    private Boolean isNullable;
    @SerializedName("default_value")
    private Object defaultValue;
}
