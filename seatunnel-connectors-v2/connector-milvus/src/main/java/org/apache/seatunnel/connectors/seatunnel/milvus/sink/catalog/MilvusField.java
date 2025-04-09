package org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog;

import com.google.gson.annotations.SerializedName;
import lombok.Data;

@Data
public class MilvusField {
    @SerializedName("source_field_name")
    private String sourceFieldName;
    @SerializedName("target_field_name")
    private String targetFieldName;
    @SerializedName("data_type")
    private Integer dataType;
    @SerializedName("element_type")
    private Integer elementType;
    @SerializedName("max_length")
    private Integer maxLength;
    @SerializedName("is_nullable")
    private Boolean isNullable;
    @SerializedName("default_value")
    private Object defaultValue;
}
