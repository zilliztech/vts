package org.apache.seatunnel.connectors.seatunnel.milvus.sink.catalog;

import com.google.gson.annotations.SerializedName;
import lombok.Data;

@Data
public class MilvusField {
    @SerializedName("field_name")
    private String fieldName;
    @SerializedName("new_field_name")
    private String newFieldName;
    @SerializedName("data_type")
    private String dataType;
    @SerializedName("element_type")
    private String elementType;
    @SerializedName("max_length")
    private Integer maxLength;
    @SerializedName("is_nullable")
    private Boolean isNullable;
    @SerializedName("default_value")
    private Object defaultValue;
}
