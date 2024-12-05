package org.apache.seatunnel.connectors.shopify.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Data;

import java.time.OffsetDateTime;
import java.util.List;

@Data
public class Product {
    private String id;
    private String title;
    private String handle;
    private String description;
    private String productType;
    private String vendor;
    private String status;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;
    private OffsetDateTime publishedAt;
    private List<String> tags;
    private PriceRange priceRange;
    private List<Variant> variants;
    private List<Image> images;

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    public String toJson() throws JsonProcessingException {
        return objectMapper.writeValueAsString(this);
    }
}
