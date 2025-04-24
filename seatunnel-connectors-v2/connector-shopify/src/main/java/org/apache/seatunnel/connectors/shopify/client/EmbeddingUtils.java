package org.apache.seatunnel.connectors.shopify.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EmbeddingUtils {
    private static final String OPENAI_API_URL = "https://api.openai.com/v1/embeddings";
    private final String apiKey;
    private final OkHttpClient client;
    private final ObjectMapper objectMapper;

    public EmbeddingUtils(String apiKey) {
        this.apiKey = apiKey;
        this.client = new OkHttpClient();
        this.objectMapper = new ObjectMapper();
    }

    public List<Float> getEmbedding(String productTitle) throws IOException {
        // Create request body
        ObjectNode requestBody = objectMapper.createObjectNode();
        requestBody.put("input", productTitle);
        requestBody.put("model", "text-embedding-3-small");  // or "text-embedding-ada-002" for older model

        // Build request
        Request request = new Request.Builder()
                .url(OPENAI_API_URL)
                .addHeader("Authorization", "Bearer " + apiKey)
                .addHeader("Content-Type", "application/json")
                .post(RequestBody.create(
                        requestBody.toString(),
                        MediaType.parse("application/json")
                ))
                .build();

        // Execute request
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected response " + response);
            }

            // Parse response
            JsonNode jsonResponse = objectMapper.readTree(response.body().string());
            JsonNode embeddingData = jsonResponse
                    .path("data")
                    .get(0)
                    .path("embedding");

            // Convert to List<Double>
            List<Float> embeddings = new ArrayList<>();
            embeddingData.forEach(node -> embeddings.add((float)node.asDouble()));

            return embeddings;
        }
    }

    // Optional: Method to calculate cosine similarity between two embeddings
    public double cosineSimilarity(List<Double> embedding1, List<Double> embedding2) {
        if (embedding1.size() != embedding2.size()) {
            throw new IllegalArgumentException("Embeddings must be of same length");
        }

        double dotProduct = 0.0;
        double norm1 = 0.0;
        double norm2 = 0.0;

        for (int i = 0; i < embedding1.size(); i++) {
            dotProduct += embedding1.get(i) * embedding2.get(i);
            norm1 += embedding1.get(i) * embedding1.get(i);
            norm2 += embedding2.get(i) * embedding2.get(i);
        }

        return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
    }
}