package org.apache.seatunnel.connectors.weaviate.config;

import io.weaviate.client.Config;
import io.weaviate.client.WeaviateAuthClient;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.v1.auth.exception.AuthException;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.weaviate.exception.WeaviateConnectorErrorCode;
import org.apache.seatunnel.connectors.weaviate.exception.WeaviateConnectoreException;

import java.io.Serializable;

@Data
public class WeaviateParameters implements Serializable {
    private String scheme;
    private String host;
    private String apiKey;

    private String className;

    public WeaviateParameters(ReadonlyConfig config) {
        this.scheme = config.get(WeaviateConfig.SCHEME);
        this.host = config.get(WeaviateConfig.HOST);
        this.apiKey = config.get(WeaviateConfig.API_KEY);
        this.className = config.get(WeaviateConfig.CLASS_NAME);
    }

    public WeaviateClient buildWeaviateClient() {
        Config config = new Config(scheme, host);
        if (StringUtils.isNotEmpty(apiKey)) {
            try {
                return WeaviateAuthClient.apiKey(config, apiKey);
            } catch (AuthException e) {
                throw new WeaviateConnectoreException(WeaviateConnectorErrorCode.AUTHENTICATION_ERROR,
                        "Failed to authenticate with Weaviate using API key: " + e.getMessage());
            }
        } else {
            return new WeaviateClient(config);
        }
    }
}
