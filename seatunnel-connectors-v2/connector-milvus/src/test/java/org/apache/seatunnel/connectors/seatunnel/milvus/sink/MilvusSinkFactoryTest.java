package org.apache.seatunnel.connectors.seatunnel.milvus.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class MilvusSinkFactoryTest {
    private final MilvusSinkFactory milvusSinkFactory = new MilvusSinkFactory();

    @Test
    void factoryIdentifier() {
        Assertions.assertEquals(
                milvusSinkFactory.factoryIdentifier(),
                MilvusSinkConfig.CONNECTOR_IDENTITY.toString());
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull(milvusSinkFactory.optionRule());
    }

    @Test
    void createIndexConfigDefaultsToTrue() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(new HashMap<>());

        Assertions.assertTrue(config.get(MilvusSinkConfig.CREATE_INDEX));
    }

    @Test
    void createIndexConfigCanBeDisabled() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("create_index", false);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        Assertions.assertFalse(config.get(MilvusSinkConfig.CREATE_INDEX));
    }
}
