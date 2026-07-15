package org.apache.seatunnel.connectors.seatunnel.milvus.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.config.MilvusSinkWriteMode;
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

    @Test
    void writeModeDefaultsToAppend() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(new HashMap<>());

        Assertions.assertEquals(MilvusSinkWriteMode.APPEND, config.get(MilvusSinkConfig.WRITE_MODE));
    }

    @Test
    void writeModeCanUseLowerCaseCdc() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("write_mode", "cdc");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        Assertions.assertEquals(MilvusSinkWriteMode.CDC, config.get(MilvusSinkConfig.WRITE_MODE));
    }

    @Test
    void cdcWriteModeRejectsDefaultSchemaSaveMode() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("write_mode", "cdc");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> MilvusSinkFactory.validateCdcSaveMode(config));
        Assertions.assertTrue(exception.getMessage().contains("ERROR_WHEN_SCHEMA_NOT_EXIST"));
    }

    @Test
    void cdcWriteModeAllowsExplicitNonDestructiveSaveMode() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("write_mode", "cdc");
        configMap.put("schema_save_mode", SchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST);
        configMap.put("data_save_mode", DataSaveMode.APPEND_DATA);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        Assertions.assertDoesNotThrow(() -> MilvusSinkFactory.validateCdcSaveMode(config));
    }

    @Test
    void cdcWriteModeRejectsRecreateSchemaSaveMode() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("write_mode", "cdc");
        configMap.put("schema_save_mode", SchemaSaveMode.RECREATE_SCHEMA);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> MilvusSinkFactory.validateCdcSaveMode(config));
        Assertions.assertTrue(exception.getMessage().contains("ERROR_WHEN_SCHEMA_NOT_EXIST"));
    }

    @Test
    void cdcWriteModeRejectsDropDataSaveMode() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("write_mode", "cdc");
        configMap.put("schema_save_mode", SchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST);
        configMap.put("data_save_mode", DataSaveMode.DROP_DATA);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> MilvusSinkFactory.validateCdcSaveMode(config));
        Assertions.assertTrue(exception.getMessage().contains("APPEND_DATA"));
    }

    @Test
    void appendWriteModeDoesNotUseCdcSaveModeRestrictions() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("write_mode", "append");
        configMap.put("schema_save_mode", SchemaSaveMode.RECREATE_SCHEMA);
        configMap.put("data_save_mode", DataSaveMode.DROP_DATA);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        Assertions.assertDoesNotThrow(() -> MilvusSinkFactory.validateCdcSaveMode(config));
    }
}
