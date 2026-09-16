/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * Licensed under the Apache License, Version 2.0
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.seatunnel.connectors.seatunnel.file.writer;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.ParquetWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ParquetReadStrategy;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

/**
 * Vector columns are written in generic, engine-neutral encodings: dense float32 vectors
 * as list&lt;f32&gt;, other dense vectors as raw binary, sparse vectors as a struct of
 * indices/values lists. These tests pin both the schema shape and the value round-trip.
 */
public class ParquetWriteStrategyVectorTest {
    private static final String TMP_PATH = "file:///tmp/seatunnel/parquet/vector/test";

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testVectorColumnsRoundTrip() throws Exception {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", TMP_PATH);
        writeConfig.put("path", "file:///tmp/seatunnel/parquet/vector");
        writeConfig.put("file_format_type", FileFormat.PARQUET.name());

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "float_vec", "binary_vec", "sparse_vec"},
                        new SeaTunnelDataType[] {
                            BasicType.LONG_TYPE,
                            VectorType.VECTOR_FLOAT_TYPE,
                            VectorType.VECTOR_BINARY_TYPE,
                            VectorType.VECTOR_SPARSE_FLOAT_TYPE
                        });
        FileSinkConfig sinkConfig =
                new FileSinkConfig(ConfigFactory.parseMap(writeConfig), rowType);
        ParquetWriteStrategy writeStrategy = new ParquetWriteStrategy(sinkConfig);
        ParquetReadStrategyTest.LocalConf hadoopConf =
                new ParquetReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", rowType));
        writeStrategy.init(hadoopConf, "vector-test", "vector-test", 0);
        writeStrategy.beginTransaction(1L);

        Float[] floatVector = new Float[] {1.5f, -2.25f, 3.0f, 0.125f};
        byte[] binaryVector = new byte[] {0x0A, 0x1B, 0x00, (byte) 0xFF};
        Map<Long, Float> sparseVector = new TreeMap<>();
        sparseVector.put(3L, 0.5f);
        sparseVector.put(17L, -1.25f);

        writeStrategy.write(
                new SeaTunnelRow(
                        new Object[] {
                            42L,
                            BufferUtils.toByteBuffer(floatVector),
                            ByteBuffer.wrap(binaryVector),
                            sparseVector
                        }));
        writeStrategy.finishAndCloseFile();
        writeStrategy.close();

        ParquetReadStrategy readStrategy = new ParquetReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles = readStrategy.getFileNamesByPath(TMP_PATH);
        Assertions.assertEquals(1, readFiles.size());
        String filePath = readFiles.get(0).replace("file://", "");

        try (ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(
                                HadoopInputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(filePath),
                                        new Configuration()))
                        .build()) {
            GenericRecord record = reader.read();
            Assertions.assertNotNull(record);
            Assertions.assertEquals(42L, record.get("id"));

            // dense float vector -> list<f32>
            Object floatVec = record.get("float_vec");
            Assertions.assertTrue(floatVec instanceof List);
            @SuppressWarnings("unchecked")
            List<Object> floats = (List<Object>) floatVec;
            Assertions.assertEquals(4, floats.size());
            Assertions.assertEquals(1.5f, (Float) floats.get(0), 1e-6);
            Assertions.assertEquals(-2.25f, (Float) floats.get(1), 1e-6);

            // binary vector -> raw bytes, lossless
            Object binaryVec = record.get("binary_vec");
            Assertions.assertTrue(binaryVec instanceof ByteBuffer);
            ByteBuffer binaryOut = ((ByteBuffer) binaryVec).duplicate();
            byte[] binaryBytes = new byte[binaryOut.remaining()];
            binaryOut.get(binaryBytes);
            Assertions.assertArrayEquals(binaryVector, binaryBytes);

            // sparse vector -> struct{indices, values}, sorted by index
            Object sparseVec = record.get("sparse_vec");
            Assertions.assertTrue(sparseVec instanceof GenericRecord);
            GenericRecord sparseRecord = (GenericRecord) sparseVec;
            @SuppressWarnings("unchecked")
            List<Object> indices = (List<Object>) sparseRecord.get("indices");
            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) sparseRecord.get("values");
            Assertions.assertEquals(2, indices.size());
            Assertions.assertEquals(3, indices.get(0));
            Assertions.assertEquals(17, indices.get(1));
            Assertions.assertEquals(0.5f, (Float) values.get(0), 1e-6);
            Assertions.assertEquals(-1.25f, (Float) values.get(1), 1e-6);

            Assertions.assertNull(reader.read());
        }
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testNullVectorColumns() throws Exception {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", "file:///tmp/seatunnel/parquet/vectornull/test");
        writeConfig.put("path", "file:///tmp/seatunnel/parquet/vectornull");
        writeConfig.put("file_format_type", FileFormat.PARQUET.name());

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "float_vec", "sparse_vec"},
                        new SeaTunnelDataType[] {
                            BasicType.LONG_TYPE,
                            VectorType.VECTOR_FLOAT_TYPE,
                            VectorType.VECTOR_SPARSE_FLOAT_TYPE
                        });
        FileSinkConfig sinkConfig =
                new FileSinkConfig(ConfigFactory.parseMap(writeConfig), rowType);
        ParquetWriteStrategy writeStrategy = new ParquetWriteStrategy(sinkConfig);
        ParquetReadStrategyTest.LocalConf hadoopConf =
                new ParquetReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", rowType));
        writeStrategy.init(hadoopConf, "vector-null-test", "vector-null-test", 0);
        writeStrategy.beginTransaction(1L);
        writeStrategy.write(new SeaTunnelRow(new Object[] {7L, null, null}));
        writeStrategy.finishAndCloseFile();
        writeStrategy.close();

        ParquetReadStrategy readStrategy = new ParquetReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles =
                readStrategy.getFileNamesByPath("file:///tmp/seatunnel/parquet/vectornull/test");
        Assertions.assertEquals(1, readFiles.size());
        try (ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(
                                HadoopInputFile.fromPath(
                                        new org.apache.hadoop.fs.Path(
                                                readFiles.get(0).replace("file://", "")),
                                        new Configuration()))
                        .build()) {
            GenericRecord record = reader.read();
            Assertions.assertEquals(7L, record.get("id"));
            Assertions.assertNull(record.get("float_vec"));
            Assertions.assertNull(record.get("sparse_vec"));
        }
    }
}
