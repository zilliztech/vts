/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ElasticSearchTypeConverterTest {

    @Test
    public void testConvertDatePreservesFormat() {
        Map<String, Object> options = new HashMap<>();
        options.put("format", "yyyy/MM/dd||epoch_millis");
        EsType nativeType = new EsType("date", options);

        BasicTypeDefine<EsType> typeDefine =
                BasicTypeDefine.<EsType>builder()
                        .name("created_at")
                        .columnType("date")
                        .dataType("date")
                        .nativeType(nativeType)
                        .build();

        Column column = ElasticSearchTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals("created_at", column.getName());
        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(3, column.getScale());
        Assertions.assertNotNull(column.getOptions());
        Assertions.assertEquals("yyyy/MM/dd||epoch_millis", column.getOptions().get("format"));
    }

    @Test
    public void testConvertDateNanosPreservesFormat() {
        Map<String, Object> options = new HashMap<>();
        options.put("format", "strict_date_optional_time_nanos||epoch_millis");
        EsType nativeType = new EsType("date_nanos", options);

        BasicTypeDefine<EsType> typeDefine =
                BasicTypeDefine.<EsType>builder()
                        .name("precise_ts")
                        .columnType("date_nanos")
                        .dataType("date_nanos")
                        .nativeType(nativeType)
                        .build();

        Column column = ElasticSearchTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(9, column.getScale());
        Assertions.assertNotNull(column.getOptions());
        Assertions.assertEquals(
                "strict_date_optional_time_nanos||epoch_millis",
                column.getOptions().get("format"));
    }

    @Test
    public void testConvertDateWithoutFormatHasNullOptions() {
        // When nativeType has no format, options should be null (not set)
        Map<String, Object> options = new HashMap<>();
        EsType nativeType = new EsType("date", options);

        BasicTypeDefine<EsType> typeDefine =
                BasicTypeDefine.<EsType>builder()
                        .name("ts")
                        .columnType("date")
                        .dataType("date")
                        .nativeType(nativeType)
                        .build();

        Column column = ElasticSearchTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(3, column.getScale());
        // No format in nativeType.options → column.options should be null
        Assertions.assertTrue(
                column.getOptions() == null || !column.getOptions().containsKey("format"));
    }

    @Test
    public void testConvertDateWithNullNativeType() {
        BasicTypeDefine<EsType> typeDefine =
                BasicTypeDefine.<EsType>builder()
                        .name("ts")
                        .columnType("date")
                        .dataType("date")
                        .build();

        Column column = ElasticSearchTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertEquals(3, column.getScale());
    }

    @Test
    public void testConvertNonDateTypeNotAffected() {
        Map<String, Object> options = new HashMap<>();
        EsType nativeType = new EsType("keyword", options);

        BasicTypeDefine<EsType> typeDefine =
                BasicTypeDefine.<EsType>builder()
                        .name("status")
                        .columnType("keyword")
                        .dataType("keyword")
                        .nativeType(nativeType)
                        .build();

        Column column = ElasticSearchTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals("status", column.getName());
        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        // Non-date types should not have format in options
        Assertions.assertTrue(
                column.getOptions() == null || !column.getOptions().containsKey("format"));
    }

    @Test
    public void testConvertDateCustomFormatEpochSecond() {
        Map<String, Object> options = new HashMap<>();
        options.put("format", "epoch_second");
        EsType nativeType = new EsType("date", options);

        BasicTypeDefine<EsType> typeDefine =
                BasicTypeDefine.<EsType>builder()
                        .name("epoch_ts")
                        .columnType("date")
                        .dataType("date")
                        .nativeType(nativeType)
                        .build();

        Column column = ElasticSearchTypeConverter.INSTANCE.convert(typeDefine);

        Assertions.assertEquals(LocalTimeType.LOCAL_DATE_TIME_TYPE, column.getDataType());
        Assertions.assertNotNull(column.getOptions());
        Assertions.assertEquals("epoch_second", column.getOptions().get("format"));
    }

    @Test
    public void testReconvertDateUsesScale() {
        // Verify that reconvert uses scale to distinguish DATE vs DATE_NANOS
        Column dateColumn = ElasticSearchTypeConverter.INSTANCE.convert(
                BasicTypeDefine.<EsType>builder()
                        .name("d")
                        .columnType("date")
                        .dataType("date")
                        .nativeType(new EsType("date", new HashMap<>()))
                        .build());
        BasicTypeDefine<EsType> reconverted =
                ElasticSearchTypeConverter.INSTANCE.reconvert(dateColumn);
        Assertions.assertEquals("date", reconverted.getDataType());

        Column dateNanosColumn = ElasticSearchTypeConverter.INSTANCE.convert(
                BasicTypeDefine.<EsType>builder()
                        .name("dn")
                        .columnType("date_nanos")
                        .dataType("date_nanos")
                        .nativeType(new EsType("date_nanos", new HashMap<>()))
                        .build());
        BasicTypeDefine<EsType> reconvertedNanos =
                ElasticSearchTypeConverter.INSTANCE.reconvert(dateNanosColumn);
        Assertions.assertEquals("date_nanos", reconvertedNanos.getDataType());
    }
}
