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

package org.apache.seatunnel.connectors.shopify.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.CommonOptions;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.shopify.client.EmbeddingUtils;
import org.apache.seatunnel.connectors.shopify.client.Product;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ShopifySourceConverter {
    private static EmbeddingUtils embeddingUtils;
    public <T> ShopifySourceConverter(String apiKey) {
        embeddingUtils = new EmbeddingUtils(apiKey);
    }

    public SeaTunnelRow convertToSeatunnelRow(TableSchema tableSchema, Product product) {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        Object[] fields = new Object[typeInfo.getTotalFields()];
        List<String> fieldNames = Arrays.stream(typeInfo.getFieldNames()).collect(Collectors.toList());
        fields[fieldNames.indexOf("id")] = product.getId();
        fields[fieldNames.indexOf("title")] = product.getTitle();
        try {
            List<Float> embedding = embeddingUtils.getEmbedding(product.getTitle());
            Float[] floatArray = embedding.toArray(new Float[0]);
            fields[fieldNames.indexOf("title_vector")] = BufferUtils.toByteBuffer(floatArray);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            fields[fieldNames.indexOf(CommonOptions.METADATA.getName())] = product.toJson();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
        seaTunnelRow.setRowKind(RowKind.INSERT);
        return seaTunnelRow;
    }
}
