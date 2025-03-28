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

package org.apache.seatunnel.connectors.shopify.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.shopify.client.Product;
import org.apache.seatunnel.connectors.shopify.client.ShopifyGraphQLClient;
import org.apache.seatunnel.connectors.shopify.client.resp.ListProductResp;
import static org.apache.seatunnel.connectors.shopify.config.ShopifyConfig.ACCESS_TOKEN;
import static org.apache.seatunnel.connectors.shopify.config.ShopifyConfig.API_KEY_OPENAI;
import static org.apache.seatunnel.connectors.shopify.config.ShopifyConfig.SHOP_URL;
import org.apache.seatunnel.connectors.shopify.exception.ShopifyConnectionErrorCode;
import org.apache.seatunnel.connectors.shopify.exception.ShopifyConnectorException;
import org.apache.seatunnel.connectors.shopify.utils.ShopifySourceConverter;

import java.io.IOException;
import java.util.List;


public class ShopifySourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final SingleSplitReaderContext context;
    private final TableSchema tableSchema;
    private final TablePath tablePath;
    private final ReadonlyConfig config;
    private ShopifyGraphQLClient client;
    private ShopifySourceConverter converter;

    public ShopifySourceReader(
            SingleSplitReaderContext context,
            ReadonlyConfig readonlyConfig,
            CatalogTable catalogTable) {
        this.config = readonlyConfig;
        this.context = context;
        this.tableSchema = catalogTable.getTableSchema();
        this.tablePath = catalogTable.getTablePath();
    }

    @Override
    public void open() throws Exception {
        String shopUrl = config.get(SHOP_URL);
        String accessToken = config.get(ACCESS_TOKEN);
        this.converter = new ShopifySourceConverter(config.get(API_KEY_OPENAI));
        this.client = new ShopifyGraphQLClient(shopUrl, accessToken);
    }

    @Override
    public void close() {
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        String afterCursor = null;
        while(true) {
            try {
                ListProductResp listProductResp = client.listProducts(afterCursor, 1);
                afterCursor = listProductResp.getAfterCursor();
                for (Product product : listProductResp.getProductList()) {
                    SeaTunnelRow row = converter.convertToSeatunnelRow(tableSchema, product);
                    output.collect(row);
                }
                if(afterCursor == null) {
                    break;
                }
            } catch (IOException e) {
                throw new ShopifyConnectorException(ShopifyConnectionErrorCode.READ_DATA_FAIL, e);
            }
        }
        if (Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            context.signalNoMoreElement();
        }
    }


}
