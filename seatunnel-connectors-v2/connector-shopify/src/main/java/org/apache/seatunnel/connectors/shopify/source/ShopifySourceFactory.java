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

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.shopify.config.ShopifyConfig;

import java.io.Serializable;

@AutoService(Factory.class)
public class ShopifySourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return ShopifyConfig.CONNECTOR_IDENTITY;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new ShopifySource(context.getOptions());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required()
                .optional(
                        ShopifyConfig.SHOP_URL,
                        ShopifyConfig.ACCESS_TOKEN)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return ShopifySource.class;
    }
}
