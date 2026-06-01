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

package org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.cdc.source.MilvusCdcSourceConfigParser;

import io.milvus.v2.client.ConnectConfig;

import java.io.Serializable;

public interface MilvusCdcClientFactory extends Serializable {

    MilvusCdcMessageClient create() throws Exception;

    static MilvusCdcClientFactory fromConfig(ReadonlyConfig config) {
        ConnectConfig connectConfig = MilvusCdcSourceConfigParser.parseConnectConfig(config);
        return new DefaultMilvusCdcClientFactory(connectConfig);
    }

    class DefaultMilvusCdcClientFactory implements MilvusCdcClientFactory {
        private static final long serialVersionUID = 1L;

        private final ConnectConfig connectConfig;

        DefaultMilvusCdcClientFactory(ConnectConfig connectConfig) {
            this.connectConfig = connectConfig;
        }

        @Override
        public MilvusCdcMessageClient create() {
            return new GrpcMilvusCdcMessageClient(connectConfig);
        }
    }
}
