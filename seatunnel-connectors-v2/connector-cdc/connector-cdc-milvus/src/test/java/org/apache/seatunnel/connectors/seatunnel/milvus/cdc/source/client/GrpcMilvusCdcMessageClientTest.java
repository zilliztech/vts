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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.milvus.shaded.io.grpc.ManagedChannel;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.utils.ClientUtils;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GrpcMilvusCdcMessageClientTest {

    @Test
    void initializationFailureClosesChannel() {
        ConnectConfig connectConfig = mock(ConnectConfig.class);
        ClientUtils clientUtils = mock(ClientUtils.class);
        ManagedChannel channel = mock(ManagedChannel.class);
        when(clientUtils.getChannel(connectConfig)).thenReturn(channel);
        when(clientUtils.getSDKVersion()).thenReturn("test-sdk");
        when(clientUtils.getHostName()).thenReturn("test-host");
        when(clientUtils.getLocalTimeStr()).thenReturn("test-time");
        when(connectConfig.getOption()).thenReturn(Collections.emptyMap());
        when(channel.newCall(any(), any())).thenThrow(new IllegalStateException("connect failed"));

        IllegalStateException failure =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () -> new GrpcMilvusCdcMessageClient(connectConfig, clientUtils));

        Assertions.assertEquals("connect failed", failure.getMessage());
        verify(channel).shutdownNow();
    }
}
