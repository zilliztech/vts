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

import io.milvus.common.interceptor.IdentifierInterceptor;
import io.milvus.grpc.ClientInfo;
import io.milvus.grpc.ConnectRequest;
import io.milvus.grpc.ConnectResponse;
import io.milvus.grpc.DumpMessagesRequest;
import io.milvus.grpc.DumpMessagesResponse;
import io.milvus.grpc.ErrorCode;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.shaded.io.grpc.Channel;
import io.milvus.shaded.io.grpc.ClientInterceptors;
import io.milvus.shaded.io.grpc.ManagedChannel;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.utils.ClientUtils;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class GrpcMilvusCdcMessageClient implements MilvusCdcMessageClient {

    private final ConnectConfig connectConfig;
    private final ClientUtils clientUtils;
    private final ManagedChannel channel;
    private final MilvusServiceGrpc.MilvusServiceBlockingStub stub;

    public GrpcMilvusCdcMessageClient(ConnectConfig connectConfig) {
        this(connectConfig, new ClientUtils());
    }

    GrpcMilvusCdcMessageClient(ConnectConfig connectConfig, ClientUtils clientUtils) {
        this.connectConfig = connectConfig;
        this.clientUtils = clientUtils;
        this.channel = clientUtils.getChannel(connectConfig);
        boolean initialized = false;
        try {
            MilvusServiceGrpc.MilvusServiceBlockingStub rawStub =
                    MilvusServiceGrpc.newBlockingStub(channel).withWaitForReady();
            long identifier = connect(rawStub);
            Channel interceptedChannel =
                    ClientInterceptors.intercept(channel, new IdentifierInterceptor(identifier));
            this.stub = MilvusServiceGrpc.newBlockingStub(interceptedChannel).withWaitForReady();
            initialized = true;
        } finally {
            if (!initialized) {
                channel.shutdownNow();
            }
        }
    }

    @Override
    public Iterator<DumpMessagesResponse> dumpMessages(DumpMessagesRequest request) {
        if (connectConfig.getRpcDeadlineMs() > 0) {
            return stub.withDeadlineAfter(connectConfig.getRpcDeadlineMs(), TimeUnit.MILLISECONDS)
                    .dumpMessages(request);
        }
        return stub.dumpMessages(request);
    }

    @Override
    public void close() {
        channel.shutdownNow();
    }

    private long connect(MilvusServiceGrpc.MilvusServiceBlockingStub rawStub) {
        String userName = connectConfig.getUsername() == null ? "" : connectConfig.getUsername();
        ClientInfo.Builder infoBuilder =
                ClientInfo.newBuilder()
                        .setSdkType("Java")
                        .setSdkVersion(clientUtils.getSDKVersion())
                        .setUser(userName)
                        .setHost(clientUtils.getHostName())
                        .setLocalTime(clientUtils.getLocalTimeStr());
        if (connectConfig.getOption() != null && !connectConfig.getOption().isEmpty()) {
            infoBuilder.putAllReserved(connectConfig.getOption());
        }
        ConnectRequest request =
                ConnectRequest.newBuilder().setClientInfo(infoBuilder.build()).build();
        ConnectResponse response =
                rawStub.withDeadlineAfter(
                                connectConfig.getConnectTimeoutMs(), TimeUnit.MILLISECONDS)
                        .connect(request);
        if (response.getStatus().getCode() != 0
                || response.getStatus().getErrorCode() != ErrorCode.Success) {
            throw new IllegalStateException(
                    "Failed to initialize Milvus CDC connection: "
                            + response.getStatus().getReason());
        }
        return response.getIdentifier();
    }
}
