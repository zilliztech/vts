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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.milvus.grpc.CollectionSchema;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.grpc.IDs;
import io.milvus.grpc.KeyValuePair;
import io.milvus.grpc.LongArray;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.grpc.MutationResult;
import io.milvus.shaded.io.grpc.CallOptions;
import io.milvus.shaded.io.grpc.Channel;
import io.milvus.shaded.io.grpc.ClientCall;
import io.milvus.shaded.io.grpc.Metadata;
import io.milvus.shaded.io.grpc.MethodDescriptor;
import io.milvus.shaded.io.grpc.Status;
import io.milvus.shaded.io.grpc.StatusRuntimeException;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.client.RetryConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class MilvusCdcWriterSdkRetryTest {
    private enum Failure {
        DELETE,
        INSERT_BEFORE_APPLY,
        INSERT_RESPONSE_LOST
    }

    @ParameterizedTest
    @EnumSource(Failure.class)
    void autoIdUpsertRetriesThroughRealSdkRequestConversion(Failure failure) throws Exception {
        ScriptedChannel channel = new ScriptedChannel(failure);
        MilvusClientV2 client = new MilvusClientV2(null);
        client.setBlockingStub(MilvusServiceGrpc.newBlockingStub(channel));
        MilvusCdcWriter writer =
                new MilvusCdcWriter(
                        MilvusCdcWriterTest.catalogTable(),
                        ReadonlyConfig.fromMap(Collections.singletonMap("enable_upsert", true)),
                        client,
                        MilvusCdcWriterTest.autoIdDescription(),
                        () -> 0L);
        RetryConfig policy = MilvusCdcWriterTest.retryConfig(writer);
        policy.setMaxRetryTimes(3);
        policy.setInitialBackOffMs(0L);
        policy.setMaxBackOffMs(0L);

        writer.write(new SeaTunnelRow(new Object[] {42L, "updated"}));
        writer.commit(false);

        assertEquals(Collections.singletonList(42L), channel.storedIds);
        assertFalse(writer.needCommit());
        assertEquals(
                failure == Failure.DELETE
                        ? Arrays.asList("delete", "delete", "insert")
                        : Arrays.asList("delete", "insert", "delete", "insert"),
                channel.operations);
    }

    // Mock only the RPC transport: VectorService must run its real request validation/conversion,
    // including the SDK's mutation of DeleteReq when converting primary keys to a filter.
    private static class ScriptedChannel extends Channel {
        private final Failure failure;
        private final List<Long> storedIds = new ArrayList<>(Arrays.asList(42L, 42L));
        private final List<String> operations = new ArrayList<>();
        private int deletes;
        private int inserts;

        private ScriptedChannel(Failure failure) {
            this.failure = failure;
        }

        @Override
        public String authority() {
            return "milvus-cdc-sdk-retry-test";
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
            return new ClientCall<ReqT, RespT>() {
                private Listener<RespT> listener;

                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    listener = responseListener;
                }

                @Override
                public void request(int numMessages) {}

                @Override
                public void cancel(String message, Throwable cause) {}

                @Override
                public void sendMessage(ReqT request) {}

                @Override
                @SuppressWarnings("unchecked")
                public void halfClose() {
                    try {
                        RespT response = (RespT) respond(method.getBareMethodName());
                        listener.onHeaders(new Metadata());
                        listener.onMessage(response);
                        listener.onClose(Status.OK, new Metadata());
                    } catch (StatusRuntimeException error) {
                        listener.onClose(error.getStatus(), new Metadata());
                    }
                }
            };
        }

        private Object respond(String method) {
            switch (method) {
                case "DescribeCollection":
                    return describeCollection();
                case "Delete":
                    operations.add("delete");
                    if (++deletes == 1 && failure == Failure.DELETE) {
                        throw Status.UNAVAILABLE.asRuntimeException();
                    }
                    storedIds.clear();
                    return MutationResult.newBuilder().setDeleteCnt(1).build();
                case "Insert":
                    operations.add("insert");
                    if (++inserts == 1 && failure == Failure.INSERT_BEFORE_APPLY) {
                        throw Status.UNAVAILABLE.asRuntimeException();
                    }
                    storedIds.add(42L);
                    if (inserts == 1 && failure == Failure.INSERT_RESPONSE_LOST) {
                        throw Status.UNAVAILABLE.asRuntimeException();
                    }
                    return MutationResult.newBuilder()
                            .setInsertCnt(1)
                            .setIDs(IDs.newBuilder().setIntId(LongArray.newBuilder().addData(42L)))
                            .build();
                default:
                    throw new AssertionError("Unexpected RPC: " + method);
            }
        }

        private DescribeCollectionResponse describeCollection() {
            return DescribeCollectionResponse.newBuilder()
                    .setCollectionID(1L)
                    .addProperties(
                            KeyValuePair.newBuilder()
                                    .setKey("allow_insert_auto_id")
                                    .setValue("true"))
                    .setSchema(
                            CollectionSchema.newBuilder()
                                    .setName("test_collection")
                                    .setEnableDynamicField(true)
                                    .addFields(
                                            FieldSchema.newBuilder()
                                                    .setFieldID(100)
                                                    .setName("id")
                                                    .setDataType(DataType.Int64)
                                                    .setIsPrimaryKey(true)
                                                    .setAutoID(true))
                                    .addFields(
                                            FieldSchema.newBuilder()
                                                    .setFieldID(101)
                                                    .setName("name")
                                                    .setDataType(DataType.VarChar)
                                                    .addTypeParams(
                                                            KeyValuePair.newBuilder()
                                                                    .setKey("max_length")
                                                                    .setValue("100"))))
                    .build();
        }
    }
}
