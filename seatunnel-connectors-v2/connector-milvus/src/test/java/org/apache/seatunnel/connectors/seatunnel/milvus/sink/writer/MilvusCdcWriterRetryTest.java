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
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import com.google.gson.JsonObject;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.shaded.io.grpc.Channel;
import io.milvus.shaded.io.grpc.Status;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.client.RetryConfig;
import io.milvus.v2.exception.ErrorCode;
import io.milvus.v2.exception.MilvusClientException;
import io.milvus.v2.service.vector.VectorService;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.response.DeleteResp;
import io.milvus.v2.service.vector.response.InsertResp;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MilvusCdcWriterRetryTest {
    @Test
    void deleteFailureRetriesBeforeInserting() throws Exception {
        TestClient test = new TestClient();
        when(test.vectors.delete(any(), any(DeleteReq.class)))
                .thenThrow(Status.UNAVAILABLE.asRuntimeException())
                .thenReturn(DeleteResp.builder().build());

        test.execute();

        InOrder order = inOrder(test.vectors);
        order.verify(test.vectors, times(2)).delete(any(), any(DeleteReq.class));
        order.verify(test.vectors).insert(any(), any(InsertReq.class));
        order.verifyNoMoreInteractions();
        test.verifyPolicyRestored();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void insertFailureRetriesWholeReplacement(boolean appliedBeforeFailure) throws Exception {
        TestClient test = new TestClient();
        List<Long> storedIds = new ArrayList<>(Arrays.asList(42L, 42L));
        AtomicInteger attempts = new AtomicInteger();
        when(test.vectors.delete(any(), any(DeleteReq.class)))
                .thenAnswer(
                        call -> {
                            storedIds.removeAll(call.<DeleteReq>getArgument(1).getIds());
                            return DeleteResp.builder().build();
                        });
        when(test.vectors.insert(any(), any(InsertReq.class)))
                .thenAnswer(
                        call -> {
                            boolean first = attempts.incrementAndGet() == 1;
                            if (!first || appliedBeforeFailure) {
                                storedIds.add(42L);
                            }
                            if (first) {
                                throw Status.UNAVAILABLE.asRuntimeException();
                            }
                            return InsertResp.builder().InsertCnt(1L).build();
                        });

        test.execute();

        InOrder order = inOrder(test.vectors);
        order.verify(test.vectors).delete(any(), any(DeleteReq.class));
        order.verify(test.vectors).insert(any(), any(InsertReq.class));
        order.verify(test.vectors).delete(any(), any(DeleteReq.class));
        order.verify(test.vectors).insert(any(), any(InsertReq.class));
        order.verifyNoMoreInteractions();
        assertEquals(Collections.singletonList(42L), storedIds);
        test.verifyPolicyRestored();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3})
    void exhaustedRetriesRestoreDefaultPolicy(int attempts) throws Exception {
        TestClient test = new TestClient();
        test.defaultPolicy.setMaxRetryTimes(attempts);
        test.defaultPolicy.setInitialBackOffMs(0L);
        test.defaultPolicy.setMaxBackOffMs(0L);
        when(test.vectors.insert(any(), any(InsertReq.class)))
                .thenThrow(Status.UNAVAILABLE.asRuntimeException());

        assertThrows(MilvusConnectorException.class, test::execute);

        verify(test.vectors, times(attempts)).delete(any(), any(DeleteReq.class));
        verify(test.vectors, times(attempts)).insert(any(), any(InsertReq.class));
        test.verifyPolicyRestored();
    }

    @ParameterizedTest
    @ValueSource(strings = {"INVALID_ARGUMENT", "PERMISSION_DENIED", "UNAUTHENTICATED"})
    void nonRetryableFailuresRestorePolicyWithoutRetrying(String code) throws Exception {
        TestClient test = new TestClient();
        when(test.vectors.insert(any(), any(InsertReq.class)))
                .thenThrow(Status.fromCode(Status.Code.valueOf(code)).asRuntimeException());

        assertThrows(MilvusConnectorException.class, test::execute);

        verify(test.vectors).delete(any(), any(DeleteReq.class));
        verify(test.vectors).insert(any(), any(InsertReq.class));
        test.verifyPolicyRestored();
    }

    @Test
    void interruptedBackoffRestoresPolicyAndPreservesInterrupt() throws Exception {
        TestClient test = new TestClient();
        when(test.vectors.insert(any(), any(InsertReq.class)))
                .thenAnswer(
                        call -> {
                            Thread.currentThread().interrupt();
                            throw Status.UNAVAILABLE.asRuntimeException();
                        });
        try {
            assertThrows(MilvusConnectorException.class, test::execute);
            assertTrue(Thread.currentThread().isInterrupted());
            verify(test.vectors).delete(any(), any(DeleteReq.class));
            verify(test.vectors).insert(any(), any(InsertReq.class));
            test.verifyPolicyRestored();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void ordinaryInsertStillUsesDefaultSdkRetryAfterUpsert() throws Exception {
        TestClient test = new TestClient();
        test.execute();
        test.verifyPolicyRestored();
        assertEquals(RetryConfig.builder().build().toString(), test.defaultPolicy.toString());
        clearInvocations(test.vectors);
        when(test.vectors.insert(any(), any(InsertReq.class)))
                .thenThrow(Status.UNAVAILABLE.asRuntimeException())
                .thenReturn(InsertResp.builder().InsertCnt(1L).build());

        test.client.insert(test.insertRequest);

        verify(test.vectors, times(2)).insert(any(), any(InsertReq.class));
    }

    @Test
    void rateLimitUsesDefaultRetryPolicyForWholeOperation() throws Exception {
        TestClient test = new TestClient();
        when(test.vectors.insert(any(), any(InsertReq.class)))
                .thenThrow(new MilvusClientException(ErrorCode.SERVER_ERROR, "rate limited", 8, 0))
                .thenReturn(InsertResp.builder().InsertCnt(1L).build());

        test.execute();

        verify(test.vectors, times(2)).delete(any(), any(DeleteReq.class));
        verify(test.vectors, times(2)).insert(any(), any(InsertReq.class));
        test.verifyPolicyRestored();
    }

    private static class TestClient {
        private final MilvusClientV2 client = spy(new MilvusClientV2(null));
        private final VectorService vectors = mock(VectorService.class);
        private final MilvusCdcWriter writer;
        private final RetryConfig defaultPolicy;
        private final InsertReq insertRequest;

        private TestClient() throws Exception {
            writer =
                    new MilvusCdcWriter(
                            MilvusCdcWriterTest.catalogTable(),
                            ReadonlyConfig.fromMap(Collections.singletonMap("enable_upsert", true)),
                            client,
                            MilvusCdcWriterTest.autoIdDescription(),
                            () -> 0L);
            defaultPolicy = MilvusCdcWriterTest.retryConfig(writer);
            client.setBlockingStub(MilvusServiceGrpc.newBlockingStub(mock(Channel.class)));
            Field field = MilvusClientV2.class.getDeclaredField("vectorService");
            field.setAccessible(true);
            field.set(client, vectors);
            clearInvocations(client);
            JsonObject row = new JsonObject();
            row.addProperty("id", 42L);
            insertRequest =
                    InsertReq.builder()
                            .collectionName("test")
                            .data(Collections.singletonList(row))
                            .build();
            when(vectors.delete(any(), any(DeleteReq.class)))
                    .thenReturn(DeleteResp.builder().build());
            when(vectors.insert(any(), any(InsertReq.class)))
                    .thenReturn(InsertResp.builder().InsertCnt(1L).build());
        }

        private void execute() {
            writer.write(new SeaTunnelRow(new Object[] {42L, "updated"}));
            writer.commit(false);
        }

        private void verifyPolicyRestored() {
            InOrder order = inOrder(client);
            order.verify(client).retryConfig(argThat(config -> config.getMaxRetryTimes() == 1));
            order.verify(client).retryConfig(same(defaultPolicy));
        }
    }
}
