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

package org.apache.seatunnel.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class HandoverTest {

    @Test
    public void testThrowExceptionWhenQueueIsEmtpy() {
        Handover<Object> handover = new Handover<>();
        handover.reportError(new RuntimeException("test"));
        Assertions.assertThrows(RuntimeException.class, handover::isEmpty);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testFirstFailureTakesPrecedenceOverBufferedDataAndClose(boolean bufferData)
            throws Exception {
        Handover<Object> handover = new Handover<>();
        if (bufferData) {
            handover.produce(new Object());
        }
        RuntimeException failure = new RuntimeException("reader failed");
        handover.reportError(failure);
        handover.reportError(new RuntimeException("another reader failed"));
        handover.close();

        Assertions.assertSame(
                failure, Assertions.assertThrows(RuntimeException.class, handover::isEmpty));
        Assertions.assertSame(
                failure, Assertions.assertThrows(RuntimeException.class, handover::pollNext));
    }
}
