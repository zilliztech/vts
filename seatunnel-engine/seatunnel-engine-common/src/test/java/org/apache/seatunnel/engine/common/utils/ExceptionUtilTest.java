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

package org.apache.seatunnel.engine.common.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExceptionUtilTest {

    @Test
    void throwsCheckedException() {
        Exception exception = new Exception("Checked Exception");
        assertThrows(Exception.class, () -> ExceptionUtil.sneakyThrow(exception));
    }

    @Test
    void throwsUncheckedException() {
        RuntimeException exception = new RuntimeException("Unchecked Exception");
        assertThrows(RuntimeException.class, () -> ExceptionUtil.sneakyThrow(exception));
    }

    @Test
    void throwsError() {
        Error error = new Error("Error");
        assertThrows(Error.class, () -> ExceptionUtil.sneakyThrow(error));
    }

    @Test
    void throwsNullPointerExceptionWhenNull() {
        assertThrows(NullPointerException.class, () -> ExceptionUtil.sneakyThrow(null));
    }
}
