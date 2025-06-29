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

package org.apache.seatunnel.connectors.seatunnel.milvus.common;

public class MilvusConstants {

    // Partition constants
    public static final String DEFAULT_PARTITION = "_default";

    // Collection options
    public static final String ENABLE_DYNAMIC_FIELD = "enableDynamicField";
    public static final String SHARDS_NUM = "shardsNum";
    public static final String PARTITION_KEY_FIELD = "partitionKeyField";
    public static final String PARTITION_NAMES = "partitionNames";
    public static final String MILVUS = "milvus";
    public static final String ENABLE_AUTO_ID = "enableAutoId";
    public static final String CONSISTENCY_LEVEL = "consistencyLevel";
    public static final String ELEMENT_TYPE = "elementType";
    public static final String MAX_CAPACITY = "maxCapacity";
    public static final String MAX_LENGTH = "maxLength";
}