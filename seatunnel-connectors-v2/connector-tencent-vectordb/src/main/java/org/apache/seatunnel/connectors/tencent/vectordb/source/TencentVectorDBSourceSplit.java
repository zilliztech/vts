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

package org.apache.seatunnel.connectors.tencent.vectordb.source;

import lombok.Data;
import lombok.experimental.SuperBuilder;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.TablePath;

@Data
@SuperBuilder
public class TencentVectorDBSourceSplit implements SourceSplit {
    private TablePath tablePath;
    private String splitId;
    private String partitionName;
    /**
     * Get the split id of this source split.
     *
     * @return id of this source split.
     */
    @Override
    public String splitId() {
        return splitId;
    }
}
