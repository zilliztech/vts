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

package mongodb.sender;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.sender.MongoDBConnectorDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils;

import org.apache.kafka.connect.source.SourceRecord;

import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.COLL_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DB_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.DOCUMENT_KEY;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.FULL_DOCUMENT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.NS_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.OPERATION_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.OPERATION_TYPE_INSERT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SNAPSHOT_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SNAPSHOT_TRUE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.SOURCE_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.TS_MS_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbRecordUtils.createSourceOffsetMap;

public class MongoDBConnectorDeserializationSchemaTest {

    @Test
    public void extractTableId() {
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "name1", BasicType.STRING_TYPE, 1L, true, null, ""))
                                .column(
                                        PhysicalColumn.of(
                                                "name1", BasicType.STRING_TYPE, 1L, true, null, ""))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "comment");
        SeaTunnelDataType<SeaTunnelRow> dataType =
                CatalogTableUtil.convertToMultipleRowType(Collections.singletonList(catalogTable));
        MongoDBConnectorDeserializationSchema schema =
                new MongoDBConnectorDeserializationSchema(dataType, dataType);

        // Build SourceRecord
        Map<String, String> partitionMap =
                MongodbRecordUtils.createPartitionMap("localhost:27017", "inventory", "products");

        BsonDocument valueDocument =
                new BsonDocument()
                        .append(
                                ID_FIELD,
                                new BsonDocument(ID_FIELD, new BsonInt64(10000000000001L)))
                        .append(OPERATION_TYPE, new BsonString(OPERATION_TYPE_INSERT))
                        .append(
                                NS_FIELD,
                                new BsonDocument(DB_FIELD, new BsonString("inventory"))
                                        .append(COLL_FIELD, new BsonString("products")))
                        .append(
                                DOCUMENT_KEY,
                                new BsonDocument(ID_FIELD, new BsonInt64(10000000000001L)))
                        .append(FULL_DOCUMENT, new BsonDocument())
                        .append(TS_MS_FIELD, new BsonInt64(System.currentTimeMillis()))
                        .append(
                                SOURCE_FIELD,
                                new BsonDocument(SNAPSHOT_FIELD, new BsonString(SNAPSHOT_TRUE))
                                        .append(TS_MS_FIELD, new BsonInt64(0L)));
        BsonDocument keyDocument = new BsonDocument(ID_FIELD, valueDocument.get(ID_FIELD));
        SourceRecord sourceRecord =
                MongodbRecordUtils.buildSourceRecord(
                        partitionMap,
                        createSourceOffsetMap(keyDocument.getDocument(ID_FIELD), true),
                        "inventory.products",
                        keyDocument,
                        valueDocument);
        Object tableId = schema.extractTableIdForTest(sourceRecord);
        Assertions.assertEquals("inventory.products", tableId);
    }
}
