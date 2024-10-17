package org.apache.seatunnel.connectors.seatunnel.milvus.utils;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.connectors.seatunnel.milvus.catalog.MilvusOptions;

import java.util.List;

@Slf4j
public class MilvusConnectorUtils {

    public static Boolean hasPartitionKey(MilvusClientV2 milvusClient, String collectionName) {

        DescribeCollectionResp describeCollectionResp = milvusClient.describeCollection(
                DescribeCollectionReq.builder()
                        .collectionName(collectionName)
                        .build());
        return describeCollectionResp.getCollectionSchema().getFieldSchemaList().stream()
                .anyMatch(CreateCollectionReq.FieldSchema::getIsPartitionKey);
    }

    public static String getDynamicField(CatalogTable catalogTable) {
        List<Column> columns =  catalogTable.getTableSchema().getColumns();
        Column dynamicField = null;
        for (Column column : columns) {
            if(column.getOptions() != null && column.getOptions().containsKey(MilvusOptions.IS_DYNAMIC_FIELD)
                    && (Boolean) column.getOptions().get(MilvusOptions.IS_DYNAMIC_FIELD)){
                // skip dynamic field
                dynamicField = column;
            }
        }
        return dynamicField == null ? null : dynamicField.getName();
    }
}
