package org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer;

import com.google.gson.reflect.TypeToken;
import io.milvus.bulkwriter.BulkImport;
import io.milvus.bulkwriter.request.describe.CloudDescribeImportRequest;
import io.milvus.bulkwriter.request.import_.CloudImportRequest;
import io.milvus.bulkwriter.response.BulkImportResponse;
import io.milvus.bulkwriter.response.GetImportProgressResponse;
import io.milvus.bulkwriter.response.RestfulResponse;
import io.milvus.common.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.common.ControllerAPI;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.common.StageBucket;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

@Slf4j
public class MilvusImport {

    private final String baseUrl;
    private final String clusterId;
    private final String collectionName;
    private final String partitionName;
    private final String minioUrl;
    private final String bucketName;
    private final String apiKey;
    private final String accessKey;
    private final String secretKey;
    private HashMap<String, String> objectUrls = new HashMap<>();
    public MilvusImport(String url, String collectionName, String partitionName, StageBucket stageBucket) {
        this.clusterId = stageBucket.getInstanceId();
        this.collectionName = collectionName;
        this.partitionName = partitionName;
        this.apiKey = stageBucket.getApiKey();
        this.minioUrl = stageBucket.getMinioUrl();
        this.bucketName = stageBucket.getBucketName();
        this.accessKey = stageBucket.getAccessKey();
        this.secretKey = stageBucket.getSecretKey();
        this.baseUrl = ControllerAPI.getControllerAPI(url);
    }
    public void importDatas(List<List<String>> objectUrls) {
        for(List<String> objectUrl : objectUrls) {
            importData(objectUrl.get(0));
        }
    }
    public void importData(String objectUrl) {
        if(objectUrls.containsKey(objectUrl)) {
            log.info("objectUrl: " + objectUrl + " has been imported, skip");
            return;
        }
        log.info("import objectUrl: " + objectUrl);
        CloudImportRequest importRequest = CloudImportRequest.builder()
                .apiKey(apiKey)
                .clusterId(clusterId)
                .collectionName(collectionName)
                .objectUrl("https://" + bucketName +  "." + minioUrl+ "/" + objectUrl)
                .accessKey(accessKey)
                .secretKey(secretKey)
                .build();
        if(StringUtils.isNotEmpty(partitionName) && !partitionName.equals("_default")){
            importRequest.setPartitionName(partitionName);
        }
        log.info("import objectUrl: " + objectUrl + " to collection: " + collectionName + " partition: " + partitionName);
        log.info("importRequest: " + importRequest);
        String body = BulkImport.bulkImport(baseUrl, importRequest);
        RestfulResponse<BulkImportResponse> response = JsonUtils.fromJson(body, (new TypeToken<RestfulResponse<BulkImportResponse>>() {
        }).getType());
        objectUrls.put(objectUrl, response.getData().getJobId());
        log.info("import objectUrl: " + objectUrl + " success");
    }
    public void waitImportFinish() {
        while(!checkImportFinish()) {
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        log.info("all import job finish");
    }
    public boolean checkImportFinish() {
        HashSet<String> jobIds = new HashSet<>(objectUrls.values());
        for(String jobId : jobIds) {
            log.info("wait import job: " + jobId + " finish");
            CloudDescribeImportRequest importProgress = CloudDescribeImportRequest.builder()
                            .apiKey(apiKey)
                            .clusterId(clusterId)
                            .jobId(jobId)
                            .build();
            String body = BulkImport.getImportProgress(this.baseUrl, importProgress);
            RestfulResponse<GetImportProgressResponse> response = JsonUtils.fromJson(body, (new TypeToken<RestfulResponse<GetImportProgressResponse>>() {
            }).getType());
            if(response.getData().getState().equals("Completed")) {
                log.info("import job: " + jobId + " finish");
            }else if(response.getData().getState().equals("Failed")) {
                log.info("import job: " + jobId + "failed");
                throw new MilvusConnectorException(MilvusConnectionErrorCode.IMPORT_JOB_FAILED, "import job: " + jobId + "failed");
            }else {
                return false;
            }
        }
        return true;
    }
}
