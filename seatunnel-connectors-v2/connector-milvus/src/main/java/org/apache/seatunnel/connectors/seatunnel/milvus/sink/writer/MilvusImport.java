package org.apache.seatunnel.connectors.seatunnel.milvus.sink.writer;

import com.google.gson.reflect.TypeToken;
import io.milvus.bulkwriter.BulkImport;
import io.milvus.bulkwriter.request.describe.CloudDescribeImportRequest;
import io.milvus.bulkwriter.response.BulkImportResponse;
import io.milvus.bulkwriter.response.GetImportProgressResponse;
import io.milvus.bulkwriter.response.RestfulResponse;
import io.milvus.common.utils.JsonUtils;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.common.ControllerAPI;
import org.apache.seatunnel.connectors.seatunnel.milvus.sink.common.StageBucket;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@Slf4j
public class MilvusImport {

    private final String baseUrl;
    private final String clusterId;
    private final String collectionName;
    private final String partitionName;
    private final String apiKey;
    private final StageBucket stageBucket;
    private HashMap<String, String> objectUrls = new HashMap<>();
    public MilvusImport(String url, String collectionName, String partitionName, StageBucket stageBucket) {
        this.stageBucket = stageBucket;
        this.clusterId = stageBucket.getInstanceId();
        this.collectionName = collectionName;
        this.partitionName = partitionName;
        this.apiKey = stageBucket.getApiKey();
        this.baseUrl = ControllerAPI.getControllerAPI(url);
    }
    public void importDatas(List<List<String>> objectUrls) {
        for(List<String> objectUrl : objectUrls) {
            importData(objectUrl.get(0));
        }
    }

    private String processUrl(String path) {
        if(stageBucket.getCloudId().equals("gcp")){
            return "https://storage.cloud.google.com/" + stageBucket.getBucketName() + "/" + path;
        }else if(stageBucket.getCloudId().equals("az")) {
            https://myaccount.blob.core.windows.net/bucket-name/parquet-folder/data.parquet
            return "https://" + stageBucket.getAccessKey() + ".blob.core.windows.net/" + stageBucket.getBucketName() + "/" + path;
        }
        return "https://" + stageBucket.getBucketName() +  "." + stageBucket.getMinioUrl()+ "/" + path;
    }

    public void importData(String objectUrl) {
        if(objectUrls.containsKey(objectUrl)) {
            log.info("objectUrl: " + objectUrl + " has been imported, skip");
            return;
        }
        String objectUrlStr = processUrl(objectUrl);
        log.info("import objectUrl: " + objectUrl);
        InnerImportRequest importRequest = InnerImportRequest.builder()
                .apiKey(apiKey)
                .clusterId(clusterId)
                .collectionName(collectionName)
                .objectUrl(objectUrlStr)
                .accessKey(stageBucket.getAccessKey())
                .secretKey(stageBucket.getSecretKey())
                //the import job will be executed in the background, not showup in the console
                .innerCall(stageBucket.getInnerCall() == null || stageBucket.getInnerCall())
                .build();

        if(StringUtils.isNotEmpty(partitionName) && !partitionName.equals("_default")){
            importRequest.setPartitionName(partitionName);
        }
        log.info("import objectUrl: " + objectUrl + " to collection: " + collectionName + " partition: " + partitionName);
        log.info("importRequest: " + importRequest);

        BulkImportResponse importResponse = importToCloud(baseUrl, importRequest);

        objectUrls.put(objectUrl, importResponse.getJobId());
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

    private BulkImportResponse importToCloud(String baseUrl, InnerImportRequest importRequest) {
        String requestURL = baseUrl + "/v2/vectordb/jobs/import/create";

        HttpResponse<String> body = Unirest.post(requestURL)
                .connectTimeout(60000)
                .headers(httpHeaders(apiKey))
                .body(JsonUtils.toJson(importRequest))
                .asString();

        RestfulResponse<BulkImportResponse> importResponseRestfulResponse =  JsonUtils.fromJson(body.getBody(), (new TypeToken<RestfulResponse<BulkImportResponse>>() {
        }).getType());
        if(importResponseRestfulResponse.getCode() != 0) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.IMPORT_JOB_FAILED, importResponseRestfulResponse.getMessage());
        }
        return importResponseRestfulResponse.getData();
    }

    protected static Map<String, String> httpHeaders(String apiKey) {
        Map<String, String> header = new HashMap<>();
        header.put("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11");
        header.put("Accept", "application/json");
        header.put("Content-Type", "application/json");
        header.put("Accept-Encodin", "gzip,deflate,sdch");
        header.put("Accept-Languag", "en-US,en;q=0.5");
        if (StringUtils.isNotEmpty(apiKey)) {
            header.put("Authorization", "Bearer " + apiKey);
        }

        return header;
    }
}
