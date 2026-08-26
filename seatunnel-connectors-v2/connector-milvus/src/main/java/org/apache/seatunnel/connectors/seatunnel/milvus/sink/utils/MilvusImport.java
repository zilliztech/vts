package org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils;

import com.google.gson.reflect.TypeToken;
import io.milvus.bulkwriter.restful.BulkImportUtils;
import io.milvus.bulkwriter.request.describe.CloudDescribeImportRequest;
import io.milvus.bulkwriter.response.BulkImportResponse;
import io.milvus.bulkwriter.response.RestfulResponse;
import io.milvus.common.utils.JsonUtils;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.milvus.external.api.ControllerAPI;
import org.apache.seatunnel.connectors.seatunnel.milvus.external.dto.StageBucket;
import org.apache.seatunnel.connectors.seatunnel.milvus.external.dto.GetImportProgressResp;
import org.apache.seatunnel.connectors.seatunnel.milvus.external.dto.InnerImportRequest;

import static org.apache.seatunnel.connectors.seatunnel.milvus.common.MilvusConstants.DEFAULT_PARTITION;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MilvusImport {

    private final String baseUrl;
    private final String clusterId;
    private final String dbName;
    private final String collectionName;
    private final String partitionName;
    private final String apiKey;
    private final StageBucket stageBucket;
    private ConcurrentHashMap<String, String> objectUrlsMap = new ConcurrentHashMap<>();
    public MilvusImport(String url, String dbName, String collectionName, String partitionName, StageBucket stageBucket) {
        this.stageBucket = stageBucket;
        this.clusterId = stageBucket.getInstanceId();
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.partitionName = partitionName;
        this.apiKey = stageBucket.getApiKey();
        // byoc pods cannot reach the public cloud api; the import trigger and progress
        // polls go to the data plane address handed down in the stage bucket config
        this.baseUrl = StringUtils.isNotEmpty(stageBucket.getCloudApiUrl())
                ? stageBucket.getCloudApiUrl()
                : ControllerAPI.getControllerAPI(url);
    }
    public void importDatas(List<List<String>> objectUrls) {
        for(List<String> objectUrl : objectUrls) {
            importData(objectUrl.get(0));
        }
    }
    public void importFolder(String objectFolder) {
        importData(objectFolder);
    }

    private String processUrl(String path) {
        if(stageBucket.getCloudId().equals("gcp")){
            return "https://storage.cloud.google.com/" + stageBucket.getBucketName() + "/" + path;
        }else if(stageBucket.getCloudId().equals("az") || stageBucket.getCloudId().equals("azure")) {
            https://myaccount.blob.core.windows.net/bucket-name/parquet-folder/data.parquet
            return "https://" + stageBucket.getAccessKey() + ".blob.core.windows.net/" + stageBucket.getBucketName() + "/" + path;
        }
        return "https://" + stageBucket.getBucketName() +  "." + stageBucket.getMinioUrl()+ "/" + path;
    }

    public void importData(String objectUrl) {
        if(objectUrlsMap.containsKey(objectUrl)) {
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
                .token(workloadIdentityToken())
                //the import job will be executed in the background, not showup in the console
                .innerCall(stageBucket.getInnerCall() == null || stageBucket.getInnerCall())
                .build();
        if(StringUtils.isNotEmpty(dbName) && !dbName.equals("default")){
            importRequest.setDbName(dbName);
        }
        if(StringUtils.isNotEmpty(partitionName) && !partitionName.equals(DEFAULT_PARTITION)){
            importRequest.setPartitionName(partitionName);
        }
        log.info("import objectUrl: " + objectUrl + " to collection: " + collectionName + " partition: " + partitionName);
        log.info("importRequest: " + importRequest);

        BulkImportResponse importResponse = importToCloud(baseUrl, importRequest);

        objectUrlsMap.put(objectUrl, importResponse.getJobId());
        log.info("[MILVUS_IMPORT_JOB] collection={}, partition={}, objectUrl={}, milvusImportJobId={}",
                collectionName, partitionName, objectUrl, importResponse.getJobId());
    }

    public void waitImportFinish() {
        while(!checkImportFinish()) {
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        log.info("all import jobs finished:{}", objectUrlsMap);
    }
    public boolean checkImportFinish() {
        HashSet<String> jobIds = new HashSet<>(objectUrlsMap.values());
        for(String jobId : jobIds) {
            log.info("wait import job: " + jobId + " finish");
            String state = StringUtils.isNotEmpty(stageBucket.getCloudApiUrl())
                    ? probeImportState(jobId)
                    : cloudImportState(jobId);
            if("Completed".equals(state)) {
                log.info("import job: " + jobId + " finish");
            }else if("Failed".equals(state)) {
                log.info("import job: " + jobId + "failed");
                throw new MilvusConnectorException(MilvusConnectionErrorCode.IMPORT_JOB_FAILED, "import job: " + jobId + "failed");
            }else {
                return false;
            }
        }
        return true;
    }

    private String cloudImportState(String jobId) {
        CloudDescribeImportRequest importProgress = CloudDescribeImportRequest.builder()
                        .apiKey(apiKey)
                        .clusterId(clusterId)
                        .jobId(jobId)
                        .build();
        String body = BulkImportUtils.getImportProgress(this.baseUrl, importProgress);
        RestfulResponse<GetImportProgressResp> response = JsonUtils.fromJson(body, (new TypeToken<RestfulResponse<GetImportProgressResp>>() {
        }).getType());
        return response.getData().getState();
    }

    private String probeImportState(String jobId) {
        String requestURL = stageBucket.getCloudApiUrl() + "/dv/v1/vts/probe/import_progress";
        Map<String, Object> body = new HashMap<>();
        body.put("regionId", stageBucket.getRegionId());
        body.put("vpcId", stageBucket.getVpcId());
        body.put("instanceId", clusterId);
        body.put("jobId", jobId);
        body.put("dbName", dbName);
        body.put("collectionName", collectionName);
        body.put("apiKey", apiKey);

        HttpResponse<String> response = Unirest.post(requestURL)
                .connectTimeout(60000)
                .headers(httpHeaders(apiKey))
                .body(JsonUtils.toJson(body))
                .asString();

        RestfulResponse<Map> parsed = JsonUtils.fromJson(response.getBody(), (new TypeToken<RestfulResponse<Map>>() {
        }).getType());
        if(parsed.getCode() != 0) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.IMPORT_JOB_FAILED, parsed.getMessage());
        }
        return (String) parsed.getData().get("state");
    }

    // mint a fresh token per request instead of caching the one from writer init:
    // the upload phase may approach the token lifetime, and the import trigger must
    // not inherit a nearly-expired credential
    private String workloadIdentityToken() {
        if (!Boolean.TRUE.equals(stageBucket.getUseWorkloadIdentity())) {
            return null;
        }
        if ("gcp".equals(stageBucket.getCloudId())) {
            return WorkloadIdentityCredentials.fetchGcpAccessToken();
        }
        return WorkloadIdentityCredentials
                .assumeAwsRoleWithWebIdentity(stageBucket.getRegionId()).getSessionToken();
    }

    private BulkImportResponse importToCloud(String baseUrl, InnerImportRequest importRequest) {
        if (StringUtils.isNotEmpty(stageBucket.getCloudApiUrl())) {
            return importViaProbe(importRequest);
        }
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

    // byoc: the probe channel on data-service bridges the call into the normal import
    // flow, so the control plane keeps the full job lifecycle as in saas
    private BulkImportResponse importViaProbe(InnerImportRequest importRequest) {
        String requestURL = stageBucket.getCloudApiUrl() + "/dv/v1/vts/probe/import_create";
        Map<String, Object> body = new HashMap<>();
        body.put("regionId", stageBucket.getRegionId());
        body.put("vpcId", stageBucket.getVpcId());
        body.put("instanceId", importRequest.getClusterId());
        body.put("objectUrl", importRequest.getObjectUrl());
        body.put("dbName", importRequest.getDbName());
        body.put("collectionName", importRequest.getCollectionName());
        body.put("partitionName", importRequest.getPartitionName());
        body.put("innerCall", importRequest.getInnerCall());
        body.put("apiKey", importRequest.getApiKey());
        body.put("accessKey", importRequest.getAccessKey());
        body.put("secretKey", importRequest.getSecretKey());
        body.put("token", importRequest.getToken());

        HttpResponse<String> response = Unirest.post(requestURL)
                .connectTimeout(60000)
                .headers(httpHeaders(apiKey))
                .body(JsonUtils.toJson(body))
                .asString();

        RestfulResponse<Map> parsed = JsonUtils.fromJson(response.getBody(), (new TypeToken<RestfulResponse<Map>>() {
        }).getType());
        if(parsed.getCode() != 0) {
            throw new MilvusConnectorException(MilvusConnectionErrorCode.IMPORT_JOB_FAILED, parsed.getMessage());
        }
        BulkImportResponse importResponse = new BulkImportResponse();
        importResponse.setJobId((String) parsed.getData().get("jobId"));
        return importResponse;
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

    /**
     * Get all Milvus import job IDs mapped to their object URLs
     * @return Map of object URL to Milvus import job ID
     */
    public Map<String, String> getImportJobIds() {
        return new HashMap<>(objectUrlsMap);
    }

    /**
     * Get the count of import jobs
     * @return Number of import jobs
     */
    public int getImportJobCount() {
        return objectUrlsMap.size();
    }

    /**
     * Get import jobs as a formatted string for logging
     * @return Formatted string of import jobs
     */
    public String getImportJobsInfo() {
        if (objectUrlsMap.isEmpty()) {
            return "No import jobs";
        }
        StringBuilder sb = new StringBuilder();
        objectUrlsMap.forEach((url, jobId) ->
            sb.append(String.format("url=%s,jobId=%s;", url, jobId))
        );
        return sb.toString();
    }
}
