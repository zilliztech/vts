package org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Mints temporary object-storage credentials from the workload identity of the cluster the
 * writer runs in, so migrations never have to handle static access keys:
 *
 * <ul>
 *   <li>AWS: the pod's service account carries AWS_ROLE_ARN and AWS_WEB_IDENTITY_TOKEN_FILE
 *       (EKS IRSA); the web identity token is exchanged for session credentials via STS
 *       AssumeRoleWithWebIdentity, an unsigned call that needs no AWS SDK.</li>
 *   <li>GCP: an OAuth2 access token is fetched from the GKE metadata server (workload
 *       identity) and passed to the bulk writer, which sends it as a Bearer credential
 *       against the GCS XML API.</li>
 *   <li>Azure: handled by the sink through DefaultAzureCredential, which discovers the AKS
 *       workload identity / managed identity by itself.</li>
 * </ul>
 *
 * The returned credentials expire (AWS session and GCP token both default to one hour).
 * Uploads that may outlive them need credential refresh inside milvus-sdk-java's bulk
 * writer, e.g. a minio WebIdentityProvider or a GCS-native storage client.
 */
@Slf4j
public class WorkloadIdentityCredentials {

    private static final int HTTP_TIMEOUT_MS = 10_000;
    private static final String GCP_TOKEN_URL =
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
    private static final int AWS_SESSION_DURATION_SECONDS = 3600;

    @Value
    public static class AwsSessionCredentials {
        String accessKey;
        String secretKey;
        String sessionToken;
        String expiration;
    }

    public static AwsSessionCredentials assumeAwsRoleWithWebIdentity(String region) {
        String roleArn = System.getenv("AWS_ROLE_ARN");
        String tokenFile = System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE");
        if (isEmpty(roleArn) || isEmpty(tokenFile)) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INIT_WRITER_ERROR,
                    "use_workload_identity is set but AWS_ROLE_ARN/AWS_WEB_IDENTITY_TOKEN_FILE are not present; "
                            + "the writer pod is not running with an IRSA-enabled service account");
        }
        String sessionName = System.getenv("AWS_ROLE_SESSION_NAME");
        if (isEmpty(sessionName)) {
            sessionName = "vts-bulk-writer";
        }
        String stsEndpoint = System.getenv("AWS_STS_ENDPOINT");
        if (isEmpty(stsEndpoint)) {
            stsEndpoint = "https://sts." + region + ".amazonaws.com";
        }
        try {
            String webIdentityToken =
                    new String(Files.readAllBytes(Paths.get(tokenFile)), StandardCharsets.UTF_8).trim();
            String body = "Action=AssumeRoleWithWebIdentity&Version=2011-06-15"
                    + "&RoleArn=" + urlEncode(roleArn)
                    + "&RoleSessionName=" + urlEncode(sessionName)
                    + "&WebIdentityToken=" + urlEncode(webIdentityToken)
                    + "&DurationSeconds=" + AWS_SESSION_DURATION_SECONDS;
            String response = httpRequest("POST", stsEndpoint, "application/x-www-form-urlencoded",
                    null, body);
            AwsSessionCredentials credentials = new AwsSessionCredentials(
                    xmlTagValue(response, "AccessKeyId"),
                    xmlTagValue(response, "SecretAccessKey"),
                    xmlTagValue(response, "SessionToken"),
                    xmlTagValue(response, "Expiration"));
            log.info("Obtained AWS session credentials via web identity, roleArn={}, expiration={}",
                    roleArn, credentials.getExpiration());
            return credentials;
        } catch (MilvusConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INIT_WRITER_ERROR,
                    "failed to assume role via web identity: " + e.getMessage(), e);
        }
    }

    public static String fetchGcpAccessToken() {
        try {
            String response = httpRequest("GET", GCP_TOKEN_URL, null,
                    "Metadata-Flavor: Google", null);
            JsonObject json = new Gson().fromJson(response, JsonObject.class);
            log.info("Obtained GCP access token from metadata server, expiresIn={}s",
                    json.get("expires_in").getAsString());
            return json.get("access_token").getAsString();
        } catch (Exception e) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INIT_WRITER_ERROR,
                    "failed to fetch an access token from the GKE metadata server; "
                            + "the writer pod may not be bound to a workload identity: " + e.getMessage(), e);
        }
    }

    private static String httpRequest(String method, String url, String contentType,
                                      String header, String body) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod(method);
        conn.setConnectTimeout(HTTP_TIMEOUT_MS);
        conn.setReadTimeout(HTTP_TIMEOUT_MS);
        if (contentType != null) {
            conn.setRequestProperty("Content-Type", contentType);
        }
        if (header != null) {
            int split = header.indexOf(':');
            conn.setRequestProperty(header.substring(0, split).trim(), header.substring(split + 1).trim());
        }
        if (body != null) {
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }
        }
        int status = conn.getResponseCode();
        String response;
        try (InputStream stream = status >= 400 ? conn.getErrorStream() : conn.getInputStream()) {
            response = new String(readAll(stream), StandardCharsets.UTF_8);
        }
        if (status >= 400) {
            throw new IOException("http " + status + " from " + url + ": " + response);
        }
        return response;
    }

    private static byte[] readAll(InputStream stream) throws IOException {
        if (stream == null) {
            return new byte[0];
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int n;
        while ((n = stream.read(buffer)) != -1) {
            out.write(buffer, 0, n);
        }
        return out.toByteArray();
    }

    private static String xmlTagValue(String xml, String tag) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            Document doc = factory.newDocumentBuilder()
                    .parse(new InputSource(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))));
            NodeList nodes = doc.getElementsByTagName(tag);
            if (nodes.getLength() == 0) {
                throw new MilvusConnectorException(
                        MilvusConnectionErrorCode.INIT_WRITER_ERROR,
                        "STS response is missing " + tag + ": " + xml);
            }
            return nodes.item(0).getTextContent();
        } catch (MilvusConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INIT_WRITER_ERROR,
                    "failed to parse the STS response: " + e.getMessage(), e);
        }
    }

    private static String urlEncode(String value) throws IOException {
        return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
    }

    private static boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }
}
