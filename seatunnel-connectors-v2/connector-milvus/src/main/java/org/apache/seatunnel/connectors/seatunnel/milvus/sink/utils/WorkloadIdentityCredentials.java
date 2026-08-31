package org.apache.seatunnel.connectors.seatunnel.milvus.sink.utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.minio.credentials.Jwt;
import io.minio.credentials.Provider;
import io.minio.credentials.WebIdentityProvider;
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
 * Bridges the workload identity of the cluster the writer runs in into object-storage
 * access, so migrations never have to handle static access keys:
 *
 * <ul>
 *   <li>AWS write path: {@link #awsWebIdentityProvider(String)} hands the bulk writer a
 *       minio {@link WebIdentityProvider} that exchanges the pod's IRSA token
 *       (AWS_ROLE_ARN / AWS_WEB_IDENTITY_TOKEN_FILE) for session credentials and
 *       refreshes them automatically before expiry, so long-running jobs never hit an
 *       expired session.</li>
 *   <li>AWS import path: {@link #assumeAwsRoleWithWebIdentity(String)} mints one session
 *       triple that is forwarded to the control plane, whose environment sits outside the
 *       identity's trust boundary. Its lifetime is fixed at mint time, so it stays
 *       configurable via VTS_AWS_SESSION_DURATION_SECONDS (default 1h, the IAM role
 *       default) instead of a hardcoded value that stock roles reject.</li>
 *   <li>GCP: {@link #fetchGcpAccessToken()} fetches an OAuth2 access token from the GKE
 *       metadata server for the import path; the write path uses the bulk writer's
 *       self-refreshing GCP provider instead.</li>
 *   <li>Azure: handled by the sink through DefaultAzureCredential, which discovers the AKS
 *       workload identity / managed identity by itself.</li>
 * </ul>
 */
@Slf4j
public class WorkloadIdentityCredentials {

    private static final int HTTP_TIMEOUT_MS = 10_000;
    private static final String GCP_TOKEN_URL =
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
    // 1h matches the IAM role default MaxSessionDuration, so stock BYOC roles work
    // unmodified; raise only when the target role allows a longer MaxSessionDuration
    public static final String AWS_SESSION_DURATION_ENV = "VTS_AWS_SESSION_DURATION_SECONDS";
    public static final int DEFAULT_AWS_SESSION_DURATION_SECONDS = 3600;

    @Value
    public static class AwsSessionCredentials {
        String accessKey;
        String secretKey;
        String sessionToken;
        String expiration;
    }

    @Value
    private static class AwsWebIdentityEnv {
        String roleArn;
        String tokenFile;
        String sessionName;
        String stsEndpoint;
    }

    /**
     * Builds the self-refreshing credentials provider for the bulk writer's S3 client.
     * DurationSeconds is deliberately omitted from the STS request (jwt expiry 0 plus a
     * null durationSeconds), so STS grants the role's default session duration and every
     * customer role works regardless of its MaxSessionDuration; the provider re-assumes
     * the role before each session expires.
     */
    public static Provider awsWebIdentityProvider(String region) {
        AwsWebIdentityEnv env = awsWebIdentityEnv(region);
        return new WebIdentityProvider(
                () -> new Jwt(readTokenFile(env.getTokenFile()), 0),
                env.getStsEndpoint(), null, null, env.getRoleArn(), env.getSessionName(), null);
    }

    /**
     * Mints one session triple for the import path, where the credentials must cross
     * into the control plane and are consumed asynchronously.
     */
    public static AwsSessionCredentials assumeAwsRoleWithWebIdentity(String region) {
        AwsWebIdentityEnv env = awsWebIdentityEnv(region);
        int durationSeconds = configuredSessionDurationSeconds();
        try {
            String webIdentityToken = readTokenFile(env.getTokenFile());
            String body = "Action=AssumeRoleWithWebIdentity&Version=2011-06-15"
                    + "&RoleArn=" + urlEncode(env.getRoleArn())
                    + "&RoleSessionName=" + urlEncode(env.getSessionName())
                    + "&WebIdentityToken=" + urlEncode(webIdentityToken)
                    + "&DurationSeconds=" + durationSeconds;
            String response = httpRequest("POST", env.getStsEndpoint(),
                    "application/x-www-form-urlencoded", null, body);
            AwsSessionCredentials credentials = new AwsSessionCredentials(
                    xmlTagValue(response, "AccessKeyId"),
                    xmlTagValue(response, "SecretAccessKey"),
                    xmlTagValue(response, "SessionToken"),
                    xmlTagValue(response, "Expiration"));
            log.info("Obtained AWS session credentials via web identity, roleArn={}, durationSeconds={}, expiration={}",
                    env.getRoleArn(), durationSeconds, credentials.getExpiration());
            return credentials;
        } catch (MilvusConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INIT_WRITER_ERROR,
                    "failed to assume role via web identity: " + e.getMessage(), e);
        }
    }

    private static AwsWebIdentityEnv awsWebIdentityEnv(String region) {
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
        return new AwsWebIdentityEnv(roleArn, tokenFile, sessionName, stsEndpoint);
    }

    static int configuredSessionDurationSeconds() {
        return parseSessionDurationSeconds(System.getenv(AWS_SESSION_DURATION_ENV));
    }

    static int parseSessionDurationSeconds(String raw) {
        if (isEmpty(raw)) {
            return DEFAULT_AWS_SESSION_DURATION_SECONDS;
        }
        final int duration;
        try {
            duration = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INIT_WRITER_ERROR,
                    AWS_SESSION_DURATION_ENV + " must be a positive integer, got: " + raw);
        }
        if (duration <= 0) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INIT_WRITER_ERROR,
                    AWS_SESSION_DURATION_ENV + " must be a positive integer, got: " + raw);
        }
        return duration;
    }

    private static String readTokenFile(String tokenFile) {
        try {
            return new String(Files.readAllBytes(Paths.get(tokenFile)), StandardCharsets.UTF_8).trim();
        } catch (IOException e) {
            throw new MilvusConnectorException(
                    MilvusConnectionErrorCode.INIT_WRITER_ERROR,
                    "failed to read the web identity token file " + tokenFile + ": " + e.getMessage(), e);
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
