package org.apache.seatunnel.connectors.seatunnel.milvus.utils;

import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.FieldSchema;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

public class MilvusConnectorUtils {
    private static final Logger log = LoggerFactory.getLogger(MilvusConnectorUtils.class);
    private static final SecretKey SECRET_KEY = generateSecretKey();

    public static String encryptToken(String token) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, SECRET_KEY);

            byte[] encryptedBytes = cipher.doFinal(token.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(encryptedBytes);
        } catch (Exception e) {
            // Handle encryption errors
            log.error("encryption error" + e.getMessage());
            return null;
        }
    }

    public static String decryptToken(String token) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, SECRET_KEY);

            byte[] encryptedBytes = Base64.getDecoder().decode(token);
            byte[] decryptedBytes = cipher.doFinal(encryptedBytes);

            return new String(decryptedBytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            // Handle decryption errors
            log.error("decryption error" + e.getMessage());
            return null;
        }
    }

    public static boolean isEncrypted(String token) {
        try {
            // Check if the string is a valid Base64 encoded string
            byte[] decodedBytes = Base64.getDecoder().decode(token);

            // Try to decrypt the string (this will throw an exception if it fails)
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, SECRET_KEY);
            cipher.doFinal(decodedBytes);

            // If decryption was successful, we assume the string was encrypted
            return true;
        } catch (Exception e) {
            // If any exception is caught, the string is likely not encrypted
            return false;
        }
    }

    public static SecretKey generateSecretKey() {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            SecureRandom secureRandom = new SecureRandom();
            keyGenerator.init(128, secureRandom);
            return keyGenerator.generateKey();
        } catch (NoSuchAlgorithmException e) {
            log.error(e.getMessage());
            return null;
        }
    }

    public static Boolean hasPartitionKey(String url, String token, String dbName, String collectionName) {
        ConnectParam connectParam = ConnectParam.newBuilder()
                .withUri(url)
                .withToken(token)
                .build();
        MilvusClient milvusClient = new MilvusServiceClient(connectParam);
        R<DescribeCollectionResponse> describeCollectionResponseR = milvusClient.describeCollection(
                DescribeCollectionParam.newBuilder()
                        .withDatabaseName(dbName)
                        .withCollectionName(collectionName)
                        .build());
        boolean hasPartitionKey = describeCollectionResponseR.getData().getSchema().getFieldsList().stream().anyMatch(FieldSchema::getIsPartitionKey);
        milvusClient.close();
        return hasPartitionKey;
    }
}
