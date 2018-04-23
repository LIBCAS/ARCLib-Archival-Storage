package cz.cas.lib.arcstorage.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cas.lib.arcstorage.domain.ChecksumType;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.exception.ConfigParserException;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.gateway.dto.Checksum;
import cz.cas.lib.arcstorage.storage.ceph.CephAdapterType;
import cz.cas.lib.arcstorage.storage.ceph.CephS3StorageService;
import cz.cas.lib.arcstorage.storage.fs.FsStorageService;
import cz.cas.lib.arcstorage.storage.fs.ZfsStorageService;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static cz.cas.lib.arcstorage.util.Utils.*;

@Slf4j
public class StorageUtils {

    public static String keyFilePath;

    public static StorageService createAdapter(StorageConfig storageConfig) throws ConfigParserException {
        switch (storageConfig.getStorageType()) {
            case FS:
                return new FsStorageService(storageConfig);
            case ZFS:
                return new ZfsStorageService(storageConfig);
            case CEPH:
                JsonNode root;
                try {
                    root = new ObjectMapper().readTree(storageConfig.getConfig());
                } catch (IOException e) {
                    throw new ConfigParserException(e);
                }
                CephAdapterType cephAdapterType = parseEnumFromConfig(root, "/adapterType", CephAdapterType.class);
                String userKey = root.at("/userKey").textValue();
                String userSecret = root.at("/userSecret").textValue();
                switch (cephAdapterType) {
                    case S3:
                        String region = root.at("/region").textValue();
                        if (userKey == null || userSecret == null)
                            throw new ConfigParserException("userKey or userSecret string missing in storage config");
                        return new CephS3StorageService(storageConfig, userKey, userSecret, region);
                    case SWIFT:
                        throw new UnsupportedOperationException();
                    case LIBRADOS:
                        throw new UnsupportedOperationException();
                }
                break;
        }
        return null;
    }

    /**
     * @param fileStream   which is closed by this method
     * @param checksumType
     * @return
     */
    public static Checksum computeChecksum(InputStream fileStream, ChecksumType checksumType) {
        MessageDigest complete = checksumComputationPrecheck(fileStream, checksumType);
        try (BufferedInputStream bis = new BufferedInputStream(fileStream)) {
            byte[] buffer = new byte[8192];
            int numRead;
            do {
                numRead = bis.read(buffer);
                if (numRead > 0) {
                    complete.update(buffer, 0, numRead);
                }
            } while (numRead != -1);
            return new Checksum(checksumType, bytesToHexString(complete.digest()));
        } catch (IOException e) {
            log.error("unable to compute hash", e);
            throw new GeneralException("unable to compute hash", e);
        }
    }

    public static MessageDigest checksumComputationPrecheck(InputStream fileStream, ChecksumType checksumType) {
        notNull(fileStream, () -> {
            throw new IllegalArgumentException();
        });
        try {
            switch (checksumType) {
                case MD5:
                    return MessageDigest.getInstance("MD5");
                case SHA512:
                    return MessageDigest.getInstance("SHA-512");
                default:
                    throw new GeneralException("unsupported checksum type: " + checksumType);
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isLocalhost(StorageConfig config) {
        return config.getHost().equals("localhost") || config.getHost().equals("127.0.0.1");
    }
}
