package cz.cas.lib.arcstorage.storage.shared;

import cz.cas.lib.arcstorage.domain.ChecksumType;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.gateway.dto.Checksum;
import cz.cas.lib.arcstorage.storage.FsStorageService;
import cz.cas.lib.arcstorage.storage.StorageService;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;

import static cz.cas.lib.arcstorage.util.Utils.bytesToHexString;
import static cz.cas.lib.arcstorage.util.Utils.notNull;

@Slf4j
public class StorageUtils {

    public static String keyFilePath;

    public static StorageService createAdapter(StorageConfig storageConfig) {
        switch (storageConfig.getStorageType()) {
            case FS:
                return new FsStorageService(storageConfig);
        }
        return null;
    }

    public static Checksum computeChecksum(InputStream fileStream, ChecksumType checksumType) {
        MessageDigest complete = checksumComputationPrecheck(fileStream, checksumType);
        try (BufferedInputStream bis = new BufferedInputStream(fileStream)) {
            byte[] buffer = new byte[1024];
            int numRead;
            do {
                numRead = bis.read(buffer);
                if (numRead > 0) {
                    complete.update(buffer, 0, numRead);
                }
            } while (numRead != -1);
            return new Checksum(checksumType, bytesToHexString(complete.digest()));
        } catch (FileNotFoundException e) {
            return null;
        } catch (IOException e) {
            log.error("unable to compute hash", e);
            throw new GeneralException("unable to compute hash", e);
        }
    }

    public static Checksum computeChecksum(InputStream fileStream, ChecksumType checksumType, AtomicBoolean rollback) {
        MessageDigest complete = checksumComputationPrecheck(fileStream, checksumType);
        try (BufferedInputStream bis = new BufferedInputStream(fileStream)) {
            byte[] buffer = new byte[1024];
            int numRead;
            do {
                if (rollback.get())
                    return null;
                numRead = bis.read(buffer);
                if (numRead > 0) {
                    complete.update(buffer, 0, numRead);
                }
            } while (numRead != -1);
            return new Checksum(checksumType, bytesToHexString(complete.digest()));
        } catch (FileNotFoundException e) {
            return null;
        } catch (IOException e) {
            log.error("unable to compute hash", e);
            throw new GeneralException("unable to compute hash", e);
        }
    }

    private static MessageDigest checksumComputationPrecheck(InputStream fileStream, ChecksumType checksumType) {
        notNull(fileStream, () -> {
            throw new IllegalArgumentException();
        });
        try {
            switch (checksumType) {
                case MD5:
                    return MessageDigest.getInstance("MD5");
                case SHA_512:
                    return MessageDigest.getInstance("SHA-512");
                default:
                    throw new GeneralException("unsupported checksum type: " + checksumType);
            }
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toXmlId(String sipId, int version) {
        return sipId + "_xml_" + version;
    }
}
