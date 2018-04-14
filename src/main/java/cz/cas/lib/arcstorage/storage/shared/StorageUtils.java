package cz.cas.lib.arcstorage.storage.shared;

import cz.cas.lib.arcstorage.domain.ChecksumType;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.gateway.dto.Checksum;
import cz.cas.lib.arcstorage.storage.FsStorageService;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.FileDoesNotExistException;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
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

    /**
     * @param fileStream   which is closed by this method
     * @param checksumType
     * @return
     * @throws FileDoesNotExistException
     */
    public static Checksum computeChecksum(InputStream fileStream, ChecksumType checksumType) throws FileDoesNotExistException {
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
            throw new FileDoesNotExistException();
        } catch (IOException e) {
            log.error("unable to compute hash", e);
            throw new GeneralException("unable to compute hash", e);
        }
    }

    /**
     * Computes checksum.
     * If rollback is set to true by another thread, immediately stops computation and returns null.
     * If checksum cant be computed, throws exception and sets rollback to true.
     *
     * @param fileStream   which is closed by this method
     * @param checksumType
     * @param rollback
     * @return
     * @throws FileDoesNotExistException
     * @throws IOStorageException
     */
    public static Checksum computeChecksum(InputStream fileStream, ChecksumType checksumType, AtomicBoolean rollback) throws FileDoesNotExistException, IOStorageException {
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
            rollback.set(true);
            throw new FileDoesNotExistException(e);
        } catch (IOException e) {
            rollback.set(true);
            throw new IOStorageException("unable to compute hash", e);
        } catch (Exception e) {
            rollback.set(true);
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
