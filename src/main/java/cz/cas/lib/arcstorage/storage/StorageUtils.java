package cz.cas.lib.arcstorage.storage;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ChecksumType;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.service.exception.InvalidChecksumException;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cz.cas.lib.arcstorage.util.Utils.bytesToHexString;
import static cz.cas.lib.arcstorage.util.Utils.notNull;

@Slf4j
public class StorageUtils {

    /**
     * Computes checksum of the given type for the file.
     *
     * @param fileStream   which is closed by this method
     * @param checksumType type of checksum to compute
     * @return computed checksum
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
            log.error("unable to compute value", e);
            throw new GeneralException("unable to compute value", e);
        }
    }

    /**
     * Reads inputstream, writes it to ouptutstream and computes checksum of the stream during the copy process.
     *
     * @param inputStream  stream to be copied
     * @param outputStream stream to which should be the input copied
     * @param checksumType type of checksum to compute
     * @return computed checksum
     */
    public static Checksum copyStreamAndComputeChecksum(InputStream inputStream, OutputStream outputStream, ChecksumType checksumType) {
        MessageDigest checksum = checksumComputationPrecheck(inputStream, checksumType);

        try (BufferedInputStream bis = new BufferedInputStream(inputStream);
             BufferedOutputStream bos = new BufferedOutputStream(outputStream)) {
            byte[] buffer = new byte[8192];
            int numRead;
            do {
                numRead = bis.read(buffer);
                if (numRead > 0) {
                    checksum.update(buffer, 0, numRead);
                    bos.write(buffer, 0, numRead);
                }
            } while (numRead != -1);
            return new Checksum(checksumType, bytesToHexString(checksum.digest()));
        } catch (IOException e) {
            log.error("unable to compute value", e);
            throw new GeneralException("unable to compute value", e);
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

    public static boolean isLocalhost(Storage storage) {
        return storage.getHost().equals("localhost") || storage.getHost().equals("127.0.0.1");
    }

    public static void validateChecksum(Checksum checksum, Path tmpSipPath) throws IOException {
        try (BufferedInputStream fis = new BufferedInputStream(new FileInputStream(tmpSipPath.toString()))) {
            Checksum computedChecksum = StorageUtils.computeChecksum(fis, checksum.getType());
            if (!checksum.equals(computedChecksum)) {
                throw new InvalidChecksumException(computedChecksum, checksum);
            }
        }
    }

    public static void validateChecksum(Checksum checksum, InputStream inputStream) throws InvalidChecksumException {
        Checksum computedChecksum = StorageUtils.computeChecksum(inputStream, checksum.getType());
        if (!checksum.equals(computedChecksum)) {
            throw new InvalidChecksumException(computedChecksum, checksum);
        }
    }

    public static String toXmlId(String sipId, int version) {
        return sipId + "_xml_" + version;
    }

    public static int extractXmlVersion(String xmlStorageId) {
        Matcher matcher = Pattern.compile("\\w+_xml_(\\d+)").matcher(xmlStorageId);
        if (!matcher.find())
            throw new GeneralException("trying to extract XML version from string which is not valid XML storageId");
        return Integer.parseInt(matcher.group(1));
    }
}
