package cz.cas.lib.arcstorage.storage;

import cz.cas.lib.arcstorage.domain.AipState;
import cz.cas.lib.arcstorage.domain.ChecksumType;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static cz.cas.lib.arcstorage.storage.StorageUtils.checksumComputationPrecheck;
import static cz.cas.lib.arcstorage.util.Utils.bytesToHexString;

/**
 * Implementation <b>must</b> store files in that way that later it is possible to retrieve:
 * <ul>
 * <li>initial MD5 checksum of file</li>
 * <li>creation time of file</li>
 * <li>info if file is being processed</li>
 * <li>info when file processing has failed</li>
 * <li>for SIP its ID and info if is {@link cz.cas.lib.arcstorage.domain.AipState#DELETED} or {@link cz.cas.lib.arcstorage.domain.AipState#REMOVED}</li>
 * <li>for XML its version and ID of SIP</li>
 * </ul>
 */

public interface StorageService {

    StorageConfig getStorageConfig();

    /**
     * Stores Aip files into storage.
     * <p>
     * If rollback is set to true by another thread, this method instance must stop computation as soon as possible.
     * In this case, throwing exception is optional and is better to avoid, so that the log does not contain exceptions from all threads even if just the first one which set rollback to true is known to be the error one.
     * </p>
     * <p>
     * If file can't be stored, checksum can't be computed or does not match, this method instance must set rollback to true and throw exception, so that other threads can follow the routine described above.
     * </p>
     *
     * @return Object containing MD5 checksums computed from stored files.
     * @throws IOException
     */
    void storeAip(AipRef aipRef, AtomicBoolean rollback) throws StorageException;

    /**
     * Retrieves references to Aip files. Caller is responsible for closing retrieved streams.
     *
     * @param sipId
     * @param xmlVersions specifies which XML versions should be retrieved, typically all or the latest only
     * @return list with opened file streams where first item is SIP inputStream and others are XML streams in the same order as was passed in {@code xmlVersions} parameter
     * @throws IOException
     */
    List<FileRef> getAip(String sipId, Integer... xmlVersions) throws StorageException;

    /**
     * Stores XML files into storage.
     *
     * @return checksum computed from stored file
     * @throws IOException
     */
    void storeXml(String sipId, XmlRef xmlFileRef, AtomicBoolean rollback) throws StorageException;

    /**
     * Retrieves reference to AipXml file. Caller is responsible for closing retrieved inputStream.
     *
     * @param sipId
     * @param version
     * @return
     * @throws IOException
     */
    FileRef getXml(String sipId, int version) throws StorageException;

    /**
     * Deletes SIP file from storage. Must not fail if SIP is already deleted.
     *
     * @param id
     * @throws IOException
     */
    void deleteSip(String id) throws StorageException;

    /**
     * Logically removes SIP. Must not fail if SIP is already removed.
     *
     * @param id
     * @throws IOException
     */
    void remove(String id) throws StorageException;

    void rollbackAip(String sipId) throws StorageException;

    /**
     * Deletes XML file from storage. Used only in case of storage/application failure.
     * <p>
     * In case that file has been already deleted (rollbacked) do nothing.
     * </p>
     *
     * @param sipId
     * @param version
     * @throws IOException
     */
    void rollbackXml(String sipId, int version) throws StorageException;

    /**
     * Computes and retrieves MD5 checksum of SIP file.
     *
     * @param sipId
     * @return MD5 checksum of SIP file or null if file does not exist or is locked (e.g. accessed by other process)
     * @throws IOException
     */
    AipStateInfo getAipInfo(String sipId, Checksum sipChecksum, AipState aipState, Map<Integer, Checksum> xmlVersions) throws StorageException;

    /**
     * Returns state of currently used storage.
     *
     * @return
     */
    StorageState getStorageState() throws StorageException;

    /**
     * Verifies checksum. IMPORTANT: returns true if checksum matches but throws exception when it does not. False is returned when the computation is interrupted by rollback flag.
     * <p>
     * If there is an error during computation or checksums do not match, rollback flag is set to true.
     * </p>
     *
     * @param fileStream
     * @param expectedChecksum
     * @param rollback
     * @return
     * @throws FileCorruptedAfterStoreException if checksums does not match
     * @throws IOStorageException               in case of any {@link IOException}
     * @throws GeneralException                 in case of any unexpected error
     */
    default boolean verifyChecksum(InputStream fileStream, Checksum expectedChecksum, AtomicBoolean rollback) throws FileCorruptedAfterStoreException, IOStorageException {
        try {
            Checksum checksum = null;
            checksum = computeChecksumRollbackAware(fileStream, expectedChecksum.getType(), rollback);
            if (checksum == null)
                return false;
            if (!checksum.getHash().equalsIgnoreCase(expectedChecksum.getHash())) {
                rollback.set(true);
                throw new FileCorruptedAfterStoreException(checksum, expectedChecksum);
            }
            return true;
        } catch (IOException e) {
            rollback.set(true);
            throw new IOStorageException("error occured while computing checksum", e);
        } catch (Exception e) {
            rollback.set(true);
            throw new GeneralException(e);
        }
    }

    /**
     * Computes checksum using buffer.
     *
     * @param fileStream   stream
     * @param checksumType checksum type
     * @param rollback     rollback flag
     * @return checksum of the stream or null, if rollback flag was set to true by another thread
     * @throws IOException
     */
    default Checksum computeChecksumRollbackAware(InputStream fileStream, ChecksumType checksumType, AtomicBoolean rollback) throws IOException {
        MessageDigest complete = checksumComputationPrecheck(fileStream, checksumType);
        try (BufferedInputStream bis = new BufferedInputStream(fileStream)) {
            byte[] buffer = new byte[8192];
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
        }
    }

    default String toXmlId(String sipId, int version) {
        return sipId + "_xml_" + version;
    }

}
