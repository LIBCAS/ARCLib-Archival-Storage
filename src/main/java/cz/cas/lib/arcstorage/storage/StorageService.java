package cz.cas.lib.arcstorage.storage;

import cz.cas.lib.arcstorage.domain.AipState;
import cz.cas.lib.arcstorage.domain.ChecksumType;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.domain.XmlState;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import cz.cas.lib.arcstorage.storage.exception.FileDoesNotExistException;
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
 * <li>initial MD5 storageChecksum of file</li>
 * <li>creation time of file</li>
 * <li>state of file matching {@link AipState} or {@link XmlState}, except FAILED state which signalizes fail of storage and thus is not retrievable (thus files marked as FAILED in database may not exist on storage or have PROCESSING state on storage)</li>
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
     * If file can't be stored, storageChecksum can't be computed or does not match, this method instance must set rollback to true and throw exception, so that other threads can follow the routine described above.
     * </p>
     * <p>
     * If the file already exists it will be overwritten.
     * </p>
     * <p>
     * This operation may take a while and therefore sets file state to PROCESSING when it starts. It is expected that calling service will also do two-phase state update i.e. set state to PROCESSING before calling this method and to desired state after the method is done.
     * </p>
     *
     * @throws StorageException
     */
    void storeAip(AipRef aipRef, AtomicBoolean rollback) throws StorageException;

    /**
     * Retrieves reference to Aip files. Caller is responsible for calling {@link FileRef#freeSources()} once the stream is not needed anymore.
     *
     * @param sipId
     * @param xmlVersions specifies which XML versions should be retrieved, typically all or the latest only
     * @return list with opened file streams where first item is SIP inputStream and others are XML streams in the same order as was passed in {@code xmlVersions} parameter
     * @throws StorageException
     * @throws cz.cas.lib.arcstorage.storage.exception.FileDoesNotExistException
     */
    List<FileRef> getAip(String sipId, Integer... xmlVersions) throws FileDoesNotExistException, StorageException;

    /**
     * Stores XML files into storage.
     * <p>
     * If rollback is set to true by another thread, this method instance must stop computation as soon as possible.
     * In this case, throwing exception is optional and is better to avoid, so that the log does not contain exceptions from all threads even if just the first one which set rollback to true is known to be the error one.
     * </p>
     * <p>
     * If file can't be stored, storageChecksum can't be computed or does not match, this method instance must set rollback to true and throw exception, so that other threads can follow the routine described above.
     * </p>
     * <p>
     * If the file already exists it will be overwritten.
     * </p>
     * * <p>
     * This operation may take a while and therefore sets file state to PROCESSING when it starts. It is expected that calling service will also do two-phase state update i.e. set state to PROCESSING before calling this method and to desired state after the method is done.
     * </p>
     */
    void storeXml(String sipId, XmlRef xmlRef, AtomicBoolean rollback) throws StorageException;

    /**
     * Retrieves reference to AipXml file. Caller is responsible for calling {@link FileRef#freeSources()} once the stream is not needed anymore.
     *
     * @param sipId
     * @param version
     * @return file stream
     * @throws StorageException
     * @throws cz.cas.lib.arcstorage.storage.exception.FileDoesNotExistException
     */
    FileRef getXml(String sipId, int version) throws FileDoesNotExistException, StorageException;

    /**
     * Deletes SIP file from storage. Must not fail if SIP package is already physically deleted.
     * <p>
     * This operation may take a while and therefore sets file state to PROCESSING when it starts. It is expected that calling service will also do two-phase state update i.e. set state to PROCESSING before calling this method and to desired state after the method is done.
     * </p>
     *
     * @param id
     * @throws StorageException
     * @throws cz.cas.lib.arcstorage.storage.exception.FileDoesNotExistException if metadata of SIP does not exist
     */
    void deleteSip(String id) throws StorageException;

    /**
     * Logically removes SIP. Must not fail if SIP is already removed.
     *
     * @param id
     * @throws StorageException
     * @throws cz.cas.lib.arcstorage.storage.exception.FileDoesNotExistException if metadata of SIP does not exist
     */
    void remove(String id) throws StorageException;

    /**
     * Rollbacks AIP and its first XML file from storage. Used only in case of cleaning process after storage/application failure.
     * <p>
     * In any case (file not found / already rollbacked / file which was never actually stored / inconsistent ...) this method has to set ROLLBACKED state in metadata and delete the file (if exists).
     * </p>
     * <p>
     * This operation may take a while and therefore sets file state to PROCESSING when it starts. It is expected that calling service will also do two-phase state update i.e. set state to PROCESSING before calling this method and to desired state after the method is done.
     * </p>
     *
     * @param sipId
     * @throws StorageException
     */
    void rollbackAip(String sipId) throws StorageException;

    /**
     * Rollbacks XML file from storage. Used only in case of cleaning process after storage/application failure.
     * <p>
     * In any case (file not found / already rollbacked / file which was never actually stored / inconsistent ...) this method has to set ROLLBACKED state in metadata and delete the file (if exists).
     * </p>
     * <p>
     * This operation may take a while and therefore sets file state to PROCESSING when it starts. It is expected that calling service will also do two-phase state update i.e. set state to PROCESSING before calling this method and to desired state after the method is done.
     * </p>
     *
     * @param sipId
     * @param version
     * @throws StorageException
     */
    void rollbackXml(String sipId, int version) throws StorageException;

    /**
     * Retrieves information about AIP such as its state etc. and also info about SIP and XMLs fixity.
     *
     * @param sipId
     * @param sipChecksum expected storageChecksum to be compared with storage storageChecksum to verify fixity
     * @param aipState    state of the AIP in database (it is used to get clue if it make sense to look for the fixity of the file e.g. when it is deleted)
     * @param xmlVersions map of xml version numbers and their expected checksums
     * @return AipStateInfo object
     * @throws StorageException
     * @throws cz.cas.lib.arcstorage.storage.exception.FileDoesNotExistException if the file is missing and thus its fixity can't be verified
     */
    AipStateInfo getAipInfo(String sipId, Checksum sipChecksum, AipState aipState, Map<Integer, Checksum> xmlVersions) throws StorageException;

    /**
     * Returns state of currently used storage.
     *
     * @return
     */
    StorageState getStorageState() throws StorageException;

    /**
     * Tests if storage is reachable.
     *
     * @return true if storage is reachable, false otherwise
     */
    boolean testConnection();

    /**
     * Verifies storageChecksum. IMPORTANT: returns true if storageChecksum matches but throws exception when it does not. False is returned when the computation is interrupted by rollback flag.
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
            throw new IOStorageException("error occured while computing storageChecksum", e);
        } catch (Exception e) {
            rollback.set(true);
            throw new GeneralException(e);
        }
    }

    /**
     * Computes storageChecksum using buffer.
     *
     * @param fileStream   stream
     * @param checksumType storageChecksum type
     * @param rollback     rollback flag
     * @return storageChecksum of the stream or null, if rollback flag was set to true by another thread
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
