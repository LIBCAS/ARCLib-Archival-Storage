package cz.cas.lib.arcstorage.storage;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static cz.cas.lib.arcstorage.storage.StorageUtils.checksumComputationPrecheck;
import static cz.cas.lib.arcstorage.util.Utils.bytesToHexString;

/**
 * Interface to be implemented by storage service adapters managing custom types of the logical storages.
 * Implementation <b>must</b> saveAip objects in that way that later it is possible to retrieve:
 * <ul>
 * <li>initial checksum of the object</li>
 * <li>creation time of the object</li>
 * <li>state of object matching {@link ObjectState}, except ARCHIVAL_FAILURE state which signalizes fail of storage and thus is not retrievable (thus objects marked as ARCHIVAL_FAILURE in database may not exist on storage or have PROCESSING state on storage)</li>
 * <li>for AIP XML its version and ID of SIP</li>
 * </ul>
 * The implementation class should specify how it fulfill this in the javadoc.
 * <p>
 * When accessing AIP XML separately (e.g. AIP XML update, AIP XML retrieval) it is treated as general object.
 * </p>
 */
public interface StorageService {

    Storage getStorage();


    /**
     * Stores Aip objects into storage.
     * <p>
     * If rollback is set to true by another thread, this method instance must stop computation/uploading as soon as possible.
     * In this case, throwing exception is optional and is better to avoid, so that the log does not contain exceptions from all threads even if just the first one which set rollback to true is known to be the error one.
     * </p>
     * <p>
     * If object can't be stored, checksum can't be computed or does not match, this method instance must set rollback to true and throw exception, so that other threads can follow the routine described above.
     * </p>
     * <p>
     * If the object already exists it will be overwritten.
     * </p>
     * <p>
     * This operation may take a while and therefore sets object state to PROCESSING when it starts. It is expected that calling service will also do two-phase state update i.e. set DB state to PROCESSING before calling this method and to desired state after the method is doneInThisPhase.
     * </p>
     *
     * @param aipDto    DTO with open and readable input streams of objects
     * @param rollback  flag watched for rollback signal
     * @param dataSpace
     * @throws StorageException in the case of error
     */
    void storeAip(AipDto aipDto, AtomicBoolean rollback, String dataSpace) throws StorageException;

    /**
     * Retrieves reference to AIP objects. Caller is responsible for calling {@link AipRetrievalResource#close()} once the streams are not needed anymore.
     *
     * @param aipId       id of AIP
     * @param xmlVersions specifies which XML versions should be retrieved, typically all or the latest only
     * @return {@link AipRetrievalResource} with opened object streams of SIP and XMLs, XMLs are grouped by the version
     * @throws StorageException in the case of error
     */
    AipRetrievalResource getAip(String aipId, String dataSpace, Integer... xmlVersions) throws StorageException;

    /**
     * Stores object into storage.
     * <p>
     * If rollback is set to true by another thread, this method instance must stop computation as soon as possible.
     * In this case, throwing exception is optional and is better to avoid, so that the log does not contain exceptions from all threads even if just the first one which set rollback to true is known to be the error one.
     * </p>
     * <p>
     * If object can't be stored, sipStorageChecksum can't be computed or does not match, this method instance must set rollback to true and throw exception, so that other threads can follow the routine described above.
     * </p>
     * <p>
     * If the object already exists it will be overwritten.
     * </p>
     * <p>
     * The operation doneInThisPhase by the storage depends on the {@link ArchivalObjectDto#state} value:
     * <ul>
     * <li>{@link ObjectState#DELETED},{@link ObjectState#DELETION_FAILURE} or {@link ObjectState#ROLLED_BACK} or NULL -> only the metadata about this state is stored.</li>
     * <li>{@link ObjectState#PROCESSING} or {@link ObjectState#ARCHIVED} -> the data and metadata are stored. For both these initial states, the final state stored on the storage is {@link ObjectState#ARCHIVED} (supposed the operation was successful).</li>
     * <li>{@link ObjectState#REMOVED} -> the data and metadata are stored.</li>
     * <li>{@link ObjectState#ARCHIVAL_FAILURE} -> {@link IllegalArgumentException} is thrown</li>
     * </ul>
     * </p>
     *
     * @param objectDto DTO with open and readable input stream of the object
     * @param rollback  flag watched for rollback signal
     * @throws StorageException         in the case of error
     * @throws IllegalArgumentException if the object to be stored has {@link ObjectState#ARCHIVAL_FAILURE}
     */
    void storeObject(ArchivalObjectDto objectDto, AtomicBoolean rollback, String dataSpace) throws StorageException;

    /**
     * Retrieves reference to the object. Caller is responsible for calling {@link ObjectRetrievalResource#close()} once the streams are not needed anymore.
     *
     * @param id of the object
     * @return {@link ObjectRetrievalResource} with opened stream of object
     * @throws StorageException in the case of error
     */
    ObjectRetrievalResource getObject(String id, String dataSpace) throws StorageException;

    /**
     * Deletes SIP object from storage. Must not fail if SIP is already physically deleted.
     * <p>
     * This operation may take a while and therefore sets object state to PROCESSING when it starts. It is expected that calling service will also do two-phase state update i.e. set state to PROCESSING before calling this method and to desired state after the method is doneInThisPhase.
     * </p>
     *
     * @param id of the SIP
     * @throws StorageException in the case of error
     */
    void delete(String id, String dataSpace) throws StorageException;

    /**
     * Logically removes SIP. Must not fail if SIP is already removed.
     *
     * @param id of the SIP
     * @throws StorageException in the case of error
     */
    void remove(String id, String dataSpace) throws StorageException;

    /**
     * Renews logically removed SIP. Must not fail if SIP is already renewed, i.e. in ARCHIVED state.
     *
     * @param id of the SIP
     * @throws StorageException in the case of error
     */
    void renew(String id, String dataSpace) throws StorageException;

    /**
     * Rollbacks AIP and its first XML object from storage. Used in case of cleaning process after storage/application failure.
     * <p>
     * In any case (object not found / already rolled back / object which was never actually stored / inconsistent ...) this method has to set ROLLED_BACK state in metadata and delete the object (if exists).
     * </p>
     * <p>
     * This operation may take a while and therefore sets object state to PROCESSING when it starts.
     * </p>
     *
     * @param sipId if of the SIP
     * @throws StorageException in the case of error
     */
    void rollbackAip(String sipId, String dataSpace) throws StorageException;

    /**
     * Rollbacks object from storage. Used in case of cleaning process after storage/application failure.
     * <p>
     * In any case (object not found / already rolled back / object which was never actually stored / inconsistent ...) this method has to set ROLLED_BACK state in metadata and delete the object (if exists).
     * </p>
     * <p>
     * This operation may take a while and therefore sets object state to PROCESSING when it starts.
     * </p>
     *
     * @param id of the object
     * @throws StorageException in the case of error
     */
    void rollbackObject(String id, String dataSpace) throws StorageException;

    /**
     * Retrieves information about AIP such as its state etc. and also info about SIP and XMLs checksums.
     *
     * @param aipId       id of the AIP
     * @param sipChecksum expected checksum to be compared with storage checksum to verify fixity
     * @param objectState state of the AIP in database (it is used to getAip clue if it make sense to look for the fixity of the object e.g. when it is deleted)
     * @param xmlVersions map of xml version numbers and their expected checksums
     * @return AipStateInfoDto object
     * @throws StorageException in the case of error
     */
    AipStateInfoDto getAipInfo(String aipId, Checksum sipChecksum, ObjectState objectState, Map<Integer, Checksum> xmlVersions, String dataSpace) throws StorageException;

    /**
     * Returns state of currently used storage.
     *
     * @return StorageStateDto
     * @throws StorageException in the case of error
     */
    StorageStateDto getStorageState() throws StorageException;

    /**
     * Quick test if storage is reachable.
     *
     * @return true if storage is reachable, false otherwise
     */
    boolean testConnection();

    /**
     * For a newly created user creates a separate space withing which all its data are stored, eg. separate folder or bucket.
     */
    void createNewDataSpace(String dataSpace) throws IOStorageException;

    /**
     * Verifies sipStorageChecksum. IMPORTANT: returns true if sipStorageChecksum matches but throws exception when it does not. False is returned when the computation is interrupted by rollback flag.
     * <p>
     * If there is an error during computation or checksums do not match, rollback flag is set to true.
     * </p>
     *
     * @param objectStream     opened stream of object to verify
     * @param expectedChecksum checksum used for comparison
     * @param rollback         flag watched for rollback signal (if set by other thread, the computation ends to do not waste resources)
     * @return True if sipStorageChecksum matches but throws exception when it does not. False is returned when the computation is interrupted by rollback flag.
     * @throws IOStorageException               in case of any {@link IOException}
     * @throws FileCorruptedAfterStoreException in case the verification fails
     */
    default boolean verifyChecksum(InputStream objectStream, Checksum expectedChecksum, AtomicBoolean rollback) throws IOStorageException, FileCorruptedAfterStoreException {
        try {
            Checksum checksum = null;
            checksum = computeChecksumRollbackAware(objectStream, expectedChecksum.getType(), rollback);
            if (checksum == null)
                return false;
            if (!checksum.getValue().equalsIgnoreCase(expectedChecksum.getValue())) {
                rollback.set(true);
                throw new FileCorruptedAfterStoreException(checksum, expectedChecksum);
            }
            return true;
        } catch (IOException e) {
            rollback.set(true);
            throw new IOStorageException("error occured while computing sipStorageChecksum", e);
        } catch (Exception e) {
            if (e instanceof FileCorruptedAfterStoreException)
                throw e;
            rollback.set(true);
            throw new GeneralException(e);
        }
    }

    /**
     * Computes checksum using buffer.
     *
     * @param objectStream stream
     * @param checksumType checksum type
     * @param rollback     flag watched for rollback signal (if set by other thread, the computation ends to do not waste resources)
     * @return checksum of the stream or null, if rollback flag was set to true by another thread
     * @throws IOException in case of error
     */
    default Checksum computeChecksumRollbackAware(InputStream objectStream, ChecksumType checksumType, AtomicBoolean rollback) throws IOException {
        MessageDigest complete = checksumComputationPrecheck(objectStream, checksumType);
        try (BufferedInputStream bis = new BufferedInputStream(objectStream)) {
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
}
