package cz.cas.lib.arcstorage.storage;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.service.ArchivalService;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.storage.fs.ObjectMetadata;
import cz.cas.lib.arcstorage.storagesync.ObjectAudit;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static cz.cas.lib.arcstorage.storage.StorageUtils.checksumComputationPrecheck;
import static cz.cas.lib.arcstorage.util.Utils.bytesToHexString;

/**
 * Interface to be implemented by storage service adapters managing custom types of the logical storages.
 * Implementation <b>must</b> saveAip objects in that way that later it is possible to retrieve:
 * <ul>
 * <li>initial checksum of the object</li>
 * <li>creation time of the object</li>
 * <li>state of object matching {@link ObjectState}, except states with {@link ObjectState#metadataMustBeStoredAtLogicalStorage} = false, e.g. {@link ObjectState#ARCHIVAL_FAILURE} which signalizes fail of storage and thus is not retrievable (objects marked with such state in database may not exist on storage or have PROCESSING state on storage)</li>
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
     * Retrieves archival objects data from the dataSpace based on their physical representations in the storage
     * <p>
     * The order is important - if there are any XMLs, first its SIP must appear in a list
     * </p>
     * <p>
     * Implementation must fill {@link ArchivalObjectDto#objectType}
     * </p>
     *
     * @param dataSpace dataSpace
     * @return archival objects retrieved
     * @throws StorageException
     */
    List<ArchivalObjectDto> createDtosForAllObjects(String dataSpace) throws StorageException;

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
     * This operation may take a while and therefore sets object state to PROCESSING when it starts. It is expected that calling service will also do two-phase state update i.e. set DB state to PROCESSING before calling this method and to desired state after the method is done.
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
     * The operation done by the storage depends on the {@link ArchivalObjectDto#state} value:
     * <ul>
     * <li>{@link ObjectState#DELETED},{@link ObjectState#DELETION_FAILURE}, {@link ObjectState#ARCHIVAL_FAILURE}, {@link ObjectState#ROLLED_BACK}, {@link ObjectState#ROLLBACK_FAILURE} or NULL -> only the metadata about this state is stored.</li>
     * <li>{@link ObjectState#PROCESSING} or {@link ObjectState#ARCHIVED} -> the data and metadata are stored. For both these initial states, the final state stored on the storage is {@link ObjectState#ARCHIVED} (supposed the operation was successful).</li>
     * <li>{@link ObjectState#REMOVED} -> the data and metadata are stored.</li>
     * </ul>
     * </p>
     *
     * @param objectDto DTO with open and readable input stream of the object
     * @param rollback  flag watched for rollback signal
     * @throws StorageException in the case of error
     */
    void storeObject(ArchivalObjectDto objectDto, AtomicBoolean rollback, String dataSpace) throws StorageException;

    /**
     * Stores only metadata of the object.
     *
     * @param objectDto
     */
    void storeObjectMetadata(ArchivalObjectDto objectDto, String dataSpace) throws StorageException;

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
     *
     * @param objectDto               of the SIP
     * @param createMetaFileIfMissing if true and .meta file is missing then it is created, otherwise exception is thrown
     * @throws StorageException in the case of error
     */
    void delete(ArchivalObjectDto objectDto, String dataSpace, boolean createMetaFileIfMissing) throws StorageException;

    /**
     * Logically removes SIP. Must not fail if SIP is already removed.
     *
     * @param objectDto               of the SIP
     * @param createMetaFileIfMissing if true and .meta file is missing then it is created, otherwise exception is thrown
     * @throws StorageException in the case of error
     */
    void remove(ArchivalObjectDto objectDto, String dataSpace, boolean createMetaFileIfMissing) throws StorageException;

    /**
     * Renews logically removed SIP. Must not fail if SIP is already renewed, i.e. in ARCHIVED state.
     *
     * @param objectDto               of the SIP
     * @param createMetaFileIfMissing if true and .meta file is missing then it is created, otherwise exception is thrown
     * @throws StorageException in the case of error
     */
    void renew(ArchivalObjectDto objectDto, String dataSpace, boolean createMetaFileIfMissing) throws StorageException;

    /**
     * Rollbacks AIP and all its XML object from storage. Used in case of cleaning process after storage/application failure.
     * <p>
     * In any case (object not found / already rolled back / object which was never actually stored / inconsistent ...) this method has to set ROLLED_BACK state in metadata and delete the object (if exists).
     * </p>
     *
     * @param aipDto dto, input streams may be null
     * @throws StorageException in the case of error
     */
    void rollbackAip(AipDto aipDto, String dataSpace) throws StorageException;

    /**
     * Rolls back object from storage. Used in case of cleaning process after storage/application failure.
     * <p>
     * In any case (object not found / already rolled back / object which was never actually stored / inconsistent ...) this method has to set ROLLED_BACK state in metadata and delete the object (if exists).
     * </p>
     *
     * @param objectDto dto, input stream may be null
     * @throws StorageException in the case of error
     */
    void rollbackObject(ArchivalObjectDto objectDto, String dataSpace) throws StorageException;

    /**
     * Forgets object at storage - completely deletes the object and leaves only metadata files at the storage.
     * Must not fail if the object is already forgot.
     * <p>
     * In this case, from all the {@link ObjectMetadata} only {@link ObjectMetadata#state} is guaranteed to be present
     * at the storage. Other metadata are optionally present.
     * </p>
     *
     * @param objectIdAtStorage    id of the object
     * @param objectAuditTimestamp <p>Should be set to null during standard forget calls made by {@link ArchivalService}
     *                             in which case this call deletes data and marks forget state no matter what state was marked
     *                             before. Exception is thrown if no state was marked before (meta object does not exist).</p>
     *                             <p>During calls which are propagating some historical modification from audit the
     *                             {@link ObjectAudit#created} should be filled. Storage service should compare the timestamp
     *                             with the creation timestamp marked with the object in metaobject. If the audit timestamp is higher
     *                             the object should be forgotten, otherwise it should be left untouched. If there is no metaobject
     *                             at all, then the object should be also forgotten.
     *                             <p>The reason for this design is that after the object is forgotten, other object may be persisted
     *                             with the same name as the object which was previously forgotten and must not forgot the new object based
     *                             on audit of forgot operation of the old one. This can typically happen with {@link AipXml} which name at
     *                             storage is determined from the related {@link AipSip} and XML version. If one XML version is forgotten then
     *                             the next one will have the same version number and thus the same place at the storage.
     * @throws StorageException in the case of error
     */
    void forgetObject(String objectIdAtStorage, String dataSpace, Instant objectAuditTimestamp) throws StorageException;

    /**
     * Retrieves information about AIP such as its state etc. and also info about SIP and XMLs checksums.
     *
     * @param aip  filled up with aip properties from DB
     * @param xmls list of xml version numbers with XML {@link ArchivalObjectDto}s as values
     * @return AipStateInfoDto object
     * @throws StorageException in the case of error, e.g. if the file could not be found
     */
    AipConsistencyVerificationResultDto getAipInfo(ArchivalObjectDto aip, Map<Integer, ArchivalObjectDto> xmls, String dataSpace) throws StorageException;

    /**
     * Verifies that the storage contains valid state metadata objects for all input objects. If verification of some object fail, immediately returns.
     * <br>
     * Counter is incremented after every verification.
     * <br>
     * Implementation should skip verification of objects with state which has {@link ObjectState#metadataMustBeStoredAtLogicalStorage}=false, while incrementing the counter.
     *
     * @return null if verification succeeded, first object for which the verification didn't succeed otherwise
     * @throws StorageException in the case of error
     */
    ArchivalObjectDto verifyStateOfObjects(List<ArchivalObjectDto> expectedStates, AtomicLong counter) throws StorageException;

    /**
     * Returns state of currently used storage. May fail if storage is unreachable. The storage state data should contain at least:
     * <ul>
     *     <li>health of the storage and its parts</li>
     *     <li>memory info (free, used etc.)</li>
     *     <li>self-healing (scrubbing) info (if supported)</li>
     * </ul>
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
    default boolean verifyChecksum(InputStream objectStream, Checksum expectedChecksum, AtomicBoolean rollback, Storage storage) throws IOStorageException, FileCorruptedAfterStoreException {
        try {
            Checksum checksum = null;
            checksum = computeChecksumRollbackAware(objectStream, expectedChecksum.getType(), rollback);
            if (checksum == null)
                return false;
            if (!checksum.getValue().equalsIgnoreCase(expectedChecksum.getValue())) {
                rollback.set(true);
                throw new FileCorruptedAfterStoreException(checksum, expectedChecksum, storage);
            }
            return true;
        } catch (IOException e) {
            rollback.set(true);
            throw new IOStorageException("error occured while computing sipStorageChecksum", e, storage);
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
