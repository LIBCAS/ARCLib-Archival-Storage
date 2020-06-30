package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.state.*;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.ObjectCouldNotBeRetrievedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.util.ApplicationContextUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.storage.StorageUtils.copyStreamAndComputeChecksum;
import static cz.cas.lib.arcstorage.util.Utils.asList;
import static cz.cas.lib.arcstorage.util.Utils.servicesToEntities;

/**
 * Service which provides methods for operations upon objects as general.
 * While the methods may internally perform different logic for {@link AipXml}, {@link AipSip} and {@link ArchivalObject}, the method
 * signatures remain transparent, hiding the differences of objects.
 */
@Service
@Slf4j
public class ArchivalService {

    private Path tmpFolder;
    private StorageProvider storageProvider;
    private ArcstorageMailCenter arcstorageMailCenter;
    private ArchivalDbService archivalDbService;
    private ArchivalAsyncService async;

    /**
     * Retrieves object if the object is in the allowed state or throws corresponding exception.
     *
     * @param objectDto dto with the object to retrieve
     * @return retrieved object
     * @throws FailedStateException
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     * @throws ObjectCouldNotBeRetrievedException
     * @throws NoLogicalStorageReachableException
     * @throws NoLogicalStorageAttachedException
     */
    public ObjectRetrievalResource getObject(ArchivalObjectDto objectDto) throws
            FailedStateException, RollbackStateException, StillProcessingStateException,
            ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        log.debug("Retrieving object with storage id " + objectDto.getStorageId() + ".");

        switch (objectDto.getState()) {
            case ROLLED_BACK:
            case ROLLBACK_FAILURE:
                throw new RollbackStateException(objectDto);
            case ARCHIVAL_FAILURE:
                throw new FailedStateException(objectDto);
            case PROCESSING:
            case PRE_PROCESSING:
                throw new StillProcessingStateException(objectDto);
        }
        ObjectRetrievalResource objectRef;
        try {
            objectRef = retrieveObject(objectDto);
        } catch (ObjectCouldNotBeRetrievedException e) {
            log.error("Storage error has occurred during retrieval process of object " + objectDto.getStorageId());
            throw e;
        }
        log.info("object " + objectDto.getStorageId() + " has been successfully retrieved.");
        return objectRef;
    }

    /**
     * Logically removes object.
     *
     * @param id id of the object to remove
     * @throws DeletedStateException
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     * @throws StorageException
     * @throws FailedStateException
     * @throws SomeLogicalStoragesNotReachableException
     * @throws ReadOnlyStateException
     * @throws NoLogicalStorageAttachedException
     */
    public void removeObject(String id) throws StillProcessingStateException, DeletedStateException,
            RollbackStateException, FailedStateException, SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException, StorageException {
        log.debug("Removing object with id " + id + ".");
        List<StorageService> reachableAdapters = storageProvider.createAdaptersForModifyOperation();
        ArchivalObject obj = archivalDbService.removeObject(id);
        async.removeObject(id, reachableAdapters, obj.getOwner().getDataSpace());
    }

    /**
     * Renews logically removed object.
     *
     * @param id id of the object to renew
     * @throws StorageException
     * @throws DeletedStateException
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     * @throws FailedStateException
     * @throws SomeLogicalStoragesNotReachableException
     * @throws NoLogicalStorageAttachedException
     * @throws ReadOnlyStateException
     */
    public void renewObject(String id) throws StillProcessingStateException, DeletedStateException,
            RollbackStateException, StorageException, FailedStateException, SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException {
        log.debug("Renewing object with id " + id + ".");
        List<StorageService> reachableAdapters = storageProvider.createAdaptersForModifyOperation();
        ArchivalObject archivalObject = archivalDbService.renewObject(id);
        async.renewObject(id, reachableAdapters, archivalObject.getOwner().getDataSpace());
    }

    /**
     * Physically removes object from storage. Data in transaction database are not removed.
     *
     * @param id id of the object to delete
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     * @throws FailedStateException
     * @throws SomeLogicalStoragesNotReachableException
     * @throws NoLogicalStorageAttachedException
     * @throws ReadOnlyStateException
     */
    public void deleteObject(String id) throws StillProcessingStateException, RollbackStateException,
            FailedStateException, SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException {
        log.debug("Deleting object with id " + id + ".");
        List<StorageService> reachableAdapters = storageProvider.createAdaptersForModifyOperation();
        ArchivalObject archivalObject = archivalDbService.deleteObject(id);
        async.deleteObject(archivalObject.toDto(), reachableAdapters);
    }

    /**
     * Rolls back object. If the requested object is {@link AipSip} then also all related {@link AipXml} are rolled back.
     *
     * @param objectToRollback
     * @throws StateException
     * @throws SomeLogicalStoragesNotReachableException
     * @throws NoLogicalStorageAttachedException
     * @throws ReadOnlyStateException
     */
    public void rollbackObject(ArchivalObject objectToRollback) throws
            SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException {
        String id = objectToRollback.getId();
        log.debug("Rolling back object with id " + id + ".");
        List<StorageService> reachableAdapters = storageProvider.createAdaptersForModifyOperation();
        List<ArchivalObject> objectsToRollback = new ArrayList<>();
        objectsToRollback.add(objectToRollback);
        if (objectToRollback instanceof AipSip) {
            List<AipXml> xmls = ((AipSip) objectToRollback).getXmls();
            objectsToRollback.addAll(xmls);
        }
        for (ArchivalObject objectInDb : objectsToRollback) {
            switch (objectInDb.getState()) {
                case ROLLED_BACK:
                    continue;
                case PROCESSING:
                case PRE_PROCESSING:
                    Pair<AtomicBoolean, Lock> rollbackFlag = ApplicationContextUtils.getProcessingObjects().get(id);
                    if (rollbackFlag != null) {
                        rollbackFlag.getRight().lock();
                        try {
                            objectInDb = archivalDbService.getObject(id);
                            //if object is still processing, just set rollback flag
                            if (objectInDb.getState() == ObjectState.PROCESSING || objectInDb.getState() == ObjectState.PRE_PROCESSING) {
                                rollbackFlag.getLeft().set(true);
                                continue;
                            }
                        } finally {
                            rollbackFlag.getRight().unlock();
                        }
                    } else {
                        objectInDb = archivalDbService.getObject(id);
                        //object can't be processing as it was not in processingObjects map
                        if (objectInDb.getState() == ObjectState.PROCESSING || objectInDb.getState() == ObjectState.PRE_PROCESSING) {
                            throw new GeneralException("Fatal error: object which has to be rolled back is not present in processing objects map" +
                                    "but in DB it is marked as " + objectInDb.getState() + ". This is unexpected state.");
                        }
                    }
                    //if object was processing and has just switched to archived, it continues with the following case (there is no break in above block)
                default:
                    archivalDbService.rollbackObject(objectInDb);
                    async.rollbackObject(objectInDb.toDto(), reachableAdapters);
            }
        }
    }

    /**
     * Retrieves object.
     * <p>
     * Storage is chosen randomly from those with highest priority. If the chose storage throws
     * {@link StorageException}, or checksum does not match, {@link #recoverObjectFromOtherStorages(ArchivalObjectDto, List, boolean)}
     * is called to scan through all storages until it finds the right one or throws {@link ObjectCouldNotBeRetrievedException} which
     * is propagated.
     *
     * @param archivalObject object from main request
     * @return {@link ObjectRetrievalResource} with valid object's input stream
     * @throws ObjectCouldNotBeRetrievedException if object is corrupted at the given storages
     */
    private ObjectRetrievalResource retrieveObject(ArchivalObjectDto archivalObject)
            throws ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        return retrieveObject(archivalObject, null);
    }

    /**
     * Retrieves object.
     * <p>
     * Storage is chosen randomly from those with highest priority. Those which are not to be used are not used. If the chose storage throws
     * {@link StorageException}, or checksum does not match, {@link #recoverObjectFromOtherStorages(ArchivalObjectDto, List, boolean)}
     * is called to scan through all storages until it finds the right one or throws {@link ObjectCouldNotBeRetrievedException} which
     * is propagated.
     *
     * @param archivalObject      object from main request
     * @param servicesNotToBeUsed services which should not be used for retrieval
     * @return {@link ObjectRetrievalResource} with valid object's input stream
     * @throws ObjectCouldNotBeRetrievedException if object is corrupted at the given storages
     */
    ObjectRetrievalResource retrieveObject(ArchivalObjectDto archivalObject, List<StorageService> servicesNotToBeUsed)
            throws ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        log.debug("Retrieving archival object with storage id " + archivalObject.getStorageId() + ".");

        List<StorageService> storageServicesByPriorities = storageProvider.createAdaptersForRead();
        if (servicesNotToBeUsed != null) {
            List<StorageService> storageServicesToBeUsed = new ArrayList<>();

            Set<Storage> setOfStoragesNotToBeUsed = servicesNotToBeUsed
                    .stream()
                    .map(StorageService::getStorage)
                    .collect(Collectors.toSet());

            for (StorageService storageService : storageServicesByPriorities) {
                if (setOfStoragesNotToBeUsed.contains(storageService.getStorage()))
                    continue;
                storageServicesToBeUsed.add(storageService);
            }
            storageServicesByPriorities = storageServicesToBeUsed;

            if (storageServicesByPriorities.isEmpty())
                throw new ObjectCouldNotBeRetrievedException(archivalObject);
        }
        ObjectRetrievalResource objectRef;
        try {
            objectRef = retrieveObjectFromStorage(archivalObject, storageServicesByPriorities.get(0));
            if (objectRef == null) {
                objectRef = recoverObjectFromOtherStorages(archivalObject, storageServicesByPriorities, true);
            }
        } catch (ObjectCouldNotBeRetrievedException e) {
            log.error("Cannot retrieve object " + archivalObject.getStorageId() + " form neither of the storages.");
            throw e;
        } catch (StorageException e) {
            log.error("Storage error has occurred during retrieval process of object: " + archivalObject.getStorageId());
            objectRef = recoverObjectFromOtherStorages(archivalObject, storageServicesByPriorities, false);
        }
        log.info("object: " + archivalObject.getStorageId() + " has been successfully retrieved.");
        return objectRef;
    }

    /**
     * Retrieves single object from storage. Returns <code>null</code> if the object has been corrupted, i.e. its checksum does not match expected value.
     * The retrieved object is stored in temporary file. Connection used for retrieval is closed.
     *
     * @param objectDto      DTO with the object to retrieve
     * @param storageService storage services to retrieve he object from
     * @return DTO with object stream if object was found and is valid, null if object checksum does not match expected value
     * @throws StorageException if an error occurred during object retrieval
     */
    private ObjectRetrievalResource retrieveObjectFromStorage(ArchivalObjectDto objectDto, StorageService storageService)
            throws StorageException {
        String storageName = storageService.getStorage().getName();
        log.debug("Storage: " + storageName + " chosen to retrieve object: " + objectDto.getStorageId());

        ObjectRetrievalResource objectFromStorage = storageService.getObject(objectDto.getStorageId(), objectDto.getOwner().getDataSpace());
        String tmpFileId = objectFromStorage.getId();
        File tmpFile = tmpFolder.resolve(tmpFileId).toFile();
        boolean valid = copyObjectToTmpFolderAndVerifyChecksum(objectDto.getDatabaseId(), objectFromStorage.getInputStream(), objectDto.getChecksum(), tmpFile, storageName);
        if (!valid)
            return null;
        try {
            log.debug("Validated checksum of object with storage id " + objectDto.getStorageId() +
                    " retrieved from storage " + storageService.getStorage().getName() + ".");
            objectFromStorage.setInputStream(new FileInputStream(tmpFile));
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException("could not find tmp file " + objectFromStorage.getId(), e);
        }
        return objectFromStorage;
    }

    /**
     * This method is called when the very first attempt to return object fails. It scans through all storages until it finds
     * valid object. Then it tries to recover all objects on storages where the object was corrupted. If the recovery fails it is logged
     * and the method continues.
     *
     * @param objectDto                 object from the main request
     * @param storageServices           storage services which are used for retrieval
     * @param problemWasInvalidChecksum true if the first storage has failed because of corrupted object,
     *                                  or false if the first attempt failed because of other error
     * @return {@link ObjectRetrievalResource} with valid object
     * @throws ObjectCouldNotBeRetrievedException if no valid object was found
     */
    private ObjectRetrievalResource recoverObjectFromOtherStorages(ArchivalObjectDto objectDto, List<StorageService> storageServices,
                                                                   boolean problemWasInvalidChecksum) throws ObjectCouldNotBeRetrievedException {
        log.debug("Recovering object " + objectDto.getStorageId() + " from other storages.");
        List<StorageService> invalidChecksumStorages = new ArrayList<>();

        //if the first storage retrieved object but it was corrupted, add the storage to those which will be recovered
        if (problemWasInvalidChecksum) {
            invalidChecksumStorages.add(storageServices.get(0));
        }

        ObjectRetrievalResource objectRetrievalResource = null;
        StorageService successfulService = null;
        //iterate over all the storages to find an uncorrupted version of the object
        for (int i = 1; i < storageServices.size(); i++) {
            try {
                objectRetrievalResource = retrieveObjectFromStorage(objectDto, storageServices.get(i));
                if (objectRetrievalResource != null) {
                    successfulService = storageServices.get(i);
                    break;
                }
                invalidChecksumStorages.add(storageServices.get(i));
            } catch (StorageException e) {
                //try other storages when the current storage has failed
                log.error("Storage error has occurred during retrieval process of object: " + objectDto.getStorageId());
            }
        }
        if (objectRetrievalResource == null) {
            log.error("Object: " + objectDto.getStorageId() + " has failed to be recovered from any storage service.");
            storageServices.removeAll(invalidChecksumStorages);
            arcstorageMailCenter.sendObjectRetrievalError(objectDto, null, servicesToEntities(storageServices),
                    servicesToEntities(invalidChecksumStorages), null);
            throw new ObjectCouldNotBeRetrievedException(objectDto);
        }

        log.debug("object " + objectDto.getStorageId() + " has been successfully retrieved");
        List<StorageService> recoveredStorages = new ArrayList<>();
        for (StorageService storageService : invalidChecksumStorages) {
            boolean success = recoverSingleObject(storageService, objectDto, objectRetrievalResource.getId());
            if (success)
                recoveredStorages.add(storageService);
        }
        storageServices.removeAll(invalidChecksumStorages);
        arcstorageMailCenter.sendObjectRetrievalError(objectDto, successfulService.getStorage(), servicesToEntities(storageServices), servicesToEntities(invalidChecksumStorages), servicesToEntities(recoveredStorages));
        return objectRetrievalResource;
    }

    /**
     * Recovers object at the provided archival storage.
     *
     * @param storageService storage service to recover at
     * @param objectDto      object to be recovered
     * @param tmpFileId      id of the file storing the object content at the temporary storage
     * @return <code>true</code> if the recovery was successful, <code>false</code> otherwise
     */
    boolean recoverSingleObject(StorageService storageService, ArchivalObjectDto objectDto, String tmpFileId) {
        log.debug("Recovering object " + objectDto.getStorageId() + " at storage " + storageService.getStorage().getName() + ".");
        try (FileInputStream objectInputStream = new FileInputStream(tmpFolder.resolve(tmpFileId).toFile())) {
            objectDto.setInputStream(objectInputStream);
            storageService.storeObject(objectDto, new AtomicBoolean(false), objectDto.getOwner().getDataSpace());
            log.info("Object " + objectDto.getStorageId() + " has been successfully recovered at storage " +
                    storageService.getStorage().getName() + ".");
        } catch (StorageException e) {
            log.error("Object " + objectDto.getStorageId() + " has failed to be recovered at storage " +
                    storageService.getStorage().getName() + ".");
            return false;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read tmp file " + tmpFileId, e);
        }
        return true;
    }

    /**
     * Copies object to temporary folder and verifies checksum.
     *
     * @param objectIs       input stream with the object
     * @param checksum       checksum of the object
     * @param tmpFile        file to copy to
     * @param objectDbId     id of the object in db
     * @param storageLogName storage name (used in the log message)
     * @return <code>true</code> in case of success, <code>false</code> otherwise
     */
    boolean copyObjectToTmpFolderAndVerifyChecksum(String objectDbId, InputStream objectIs, Checksum checksum, File tmpFile,
                                                   String storageLogName) {
        try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
            Checksum objectStorageChecksum = copyStreamAndComputeChecksum(objectIs, fos, checksum.getType());
            if (!checksum.equals(objectStorageChecksum)) {
                log.error("Checksum for object with database id: " + objectDbId + " (temporarily stored in archival storage workspace as file: " + tmpFile.getName() + ") is invalid at storage " + storageLogName +
                        ". Expected checksum: " + checksum + " computed checksum: " + objectStorageChecksum);
                tmpFile.delete();
                return false;
            }
            return true;
        } catch (IOException e) {
            if (tmpFile.exists())
                tmpFile.delete();
            throw new UncheckedIOException(
                    "Error while creating or writing to file at " + tmpFile, e);
        }
    }

    @Inject
    public void setArcstorageMailCenter(ArcstorageMailCenter arcstorageMailCenter) {
        this.arcstorageMailCenter = arcstorageMailCenter;
    }

    @Inject
    public void setTmpFolder(@Value("${arcstorage.tmpFolder}") String path) {
        this.tmpFolder = Paths.get(path);
    }

    @Inject
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    @Inject
    public void setArchivalDbService(ArchivalDbService archivalDbService) {
        this.archivalDbService = archivalDbService;
    }

    @Inject
    public void setAsync(ArchivalAsyncService async) {
        this.async = async;
    }
}
