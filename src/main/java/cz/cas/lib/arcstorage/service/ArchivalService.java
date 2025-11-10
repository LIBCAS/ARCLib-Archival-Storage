package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.*;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ObjectRetrievalResource;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.jms.JmsAction;
import cz.cas.lib.arcstorage.jms.JmsHealthCheckException;
import cz.cas.lib.arcstorage.jms.JmsQueueManager;
import cz.cas.lib.arcstorage.jms.JmsSender;
import cz.cas.lib.arcstorage.jms.context.StoragesContextRegistry;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.state.*;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.ObjectCouldNotBeRetrievedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.storagesync.ObjectAudit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.dto.ObjectState.*;
import static cz.cas.lib.arcstorage.storage.StorageUtils.copyStreamAndComputeChecksum;
import static cz.cas.lib.arcstorage.util.Utils.servicesToEntities;

/**
 * Service which provides methods for operations upon objects as general.
 * While the methods may internally perform different logic for {@link AipXml}, {@link AipSip} and {@link ArchivalObject}, the method
 * signatures remain general, hiding the differences of objects.
 */
@Service
@Slf4j
public class ArchivalService {

    private StorageProvider storageProvider;
    private ArcstorageMailCenter arcstorageMailCenter;
    private ArchivalDbService archivalDbService;
    private FileLocationResolver fileLocationResolver;
    private JmsQueueManager queueManager;
    private StoragesContextRegistry storagesContextRegistry;
    private JmsSender jmsSender;

    public static final Set<ObjectState> CLEANUP_ALLOWED_STATES = Set.of(ARCHIVAL_FAILURE,
            DELETION_FAILURE,
            ROLLBACK_FAILURE,
            PROCESSING,
            PRE_PROCESSING);

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
            ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException, SomeLogicalStoragesNotReachableException {
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
            RollbackStateException, FailedStateException, ReadOnlyStateException, JmsHealthCheckException {
        log.debug("Removing object with id: {}", id);
        jmsSender.healthCheck();
        Pair<ArchivalObject, ObjectAudit> res = archivalDbService.removeObject(id);

        try {
            Set<Storage> allStorages = storageProvider.getAllStorages();
            for (Storage a : allStorages) {
                jmsSender.modifyObject(a.getId(), res.getLeft().toDto(), res.getRight().getCreated(), JmsAction.REMOVE);
            }
        } catch (Exception e) {
            log.error("removal of object {} failed", id, e);
        }
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
            RollbackStateException, FailedStateException, ReadOnlyStateException, JmsHealthCheckException {
        log.debug("Renewing object with id " + id + ".");
        jmsSender.healthCheck();
        Pair<ArchivalObject, ObjectAudit> res = archivalDbService.renewObject(id);

        try {
            Set<Storage> allStorages = storageProvider.getAllStorages();
            for (Storage a : allStorages) {
                jmsSender.modifyObject(a.getId(), res.getLeft().toDto(), res.getRight().getCreated(), JmsAction.RENEW);
            }
        } catch (Exception e) {
            log.error("renewal of object {} failed", id, e);
        }
    }

    /**
     * Physically removes object from storage. Data in database are not removed.
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
            FailedStateException, ReadOnlyStateException, JmsHealthCheckException {

        log.debug("Deleting object with id " + id + ".");
        jmsSender.healthCheck();
        Pair<ArchivalObject, ObjectAudit> res = archivalDbService.deleteObject(id);
        ArchivalObject objectInDb = res.getLeft();

        try {
            storagesContextRegistry.findProcessingObjectContexts(objectInDb.getId()).forEach(c -> c.getStopSignal().set(true));
            Set<Storage> allStorages = storageProvider.getAllStorages();
            for (Storage a : allStorages) {
                jmsSender.modifyObject(a.getId(), objectInDb.toDto(), res.getRight().getCreated(), JmsAction.DELETE);
            }
        } catch (Exception e) {
            log.error("deletion of object {} failed", objectInDb.getId(), e);
            archivalDbService.setObjectsState(ObjectState.DELETION_FAILURE, objectInDb.getId());
        }

        //just optimization.. not needed to interfere with transactions
        queueManager.deleteNotProcessingStoreRequests(Set.of(id), res.getRight().getCreated());
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
            NoLogicalStorageAttachedException, ReadOnlyStateException, JmsHealthCheckException {

        String id = objectToRollback.getId();
        log.debug("Rolling back object with id " + id + ".");
        List<ArchivalObject> objectsToRollback = new ArrayList<>();
        objectsToRollback.add(objectToRollback);
        if (objectToRollback instanceof AipSip) {
            List<AipXml> xmls = ((AipSip) objectToRollback).getXmls();
            objectsToRollback.addAll(xmls);
        }
        Instant now = Instant.now();
        jmsSender.healthCheck();

        for (ArchivalObject objectInDb : objectsToRollback) {
            if (objectInDb.getState() == ObjectState.ROLLED_BACK) {
                continue;
            }
            Pair<ArchivalObject, ObjectAudit> res = archivalDbService.rollbackObject(objectInDb);
            Set<Storage> allStorages = storageProvider.getAllStorages();

            try {
                storagesContextRegistry.findProcessingObjectContexts(objectInDb.getId()).forEach(c -> c.getStopSignal().set(true));
                for (Storage a : allStorages) {
                    jmsSender.modifyObject(a.getId(), res.getLeft().toDto(), res.getRight().getCreated(), JmsAction.ROLLBACK);
                }
            } catch (Exception e) {
                log.error("rollback of object {} failed", objectInDb.getId(), e);
                archivalDbService.setObjectsState(ObjectState.ROLLBACK_FAILURE, objectInDb.getId());
            }
        }

        //just optimization.. not needed to interfer with transactions
        queueManager.deleteNotProcessingStoreRequests(objectsToRollback.stream().map(DomainObject::getId).collect(Collectors.toSet()), now);
    }

    /**
     * Forgets object. If the requested object is {@link AipSip} then also all related {@link AipXml} are forget.
     *
     * @param objectToForget
     * @throws StateException
     * @throws SomeLogicalStoragesNotReachableException
     * @throws NoLogicalStorageAttachedException
     * @throws ReadOnlyStateException
     */
    public void forgetObject(ArchivalObject objectToForget) throws
            StateException,
            SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException, StorageException, JmsHealthCheckException {
        String id = objectToForget.getId();
        log.debug("Forget object with id " + id + ".");
        List<ArchivalObject> allObjectsToForget = new ArrayList<>();
        allObjectsToForget.add(objectToForget);
        boolean isAip = objectToForget instanceof AipSip;
        if (isAip) {
            List<AipXml> xmls = ((AipSip) objectToForget).getXmlsSortedByVersionAsc();
            allObjectsToForget.addAll(xmls);
        }
        for (ArchivalObject archivalObject : allObjectsToForget) {
            switch (archivalObject.getState()) {
                case ARCHIVED:
                case DELETED:
                case REMOVED:
                case ROLLED_BACK:
                    continue;
                default:
                    throw new StateException(archivalObject);
            }
        }
        ArrayList<ArchivalObject> allObjectsToForgetReversedOrder = new ArrayList<>(allObjectsToForget);
        Collections.reverse(allObjectsToForgetReversedOrder);
        jmsSender.healthCheck();
        Instant dbTimestamp = archivalDbService.forgetObjects(allObjectsToForgetReversedOrder);
        Set<Storage> allStorages = storageProvider.getAllStorages();

        for (ArchivalObject archivalObject : allObjectsToForgetReversedOrder) {
            storagesContextRegistry.findProcessingObjectContexts(id).forEach(c -> c.getStopSignal().set(true));
            for (Storage a : allStorages) {
                jmsSender.modifyObject(a.getId(), archivalObject.toDto(), dbTimestamp, JmsAction.FORGET);
            }
        }
        log.info("Object with id " + id + " have been forgotten.");

        //just optimization.. not needed to interfer with transactions
        queueManager.deleteNotProcessingStoreRequests(allObjectsToForgetReversedOrder.stream().map(DomainObject::getId).collect(Collectors.toSet()), dbTimestamp);
    }

    /**
     * Performs clean up of provided objects (candidates are failed or hanging ones).
     * 1. deleting the objects with state DELETION_FAILURE
     * 2. rolling back all other objects
     * <p>
     *
     * @param objects objects to be cleaned
     */
    public void cleanUp(List<ArchivalObject> objects, Instant timestamp) throws JmsHealthCheckException {

        if (!objects.stream().allMatch(o -> CLEANUP_ALLOWED_STATES.contains(o.getState()))) {
            throw new IllegalArgumentException("some object was in unsupported state");
        }

        jmsSender.healthCheck();

        for (ArchivalObject archivalObject : objects) {
            storagesContextRegistry.findProcessingObjectContexts(archivalObject.getId()).forEach(c -> c.getStopSignal().set(true));
        }
        queueManager.deleteNotProcessingStoreRequests(objects.stream().map(DomainObject::getId).collect(Collectors.toSet()), timestamp);

        log.info("sending messages to clean storage of objects {}", StringUtils.join(objects, ","));
        Collection<Storage> allStorages = storageProvider.getAllStorages();

        Set<ArchivalObject> rolledBackObjects = new HashSet<>();
        Set<ArchivalObject> deletedObjects = new HashSet<>();
        for (ArchivalObject archivalObject : objects) {
            if (archivalObject.getState() == DELETION_FAILURE) {
                deletedObjects.add(archivalObject);
            } else {
                rolledBackObjects.add(archivalObject);
            }
        }

        if (!rolledBackObjects.isEmpty()) {
            archivalDbService.setObjectsState(ObjectState.ROLLED_BACK, rolledBackObjects.stream().map(DomainObject::getId).toArray(String[]::new));
            log.info("successfully rolled back " + rolledBackObjects.size() + " objects in DB");
            for (ArchivalObject archivalObject : rolledBackObjects) {
                for (Storage a : allStorages) {
                    jmsSender.modifyObject(a.getId(), archivalObject.toDto(), timestamp, JmsAction.ROLLBACK);
                }
            }
            log.debug("sent rollback messages for objects: " + Arrays.toString(rolledBackObjects.stream().map(o -> o.toDto().toString()).toArray()));
        }

        if (!deletedObjects.isEmpty()) {
            archivalDbService.setObjectsState(ObjectState.DELETED, deletedObjects.stream().map(DomainObject::getId).toArray(String[]::new));
            log.info("successfully deleted " + deletedObjects.size() + " objects in DB");
            for (ArchivalObject archivalObject : deletedObjects) {
                for (Storage a : allStorages) {
                    jmsSender.modifyObject(a.getId(), archivalObject.toDto(), timestamp, JmsAction.DELETE);
                }
            }
            log.debug("sent delete messages for objects: " + Arrays.toString(deletedObjects.stream().map(o -> o.toDto().toString()).toArray()));
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
            throws ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException, SomeLogicalStoragesNotReachableException {
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
            throws ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException, SomeLogicalStoragesNotReachableException {
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

        ObjectRetrievalResource objectFromStorage = storageService.getObject(objectDto.getStorageId(), objectDto.getDataSpace());
        File tmpFile = fileLocationResolver.getFileTmpPath(objectFromStorage.getId()).toFile();
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
        try (FileInputStream objectInputStream = new FileInputStream(fileLocationResolver.getFileTmpPath(tmpFileId).toFile())) {
            objectDto.setInputStream(objectInputStream);
            storageService.storeObject(objectDto, new AtomicBoolean(false), objectDto.getDataSpace(), Instant.now());
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

    @Autowired
    public void setArcstorageMailCenter(ArcstorageMailCenter arcstorageMailCenter) {
        this.arcstorageMailCenter = arcstorageMailCenter;
    }

    @Autowired
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    @Autowired
    public void setArchivalDbService(ArchivalDbService archivalDbService) {
        this.archivalDbService = archivalDbService;
    }

    @Autowired
    public void setFileLocationResolver(FileLocationResolver fileLocationResolver) {
        this.fileLocationResolver = fileLocationResolver;
    }

    @Autowired
    public void setQueueManager(JmsQueueManager queueManager) {
        this.queueManager = queueManager;
    }

    @Autowired
    public void setStoragesContextRegistry(StoragesContextRegistry storagesContextRegistry) {
        this.storagesContextRegistry = storagesContextRegistry;
    }

    @Autowired
    public void setJmsSender(JmsSender jmsSender) {
        this.jmsSender = jmsSender;
    }
}
