package cz.cas.lib.arcstorage.service;


import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.DomainObject;
import cz.cas.lib.arcstorage.dto.AipDto;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.dto.TmpSourceHolder;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.service.exception.CantReadException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.util.ApplicationContextUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;

/**
 * Time-consuming operations which are expected to be processed in parallel should be processed using defined
 * {@link #batchOpsExecutor} service instead of using {@link Async} or {@link #executor}.
 */
@Service
@Slf4j
public class ArchivalAsyncService {

    private ArchivalDbService archivalDbService;
    private ExecutorService executor;
    private ArcstorageMailCenter mailCenter;
    private ExecutorService batchOpsExecutor;

    /**
     * Saves AIP asynchronously to the provided storage services.
     * <p>
     * If the storage process succeeds at every storage service, the AIP in DB changes state to ARCHIVED.
     * In case of an archival storage error at any of the storages, the storage process is rolled back.
     * If the rollback succeeds, AIP in DB is set to ROLL_BACKED.
     * If the rollback fails at any of the storages, AIP in DB is set to ARCHIVAL FAILURE.
     *
     * @param aip             AIP DTO
     * @param tmpSip      temporary source holder of SIP content
     * @param tmpXml      temporary source holder of XML content
     * @param storageServices storage services to store AIP to
     * @param dataSpace       data space of the AIP owner
     */
    public void saveAip(AipDto aip, TmpSourceHolder tmpSip, TmpSourceHolder tmpXml, List<StorageService> storageServices, String dataSpace) {
        CompletableFuture<Void> parentThread = CompletableFuture.runAsync(() -> {
            String op = "Storing aip: ";
            String failureMsgFormat = op + " failed, because %s, current object state: %s";
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            String sipDbId = aip.getSip().getDatabaseId();
            String xmlDbId = aip.getXml().getDatabaseId();
            Pair<AtomicBoolean, Lock> aipRollbackFlag = ApplicationContextUtils.getProcessingObjects().get(sipDbId);
            for (StorageService a : storageServices) {
                CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                    if (aipRollbackFlag.getLeft().get())
                        return;
                            try (InputStream sipStream = tmpSip.createInputStream();
                                 InputStream xmlStream = tmpXml.createInputStream()) {
                                a.storeAip(new AipDto(aip, sipStream, xmlStream), aipRollbackFlag.getLeft(), dataSpace);
                                log.debug(a.getStorage() + ", " + aip + ", " + op + "success");
                            } catch (StorageException e) {
                                log.warn(a.getStorage() + ", " + aip + ", " + op + "error: " + e);
                                throw new GeneralException(e);
                            } catch (IOException e) {
                                throw new CantReadException("SIP tmp file at path " + tmpSip.toString() +
                                        " or XML tmp file at path " + tmpXml, e);
                            }
                        }, batchOpsExecutor
                );
                futures.add(c);
            }
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
            } catch (InterruptedException e) {
                String msg = String.format(failureMsgFormat, "main thread has been interrupted", ObjectState.ARCHIVAL_FAILURE);
                archivalDbService.setObjectsState(ObjectState.ARCHIVAL_FAILURE, sipDbId, xmlDbId);
                throw new GeneralException(msg, e);
            } catch (ExecutionException e) {
                aipRollbackFlag.getLeft().set(true);
                log.error(op + "some storage has encountered problem", e);
            } finally {
                tmpSip.freeSpace();
                tmpXml.freeSpace();
            }
            aipRollbackFlag.getRight().lock();
            try {
                if (!aipRollbackFlag.getLeft().get()) {
                    archivalDbService.setObjectsState(ObjectState.ARCHIVED, sipDbId, xmlDbId);
                    log.info(aip + op + "success on all storages");
                    return;
                }
            } finally {
                aipRollbackFlag.getRight().unlock();
            }

            log.info(op + "Archival storage error. Starting rollback.");
            futures = new ArrayList<>();
            for (StorageService a : storageServices) {
                CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                    try {
                        a.rollbackAip(aip, dataSpace);
                        log.warn(a.getStorage() + ", " + aip + ", " + "rolled back");
                    } catch (StorageException e) {
                        log.error(a.getStorage() + ", " + aip + ", " + "rollback process error: " + e);
                        throw new GeneralException(e);
                    }
                }, batchOpsExecutor);
                futures.add(c);
            }
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
                archivalDbService.setObjectsState(ObjectState.ROLLED_BACK, sipDbId, xmlDbId);
                log.info(aip + " rollback successful on all storages.");
            } catch (InterruptedException e) {
                String msg = String.format(failureMsgFormat, "main thread has been interrupted", ObjectState.ARCHIVAL_FAILURE);
                archivalDbService.setObjectsState(ObjectState.ARCHIVAL_FAILURE, sipDbId, xmlDbId);
                throw new GeneralException(msg, e);
            } catch (ExecutionException e) {
                String msg = String.format(failureMsgFormat, "some logical storage failed during storing, and also during rollback", ObjectState.ARCHIVAL_FAILURE);
                archivalDbService.setObjectsState(ObjectState.ARCHIVAL_FAILURE, sipDbId, xmlDbId);
                throw new GeneralException(msg, e);
            }
        }, batchOpsExecutor);
    }

    /**
     * Saves archival object to the provided storage services.
     * <p>
     * If the storage process succeeds at every storage service, the object in DB changes state to ARCHIVED.
     * In case of an archival storage error at any of the storages, the storage process is rolled back.
     * If the rollback succeeds, object in DB is set to ROLLED_BACK.
     * If the rollback fails at any of the storages, object in DB is set to ARCHIVAL FAILURE.
     *
     * @param archivalObject  DTO of the archival object to store
     * @param tmpSourceHolder source holder with the object to store
     * @param storageServices storage services to store to
     * @param sync if true, the operation is processed synchronously
     */
    public void saveObject(ArchivalObjectDto archivalObject, TmpSourceHolder tmpSourceHolder, List<StorageService> storageServices, boolean sync) {
        Executor executorToUse = sync ? executor : batchOpsExecutor;
        String op = sync ? "Synchronously storing object: " : "Storing object: ";
        String failureMsgFormat = op + " failed, because %s, current object state: %s";
        String objDbId = archivalObject.getDatabaseId();
        Pair<AtomicBoolean, Lock> objRollbackFlag = ApplicationContextUtils.getProcessingObjects().get(objDbId);
        Runnable saveObjectProcess = () -> {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            AtomicBoolean rollback = new AtomicBoolean(false);
            for (StorageService a : storageServices) {
                CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                            try (InputStream objectStream = tmpSourceHolder.createInputStream()) {
                                ArchivalObjectDto archivalObjectCpy = new ArchivalObjectDto(archivalObject, objectStream);
                                a.storeObject(archivalObjectCpy, rollback, archivalObject.getOwner().getDataSpace());
                                log.debug(a.getStorage() + ", " + archivalObject + ", " + op + "success");
                            } catch (StorageException e) {
                                log.warn(a.getStorage() + ", " + archivalObject + ", " + op + "error");
                                throw new GeneralException(e);
                            } catch (IOException e) {
                                throw new CantReadException("Object tmp file at path " + tmpSourceHolder.toString(), e);
                            }
                        }, executorToUse
                );
                futures.add(c);
            }
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
            } catch (InterruptedException e) {
                String msg = String.format(failureMsgFormat, "main thread has been interrupted", ObjectState.ARCHIVAL_FAILURE);
                archivalDbService.setObjectsState(ObjectState.ARCHIVAL_FAILURE, objDbId);
                throw new GeneralException(msg, e);
            } catch (ExecutionException e) {
                rollback.set(true);
                log.error(op + "some storage has encountered problem", e);
            } finally {
                tmpSourceHolder.freeSpace();
            }
            objRollbackFlag.getRight().lock();
            try {
                if (!rollback.get()) {
                    archivalDbService.setObjectsState(ObjectState.ARCHIVED, objDbId);
                    log.info(archivalObject + op + "success on all storages");
                    return;
                }
            } finally {
                objRollbackFlag.getRight().unlock();
            }

            log.info(op + "Archival storage error. Starting rollback.");
            futures = new ArrayList<>();
            for (StorageService a : storageServices) {
                CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                    try {
                        a.rollbackObject(archivalObject, archivalObject.getOwner().getDataSpace());
                        log.warn(a.getStorage() + ", " + archivalObject + ", " + "rolled back");
                    } catch (StorageException e) {
                        log.error(a.getStorage() + ", " + archivalObject + ", " + "rollback process error");
                        throw new GeneralException(e);
                    }
                }, executorToUse);
                futures.add(c);
            }
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
                archivalDbService.setObjectsState(ObjectState.ROLLED_BACK, objDbId);
                log.info(archivalObject + "rollback successful on all storages.");
                String msg = String.format(failureMsgFormat, "some logical storage failed during storing, however all succeeded during rollback", ObjectState.ROLLED_BACK);
                if (sync)
                    throw new GeneralException(msg);
            } catch (InterruptedException e) {
                String msg = String.format(failureMsgFormat, "main thread has been interrupted", ObjectState.ARCHIVAL_FAILURE);
                archivalDbService.setObjectsState(ObjectState.ARCHIVAL_FAILURE, objDbId);
                throw new GeneralException(msg, e);
            } catch (ExecutionException e) {
                String msg = String.format(failureMsgFormat, "some logical storage failed during storing, and also during rollback", ObjectState.ARCHIVAL_FAILURE);
                archivalDbService.setObjectsState(ObjectState.ARCHIVAL_FAILURE, objDbId);
                throw new GeneralException(msg, e);
            }
        };
        if (sync)
            saveObjectProcess.run();
        else
            CompletableFuture.runAsync(saveObjectProcess, batchOpsExecutor);
    }
    /**
     * Deletes object at the provide storage services.
     * <p>
     * In case of of error during the deletion process at any of the storage,
     * state of the object in DB is set to DELETION_FAILURE.
     *
     * @param archivalObject DTO with the object to delete
     * @param storageServices   storage services to delete the object from
     */
    public void deleteObject(ArchivalObjectDto archivalObject, List<StorageService> storageServices) {
        CompletableFuture<Void> parentThread = CompletableFuture.runAsync(() -> {
            String op = "deleting object: ";
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            boolean successOnAllStorages = true;
            for (StorageService a : storageServices) {
                CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                            try {
                                a.delete(archivalObject.getStorageId(), archivalObject.getOwner().getDataSpace());
                                log.debug(a.getStorage() + ", " + archivalObject + ", " + op + "success");
                            } catch (StorageException e) {
                                log.warn(a.getStorage() + ", " + archivalObject + ", " + op + "error");
                                throw new GeneralException(e);
                            }
                        }, batchOpsExecutor
                );
                futures.add(c);
            }
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
            } catch (InterruptedException e) {
                String s = op + "main thread has been interrupted";
                archivalDbService.setObjectsState(ObjectState.DELETION_FAILURE, archivalObject.getDatabaseId());
                throw new GeneralException(s, e);
            } catch (ExecutionException e) {
                successOnAllStorages = false;
                log.error(op + "some storage has encountered problem", e);
            }
            if (successOnAllStorages) {
                log.info(archivalObject + op + "success on all storages");
                return;
            }
            archivalDbService.setObjectsState(ObjectState.DELETION_FAILURE, archivalObject.getDatabaseId());
            log.error(archivalObject + "deletion failed on some storages");
        }, batchOpsExecutor);
    }

    /**
     * Rolls back object at the provide storage services.
     * <p>
     * In case of of error during the deletion process at any of the storage,
     * state of the object in DB is set to {@link ObjectState#ROLLBACK_FAILURE}.
     *
     * @param archivalObject  DTO with the object to rollback
     * @param storageServices storage services to rollback the object from
     */
    public void rollbackObject(ArchivalObjectDto archivalObject, List<StorageService> storageServices) {
        CompletableFuture<Void> parentThread = CompletableFuture.runAsync(() -> {
            String op = "rolling back object: ";
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            boolean successOnAllStorages = true;
            for (StorageService a : storageServices) {
                CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                            try {
                                a.rollbackObject(archivalObject, archivalObject.getOwner().getDataSpace());
                                log.debug(a.getStorage() + ", " + archivalObject + ", " + op + "success");
                            } catch (StorageException e) {
                                log.warn(a.getStorage() + ", " + archivalObject + ", " + op + "error");
                                throw new GeneralException(e);
                            }
                        }, batchOpsExecutor
                );
                futures.add(c);
            }
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
            } catch (InterruptedException e) {
                String s = op + "main thread has been interrupted";
                archivalDbService.setObjectsState(ObjectState.ROLLBACK_FAILURE, archivalObject.getDatabaseId());
                throw new GeneralException(s, e);
            } catch (ExecutionException e) {
                successOnAllStorages = false;
                log.error(op + "some storage has encountered problem", e);
            }
            if (successOnAllStorages) {
                log.info(archivalObject + op + "success on all storages");
                return;
            }
            archivalDbService.setObjectsState(ObjectState.ROLLBACK_FAILURE, archivalObject.getDatabaseId());
            log.error(archivalObject + "rollback failed on some storages");
        }, batchOpsExecutor);
    }

    /**
     * Performs clean up for the objects at the provided storage services by:
     * 1. deleting the objects with state DELETION_FAILURE
     * 2. rolling back all other objects
     * <p>
     * In case of an error during the clean up at any of the storage services, a notification mail is sent with the description
     * of the error.
     *
     * @param objects         objects to be cleaned
     * @param storageServices storage services to perform the clean up at
     */
    @Async
    public void cleanUp(List<ArchivalObject> objects, List<StorageService> storageServices) {
        String op = "cleaning object ";
        List<ArchivalObject> rolledBackObjects = new ArrayList<>();
        List<ArchivalObject> deletedObjects = new ArrayList<>();
        List<ArchivalObject> failedObjects = new ArrayList<>();
        for (ArchivalObject archivalObject : objects) {
            String storageId = archivalObject instanceof AipXml ? toXmlId(((AipXml) archivalObject).getSip().getId(), ((AipXml) archivalObject).getVersion()) : archivalObject.getId();
            boolean rollback = archivalObject.getState() != ObjectState.DELETION_FAILURE;
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            boolean successOnAllStorages = true;
            for (StorageService a : storageServices) {
                CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                            try {
                                if (rollback)
                                    a.rollbackObject(archivalObject.toDto(), archivalObject.getOwner().getDataSpace());
                                else
                                    a.delete(storageId, archivalObject.getOwner().getDataSpace());
                                log.debug(a.getStorage() + ", " + archivalObject + ", " + op + "success");
                            } catch (StorageException e) {
                                log.warn(a.getStorage() + ", " + archivalObject + ", " + op + "error");
                                throw new GeneralException(e);
                            }
                        }, executor
                );
                futures.add(c);
            }
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
            } catch (InterruptedException e) {
                String s = op + "main thread has been interrupted";
                throw new GeneralException(s, e);
            } catch (ExecutionException e) {
                successOnAllStorages = false;
                log.error(op + "some storage has encountered problem", e);
            }
            if (successOnAllStorages) {
                if (rollback)
                    rolledBackObjects.add(archivalObject);
                else
                    deletedObjects.add(archivalObject);
                continue;
            }
            failedObjects.add(archivalObject);
        }
        log.info("cleanup finished");
        if (!rolledBackObjects.isEmpty()) {
            archivalDbService.setObjectsState(ObjectState.ROLLED_BACK, rolledBackObjects.stream().map(DomainObject::getId).toArray(String[]::new));
            log.info("successfully rolled back " + rolledBackObjects.size() + " objects");
            log.debug("rolled back objects: " + Arrays.toString(rolledBackObjects.stream().map(o -> o.toDto().toString()).toArray()));
        }
        if (!deletedObjects.isEmpty()) {
            archivalDbService.setObjectsState(ObjectState.DELETED, deletedObjects.stream().map(DomainObject::getId).toArray(String[]::new));
            log.info("successfully deleted " + deletedObjects.size() + " objects");
            log.debug("deleted objects: " + Arrays.toString(deletedObjects.stream().map(o -> o.toDto().toString()).toArray()));
        }
        if (!failedObjects.isEmpty()) {
            log.error("unable to cleanup " + failedObjects.size() + " objects: " + Arrays.toString(failedObjects.stream().map(o -> o.toDto().toString()).toArray()));
            mailCenter.sendCleanupError(objects, deletedObjects, rolledBackObjects, failedObjects);
        }
    }

    /**
     * Removes object from the provided storage services.
     *
     * @param id              id of the object to remove
     * @param storageServices storage services to remove from
     * @param dataSpace       data space of the owner of the object
     * @throws StorageException storage error has occurred during removal of object
     */
    @Async
    public void removeObject(String id, List<StorageService> storageServices, String dataSpace) throws StorageException {
        try {
            for (StorageService storageService : storageServices) {
                storageService.remove(id, dataSpace);
            }
        } catch (StorageException e) {
            log.error("Storage error has occurred during removal of object: " + id + ". Object is marked as removed in DB but may not be marked as removed on every storage.");
            throw e;
        }
        log.info("Object: " + id + " has been successfully removed.");
    }

    /**
     * Renews object at the provided storage services.
     *
     * @param id              id of the object to renew
     * @param storageServices storage services to renew object at
     * @param dataSpace       data space of the owner of the object
     * @throws StorageException storage error has occurred during renewing of object
     */
    @Async
    public void renewObject(String id, List<StorageService> storageServices, String dataSpace) throws StorageException {
        try {
            for (StorageService storageService : storageServices) {
                storageService.renew(id, dataSpace);
            }
        } catch (StorageException e) {
            log.error("Storage error has occurred during renewing of object: " + id + ". Object is marked as archived in DB but may not be marked as archived on every storage.");
            throw e;
        }
        log.info("Object: " + id + " has been successfully renewed.");
    }

    @Inject
    public void setArchivalDbService(ArchivalDbService archivalDbService) {
        this.archivalDbService = archivalDbService;
    }

    @Inject
    public void setMailCenter(ArcstorageMailCenter mailCenter) {
        this.mailCenter = mailCenter;
    }

    @Inject
    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    @Resource(name = "BatchOpsExecutorService")
    public void setBatchOpsExecutor(ExecutorService batchOpsExecutor) {
        this.batchOpsExecutor = batchOpsExecutor;
    }
}
