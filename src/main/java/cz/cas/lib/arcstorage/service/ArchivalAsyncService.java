package cz.cas.lib.arcstorage.service;


import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.DomainObject;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.dto.AipDto;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.dto.TmpSourceHolder;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.service.exception.CantReadException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static cz.cas.lib.arcstorage.util.Utils.*;

@Service
@Slf4j
public class ArchivalAsyncService {

    private ArchivalDbService archivalDbService;
    private ExecutorService executor;
    private ArcstorageMailCenter mailCenter;

    /**
     * Saves AIP asynchronously to the provided storage services.
     * <p>
     * If the storage process succeeds at every storage service, the AIP in DB changes state to ARCHIVED.
     * In case of an archival storage error at any of the storages, the storage process is rolled back.
     * If the rollback succeeds, AIP in DB is set to ROLL_BACKED.
     * If the rollback fails at any of the storages, AIP in DB is set to ARCHIVAL FAILURE.
     *
     * @param aip             AIP DTO
     * @param tmpSipPath      path to the temporary location storing the SIP content
     * @param xmlContent      content of the XML
     * @param storageServices storage services to store AIP to
     * @param dataSpace       data space of the AIP owner
     */
    @Async
    @Transactional
    public void saveAip(AipDto aip, Path tmpSipPath, byte[] xmlContent, List<StorageService> storageServices, String dataSpace) {
        String op = "storing AIP: ";
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicBoolean rollback = new AtomicBoolean(false);
        for (StorageService a : storageServices) {
            CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                        try (InputStream sipStream = new FileInputStream(tmpSipPath.toFile());
                             InputStream xmlStream = new ByteArrayInputStream(xmlContent)) {
                            a.storeAip(new AipDto(aip, sipStream, xmlStream), rollback, dataSpace);
                            log.info(strSA(a.getStorage().getName(), aip.getSip().getDatabaseId()) + op + "success");
                        } catch (StorageException e) {
                            log.warn(strSA(a.getStorage().getName(), aip.getSip().getDatabaseId()) + op + "error: " + e);
                            throw new GeneralException(e);
                        } catch (IOException e) {
                            throw new CantReadException("SIP tmp file at path " + tmpSipPath.toString() +
                                    " or stream of XML " + aip.getXml().getStorageId(), e);
                        }
                    }, executor
            );
            futures.add(c);
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
        } catch (InterruptedException e) {
            String s = op + "main thread has been interrupted";
            log.error(s);
            throw new GeneralException(s, e);
        } catch (ExecutionException e) {
            rollback.set(true);
            log.error(op + "some storage has encountered problem");
        } finally {
            try {
                Files.delete(tmpSipPath);
            } catch (IOException e) {
                log.error("Could not delete temporary file " + tmpSipPath);
            }
        }
        if (!rollback.get()) {
            archivalDbService.finishAipCreation(aip.getSip().getDatabaseId(), aip.getXml().getDatabaseId());
            log.info(strA(aip.getSip().getDatabaseId()) + op + "success on all storages");
            return;
        }
        log.info(op + "Archival storage error. Starting rollback.");
        futures = new ArrayList<>();
        for (StorageService a : storageServices) {
            CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                try {
                    a.rollbackAip(aip.getSip().getDatabaseId(), dataSpace);
                    log.warn(strSA(a.getStorage().getName(), aip.getSip().getDatabaseId()) + "rolled back");
                } catch (StorageException e) {
                    log.error(strSA(a.getStorage().getName(), aip.getSip().getDatabaseId()) + "rollback process error: " + e);
                    throw new GeneralException(e);
                }
            }, executor);
            futures.add(c);
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
            archivalDbService.rollbackAip(aip.getSip().getDatabaseId(), aip.getXml().getDatabaseId());
            log.info(strA(aip.getSip().getDatabaseId()) + "rollback successful on all storages.");
        } catch (InterruptedException e) {
            archivalDbService.setAipFailed(aip.getSip().getDatabaseId(), aip.getXml().getDatabaseId());
            log.error("Main thread has been interrupted during rollback.");
        } catch (ExecutionException e) {
            archivalDbService.setAipFailed(aip.getSip().getDatabaseId(), aip.getXml().getDatabaseId());
            log.error(strA(aip.getSip().getDatabaseId()) + "rollback failed on some storages: " + e);
        }
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
     */
    @Async
    public void saveObject(ArchivalObjectDto archivalObject, TmpSourceHolder tmpSourceHolder, List<StorageService> storageServices) {
        String op = "storing object: ";
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicBoolean rollback = new AtomicBoolean(false);
        for (StorageService a : storageServices) {
            CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                        try (InputStream objectStream = tmpSourceHolder.createInputStream()) {
                            ArchivalObjectDto archivalObjectCpy = new ArchivalObjectDto(archivalObject, objectStream);
                            a.storeObject(archivalObjectCpy, rollback, archivalObject.getOwner().getDataSpace());
                            log.info(strSX(a.getStorage().getName(), archivalObject.getStorageId()) + op + "success");
                        } catch (StorageException e) {
                            log.warn(strSX(a.getStorage().getName(), archivalObject.getStorageId()) + op + "error");
                            throw new GeneralException(e);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }, executor
            );
            futures.add(c);
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
        } catch (InterruptedException e) {
            String s = op + "main thread has been interrupted";
            log.error(s);
            throw new GeneralException(s, e);
        } catch (ExecutionException e) {
            rollback.set(true);
            log.error(op + "some storage has encountered problem");
        } finally {
            tmpSourceHolder.freeSpace();
        }

        if (!rollback.get()) {
            archivalDbService.setObjectState(archivalObject.getDatabaseId(), ObjectState.ARCHIVED);
            log.info(strX(archivalObject.getStorageId()) + op + "success on all storages");
            return;
        }
        log.info(op + "Archival storage error. Starting rollback.");
        futures = new ArrayList<>();
        for (StorageService a : storageServices) {
            CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                try {
                    a.rollbackObject(archivalObject.getStorageId(), archivalObject.getOwner().getDataSpace());
                    log.warn(strSX(a.getStorage().getName(), archivalObject.getStorageId()) + "rolled back");
                } catch (StorageException e) {
                    log.error(strSX(a.getStorage().getName(), archivalObject.getStorageId()) + "rollback process error");
                    throw new GeneralException(e);
                }
            }, executor);
            futures.add(c);
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
            archivalDbService.setObjectState(archivalObject.getDatabaseId(), ObjectState.ROLLED_BACK);
            log.info(strX(archivalObject.getStorageId()) + "rollback successful on all storages.");
        } catch (InterruptedException e) {
            archivalDbService.setObjectState(archivalObject.getDatabaseId(), ObjectState.ARCHIVAL_FAILURE);
            log.error("Main thread has been interrupted during rollback.");
        } catch (ExecutionException e) {
            archivalDbService.setObjectState(archivalObject.getDatabaseId(), ObjectState.ARCHIVAL_FAILURE);
            log.error(strX(archivalObject.getStorageId()) + "rollback failed on some storages.");
        }
    }

    /**
     * Deletes object at the provide storage services.
     * <p>
     * In case of of error during the deletion process at any of the storage,
     * state of the object in DB is set to DELETION_FAILURE.
     *
     * @param archivalObjectDto DTO with the object to delete
     * @param storageServices   storage services to delete the object from
     */
    @Async
    public void deleteObject(ArchivalObjectDto archivalObjectDto, List<StorageService> storageServices) {
        String op = "deleting object: ";
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        boolean successOnAllStorages = true;
        for (StorageService a : storageServices) {
            CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                        try {
                            a.delete(archivalObjectDto.getStorageId(), archivalObjectDto.getOwner().getDataSpace());
                            log.info(strSX(a.getStorage().getName(), archivalObjectDto.getStorageId()) + op + "success");
                        } catch (StorageException e) {
                            log.warn(strSX(a.getStorage().getName(), archivalObjectDto.getStorageId()) + op + "error");
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
            log.error(s);
            throw new GeneralException(s, e);
        } catch (ExecutionException e) {
            successOnAllStorages = false;
            log.error(op + "some storage has encountered problem");
        }
        if (successOnAllStorages) {
            log.info(strX(archivalObjectDto.getStorageId()) + op + "success on all storages");
            return;
        }
        archivalDbService.setObjectState(archivalObjectDto.getDatabaseId(), ObjectState.DELETION_FAILURE);
        log.error(strX(archivalObjectDto.getStorageId()) + "deletion failed on some storages");
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
        for (ArchivalObject object : objects) {
            String storageId = object instanceof AipXml ? toXmlId(((AipXml) object).getSip().getId(), ((AipXml) object).getVersion()) : object.getId();
            boolean rollback = object.getState() != ObjectState.DELETION_FAILURE;
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            boolean successOnAllStorages = true;
            for (StorageService a : storageServices) {
                CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                            try {
                                if (rollback)
                                    a.rollbackObject(storageId, object.getOwner().getDataSpace());
                                else
                                    a.delete(storageId, object.getOwner().getDataSpace());
                                log.debug(strSX(a.getStorage().getName(), storageId) + op + "success");
                            } catch (StorageException e) {
                                log.warn(strSX(a.getStorage().getName(), storageId) + op + "error");
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
                log.error(s);
                throw new GeneralException(s, e);
            } catch (ExecutionException e) {
                successOnAllStorages = false;
            }
            if (successOnAllStorages) {
                if (rollback)
                    rolledBackObjects.add(object);
                else
                    deletedObjects.add(object);
                continue;
            }
            failedObjects.add(object);
        }
        log.info("cleanup finished");
        if (!rolledBackObjects.isEmpty()) {
            archivalDbService.setObjectsState(ObjectState.ROLLED_BACK, rolledBackObjects.stream().map(DomainObject::getId).collect(Collectors.toList()));
            log.info("successfully rolled back " + rolledBackObjects.size() + " objects");
            log.debug("rolled back objects: " + Arrays.toString(rolledBackObjects.toArray()));
        }
        if (!deletedObjects.isEmpty()) {
            archivalDbService.setObjectsState(ObjectState.DELETED, deletedObjects.stream().map(DomainObject::getId).collect(Collectors.toList()));
            log.info("successfully deleted " + deletedObjects.size() + " objects");
            log.debug("deleted objects: " + Arrays.toString(deletedObjects.toArray()));
        }
        if (!failedObjects.isEmpty()) {
            log.error("unable to cleanup " + failedObjects.size() + " objects: " + Arrays.toString(failedObjects.toArray()));
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
}
