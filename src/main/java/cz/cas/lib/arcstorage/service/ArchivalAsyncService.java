package cz.cas.lib.arcstorage.service;


import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.dto.AipDto;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.dto.TmpSourceHolder;
import cz.cas.lib.arcstorage.exception.GeneralException;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static cz.cas.lib.arcstorage.util.Utils.*;

@Service
@Slf4j
public class ArchivalAsyncService {

    private ArchivalDbService archivalDbService;
    private ExecutorService executor;

    @Async
    @Transactional
    public void saveAip(AipDto aip, Path tmpSipPath, byte[] xmlContent, List<StorageService> storageServices) {
        String op = "AIP storage ";
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicBoolean rollback = new AtomicBoolean(false);
        for (StorageService a : storageServices) {
            CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                        try (InputStream sipStream = new FileInputStream(tmpSipPath.toFile());
                             InputStream xmlStream = new ByteArrayInputStream(xmlContent)) {
                            a.storeAip(new AipDto(aip, sipStream, xmlStream), rollback);
                            log.info(strSA(a.getStorage().getName(), aip.getSip().getId()) + op + "success");
                        } catch (StorageException e) {
                            log.warn(strSA(a.getStorage().getName(), aip.getSip().getId()) + op + "error: " + e);
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
                log.error("Could not deleteAip temporary file " + tmpSipPath);
            }
        }
        if (!rollback.get()) {
            archivalDbService.finishAipCreation(aip.getSip().getId(), aip.getXml().getDatabaseId());
            log.info(strA(aip.getSip().getId()) + op + "success on all storages");
            return;
        }
        log.info(op + "Archival storage error. Starting rollback.");
        futures = new ArrayList<>();
        for (StorageService a : storageServices) {
            CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                try {
                    a.rollbackAip(aip.getSip().getId());
                    log.warn(strSA(a.getStorage().getName(), aip.getSip().getId()) + "rolled back");
                } catch (StorageException e) {
                    log.error(strSA(a.getStorage().getName(), aip.getSip().getId()) + "rollback process error: " + e);
                    throw new GeneralException(e);
                }
            }, executor);
            futures.add(c);
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
            archivalDbService.rollbackAip(aip.getSip().getId(), aip.getXml().getDatabaseId());
            log.info(strA(aip.getSip().getId()) + "rollback successful on all storages.");
        } catch (InterruptedException e) {
            archivalDbService.setAipFailed(aip.getSip().getId(), aip.getXml().getDatabaseId());
            log.error("Main thread has been interrupted during rollback.");
        } catch (ExecutionException e) {
            archivalDbService.setAipFailed(aip.getSip().getId(), aip.getXml().getDatabaseId());
            log.error(strA(aip.getSip().getId()) + "rollback failed on some storages: " + e);
        }
    }

    @Async
    public void saveObject(ArchivalObjectDto archivalObject, ObjectType objectType, TmpSourceHolder tmpSourceHolder, List<StorageService> storageServices) {
        String op = "AIP storage ";
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicBoolean rollback = new AtomicBoolean(false);
        for (StorageService a : storageServices) {
            CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                        try (InputStream objectStream = tmpSourceHolder.createInputStream()) {
                            ArchivalObjectDto archivalObjectCpy = new ArchivalObjectDto(archivalObject.getDatabaseId(), archivalObject.getStorageId(), objectStream, archivalObject.getChecksum());
                            a.storeObject(archivalObjectCpy, rollback);
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
            archivalDbService.setObjectState(archivalObject.getDatabaseId(), objectType, ObjectState.ARCHIVED);
            log.info(strX(archivalObject.getStorageId()) + op + "success on all storages");
            return;
        }
        log.info(op + "Archival storage error. Starting rollback.");
        futures = new ArrayList<>();
        for (StorageService a : storageServices) {
            CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                try {
                    a.rollbackObject(archivalObject.getStorageId());
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
            archivalDbService.setObjectState(archivalObject.getDatabaseId(), objectType, ObjectState.ROLLED_BACK);
            log.info(strX(archivalObject.getStorageId()) + "rollback successful on all storages.");
        } catch (InterruptedException e) {
            archivalDbService.setObjectState(archivalObject.getDatabaseId(), objectType, ObjectState.FAILED);
            log.error("Main thread has been interrupted during rollback.");
        } catch (ExecutionException e) {
            archivalDbService.setObjectState(archivalObject.getDatabaseId(), objectType, ObjectState.FAILED);
            log.error(strX(archivalObject.getStorageId()) + "rollback failed on some storages.");
        }
    }

    @Async
    public void deleteAip(String sipId, List<StorageService> storageServices) throws StorageException {
        try {
            for (StorageService storageService : storageServices) {
                storageService.deleteSip(sipId);
            }
        } catch (StorageException e) {
            log.error("Storage error has occurred during deletion of AIP: " + sipId);
            throw e;
        }
        archivalDbService.setObjectState(sipId, ObjectType.SIP, ObjectState.DELETED);
        log.info("AIP: " + sipId + " has been successfully deleted.");
    }

    @Async
    public void removeAip(String sipId, List<StorageService> storageServices) throws StorageException {
        try {
            for (StorageService storageService : storageServices) {
                storageService.remove(sipId);
            }
        } catch (StorageException e) {
            log.error("Storage error has occurred during removal of AIP: " + sipId + ". AIP is marked as removed in DB but may not be marked as removed on every storage.");
            throw e;
        }
        log.info("AIP: " + sipId + " has been successfully removed.");
    }

    @Async
    public void renewAip(String sipId, List<StorageService> storageServices) throws StorageException {
        try {
            for (StorageService storageService : storageServices) {
                storageService.renew(sipId);
            }
        } catch (StorageException e) {
            log.error("Storage error has occurred during renewing of AIP: " + sipId + ". AIP is marked as archived in DB but may not be marked as archived on every storage.");
            throw e;
        }
        log.info("AIP: " + sipId + " has been successfully removed.");
    }

    @Inject
    public void setArchivalDbService(ArchivalDbService archivalDbService) {
        this.archivalDbService = archivalDbService;
    }

    @Inject
    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }
}
