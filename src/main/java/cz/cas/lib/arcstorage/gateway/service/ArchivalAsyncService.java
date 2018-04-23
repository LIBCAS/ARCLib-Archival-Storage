package cz.cas.lib.arcstorage.gateway.service;


import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.gateway.dto.AipRef;
import cz.cas.lib.arcstorage.gateway.dto.FileRef;
import cz.cas.lib.arcstorage.gateway.dto.XmlRef;
import cz.cas.lib.arcstorage.gateway.exception.CantReadException;
import cz.cas.lib.arcstorage.gateway.exception.CantWriteException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.StorageUtils;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.store.StorageConfigStore;
import cz.cas.lib.arcstorage.store.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.util.Utils.*;

@Service
@Slf4j
public class ArchivalAsyncService {

    private ArchivalDbService archivalDbService;
    private StorageConfigStore storageConfigStore;
    private ExecutorService executor;
    private Path tmpFolder;

    @Async
    @Transactional
    public void store(AipRef aip) {
        String op = "AIP storage ";
        Path tmpXmlPath = tmpFolder.resolve(aip.getXml().getId());
        Path tmpSipPath = tmpFolder.resolve(aip.getSip().getId());
        try {
            Files.copy(aip.getXml().getInputStream(), tmpXmlPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new CantWriteException(tmpXmlPath.toString(), e);
        }
        try {
            Files.copy(aip.getSip().getInputStream(), tmpSipPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new CantWriteException(tmpSipPath.toString(), e);
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicBoolean rollback = new AtomicBoolean(false);
        List<StorageService> adapters = storageConfigStore.findAll().stream().map(StorageUtils::createAdapter).collect(Collectors.toList());
        for (StorageService a : adapters) {
            CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                        try (BufferedInputStream sipStream = new BufferedInputStream(new FileInputStream(tmpSipPath.toFile()));
                             BufferedInputStream xmlStream = new BufferedInputStream(new FileInputStream(tmpXmlPath.toFile()))) {
                            a.storeAip(new AipRef(aip, new FileRef(sipStream), new FileRef(xmlStream)), rollback);
                            log.info(strSA(a.getStorageConfig().getName(), aip.getSip().getId()) + op + "success");
                        } catch (StorageException e) {
                            log.warn(strSA(a.getStorageConfig().getName(), aip.getSip().getId()) + op + "error: " + e);
                            throw new GeneralException(e);
                        } catch (IOException e) {
                            throw new CantReadException(tmpSipPath.toString() + " or " + tmpXmlPath.toString(), e);
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
                Files.delete(tmpXmlPath);
                Files.delete(tmpSipPath);
            } catch (IOException e) {
                log.error("Could not delete temporary files " + tmpSipPath + " " + tmpXmlPath);
            }
        }
        if (!rollback.get()) {
            archivalDbService.finishAipCreation(aip.getSip().getId(), aip.getXml().getId());
            log.info(strA(aip.getSip().getId()) + op + "success on all storages");
            return;
        }
        log.info(op + "Archival storage error. Starting rollback.");
        futures = new ArrayList<>();
        for (StorageService a : adapters) {
            CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                try {
                    a.rollbackAip(aip.getSip().getId());
                    log.warn(strSA(a.getStorageConfig().getName(), aip.getSip().getId()) + "rollbacked");
                } catch (StorageException e) {
                    log.error(strSA(a.getStorageConfig().getName(), aip.getSip().getId()) + "rollback process error: " + e);
                    throw new GeneralException(e);
                }
            }, executor);
            futures.add(c);
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
            archivalDbService.rollbackSip(aip.getSip().getId(), aip.getXml().getId());
            log.info(strA(aip.getSip().getId()) + "rollback successful on all storages.");
        } catch (InterruptedException e) {
            archivalDbService.registerSipAndXmlRollback(aip.getSip().getId(), aip.getXml().getId());

            log.error("Main thread has been interrupted during rollback.");
        } catch (ExecutionException e) {
            archivalDbService.registerSipAndXmlRollback(aip.getSip().getId(), aip.getXml().getId());

            log.error(strA(aip.getSip().getId()) + "rollback failed on some storages: " + e);
        }
    }

    @Async
    public void updateXml(String sipId, XmlRef xml) {
        String op = "AIP storage ";
        Path tmpXmlPath = tmpFolder.resolve(xml.getId());
        try {
            Files.copy(xml.getInputStream(), tmpXmlPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new CantWriteException(tmpXmlPath.toString(), e);
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicBoolean rollback = new AtomicBoolean(false);
        List<StorageService> adapters = storageConfigStore.findAll().stream().map(StorageUtils::createAdapter).collect(Collectors.toList());
        for (StorageService a : adapters) {
            CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                        try (BufferedInputStream xmlStream = new BufferedInputStream(new FileInputStream(tmpXmlPath.toFile()))) {
                            a.storeXml(sipId, new XmlRef(xml, new FileRef(xmlStream)), rollback);
                            log.info(strSX(a.getStorageConfig().getName(), xml.getId()) + op + "success");
                        } catch (StorageException e) {
                            log.warn(strSX(a.getStorageConfig().getName(), xml.getId()) + op + "error");
                            throw new GeneralException(e);
                        } catch (IOException e) {
                            throw new CantReadException(tmpXmlPath.toString(), e);
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
                Files.delete(tmpXmlPath);
            } catch (IOException e) {
                log.error("Could not delete temporary file " + tmpXmlPath);
            }
        }

        if (!rollback.get()) {
            archivalDbService.finishXmlProcess(xml.getId());
            log.info(strX(xml.getId()) + op + "success on all storages");
            return;
        }
        log.info(op + "Archival storage error. Starting rollback.");
        futures = new ArrayList<>();
        for (StorageService a : adapters) {
            CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                try {
                    a.rollbackXml(xml.getId(), xml.getVersion());
                    log.warn(strSX(a.getStorageConfig().getName(), xml.getId()) + "rollbacked");
                } catch (StorageException e) {
                    log.error(strSX(a.getStorageConfig().getName(), xml.getId()) + "rollback process error");
                    throw new GeneralException(e);
                }
            }, executor);
            futures.add(c);
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
            archivalDbService.rollbackXml(xml.getId());
            log.info(strX(xml.getId()) + "rollback successful on all storages.");
        } catch (InterruptedException e) {
            log.error("Main thread has been interrupted during rollback.");
        } catch (ExecutionException e) {
            log.error(strX(xml.getId()) + "rollback failed on some storages.");
        }
    }


    @Async
    public void delete(String sipId) throws StorageException {
        try {
            for (StorageService storageService : storageConfigStore.findAll().stream().map(StorageUtils::createAdapter).collect(Collectors.toList())) {
                storageService.deleteSip(sipId);
            }
        } catch (StorageException e) {
            log.error("Storage error has occurred during deletion of AIP: " + sipId);
            throw e;
        }
        archivalDbService.finishSipDeletion(sipId);
        log.info("AIP: " + sipId + " has been successfully deleted.");
    }

    @Async
    public void remove(String sipId) throws StorageException {
        try {
            for (StorageService storageService : storageConfigStore.findAll().stream().map(StorageUtils::createAdapter).collect(Collectors.toList())) {
                storageService.remove(sipId);
            }
        } catch (StorageException e) {
            log.error("Storage error has occurred during removal of AIP: " + sipId + ". AIP is marked as removed in DB but may not be marked as removed on every storage.");
            throw e;
        }
        log.info("AIP: " + sipId + " has been successfully removed.");
    }

    @Inject
    public void setArchivalDbService(ArchivalDbService archivalDbService) {
        this.archivalDbService = archivalDbService;
    }

    @Inject
    public void setStorageConfigStore(StorageConfigStore storageConfigStore) {
        this.storageConfigStore = storageConfigStore;
    }

    @Inject
    public void setTmpFolder(@Value("${arcstorage.tmp-folder}") String path) {
        this.tmpFolder = Paths.get(path);
    }

    @Inject
    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }
}
