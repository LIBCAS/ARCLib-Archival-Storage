package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.Configuration;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.domain.store.ArchivalObjectStore;
import cz.cas.lib.arcstorage.domain.store.ConfigurationStore;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.security.user.UserStore;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storagesync.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Slf4j
public class StorageAdministrationService {
    private StorageStore storageStore;
    private StorageSyncStatusStore syncStatusStore;
    private StorageSyncService storageSyncService;
    private StorageProvider storageProvider;
    private ConfigurationStore configurationStore;
    private UserStore userStore;
    private ArchivalObjectStore archivalObjectStore;
    private int transactionTimeoutSeconds;
    private int synchronizationInitTimeoutSeconds;

    public Storage attachStorage(Storage storage) throws SomeLogicalStoragesNotReachableException, InterruptedException,
            StorageStillProcessObjectsException, IOStorageException, SynchronizationInProgressException {
        log.info("attaching " + storage);
        storage.setWriteOnly(true);
        storage = storageStore.save(storage);
        StorageService destinationStorageService = storageProvider.createAdapter(storage.getId());
        if (!destinationStorageService.getStorage().isReachable()) {
            logInitError(storage);
            storageStore.delete(storage);
            throw new SomeLogicalStoragesNotReachableException(destinationStorageService.getStorage());
        }
        log.debug(storage + " reachable, starting preparation for synchronization");
        StorageSyncStatus status = new StorageSyncStatus(storage);
        syncStatusStore.save(status);
        Configuration configuration = configurationStore.get();
        configuration.setReadOnly(true);
        configurationStore.save(configuration);
        log.info("system set to read-only mode");

        Thread.sleep(transactionTimeoutSeconds * 1000);
        List<ArchivalObject> processingObjects = archivalObjectStore.findProcessingObjects();
        int waitedSeconds = transactionTimeoutSeconds;
        while (!processingObjects.isEmpty()) {
            log.debug("cant start synchronization because of objects which are still processing: " +
                    Arrays.toString(processingObjects.toArray()) + " Archival storage will wait max. " + synchronizationInitTimeoutSeconds +
                    " seconds for processing objects to finish. Already waited " + waitedSeconds + " seconds");
            if (waitedSeconds > synchronizationInitTimeoutSeconds) {
                logInitError(storage);
                storageStore.delete(storage);
                syncStatusStore.delete(status);
                setReadWriteConfig(configuration);
                throw new StorageStillProcessObjectsException(processingObjects);
            }
            Thread.sleep(1000);
            waitedSeconds++;
            processingObjects = archivalObjectStore.findProcessingObjects();
        }
        Set<String> dataSpaces = userStore.findAll().stream().map(User::getDataSpace).filter(Objects::nonNull).collect(Collectors.toSet());
        try {
            log.debug("creating dataspaces for storage " + storage);
            for (String dataSpace : dataSpaces) {
                destinationStorageService.createNewDataSpace(dataSpace);
            }
        } catch (IOStorageException e) {
            logInitError(storage);
            storageStore.delete(storage);
            syncStatusStore.delete(status);
            setReadWriteConfig(configuration);
            throw e;
        }
        status.setCreated(Instant.now());
        status.setPhase(StorageSyncPhase.COPYING_ARCHIVED_OBJECTS);
        syncStatusStore.save(status);
        setReadWriteConfig(configuration);
        log.info(StorageSyncPhase.INIT + " synchronization phase of " + storage + " has ended");
        synchronizeStorage(status, true);
        return storage;
    }

    public void synchronizeStorage(StorageSyncStatus syncStatus, boolean firstTime) throws SomeLogicalStoragesNotReachableException,
            SynchronizationInProgressException, InterruptedException {
        if (syncStatus.getPhase() == StorageSyncPhase.DONE || syncStatus.getPhase() == null)
            return;
        if (!firstTime && syncStatus.getStuckAt() == null)
            throw new SynchronizationInProgressException(syncStatus);

        StorageService destinationStorageService = storageProvider.createAdapter(syncStatus.getStorage().getId());
        if (!destinationStorageService.getStorage().isReachable()) {
            throw new SomeLogicalStoragesNotReachableException(destinationStorageService.getStorage());
        }

        switch (syncStatus.getPhase()) {
            case COPYING_ARCHIVED_OBJECTS:
                storageSyncService.copyStoragePhase1(destinationStorageService, syncStatus);
                break;
            case PROPAGATING_OPERATIONS:
                storageSyncService.copyStoragePhase2(destinationStorageService, syncStatus);
                break;
        }
    }

    private void setReadWriteConfig(Configuration configuration) {
        if (!syncStatusStore.anyInInitialOrFinishingPhase()) {
            configuration.setReadOnly(false);
            configurationStore.save(configuration);
            log.info("system set to read-write mode");
        }
    }

    private void logInitError(Storage storage) {
        log.debug("error occurred during initial phase of synchronizing storage: " + storage + " the storage and its status entities will be deleted");
    }

    @Inject
    public void setSyncStatusStore(StorageSyncStatusStore syncStatusStore) {
        this.syncStatusStore = syncStatusStore;
    }

    @Inject
    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }

    @Inject
    public void setStorageSyncService(StorageSyncService storageSyncService) {
        this.storageSyncService = storageSyncService;
    }

    @Inject
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    @Inject
    public void setUserStore(UserStore userStore) {
        this.userStore = userStore;
    }

    @Inject
    public void setConfigurationStore(ConfigurationStore configurationStore) {
        this.configurationStore = configurationStore;
    }

    @Inject
    public void setArchivalObjectStore(ArchivalObjectStore archivalObjectStore) {
        this.archivalObjectStore = archivalObjectStore;
    }

    @Inject
    public void setTransactionTimeoutSeconds(@Value("${arcstorage.state-change-transaction-timeout}") int transactionTimeoutSeconds) {
        this.transactionTimeoutSeconds = transactionTimeoutSeconds;
    }

    @Inject
    public void setSynchronizationInitTimeoutSeconds(@Value("${arcstorage.synchronization-init-timeout}") int synchronizationInitTimeoutSeconds) {
        this.synchronizationInitTimeoutSeconds = synchronizationInitTimeoutSeconds;
    }
}
