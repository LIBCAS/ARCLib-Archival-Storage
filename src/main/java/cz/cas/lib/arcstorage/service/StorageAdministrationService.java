package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.domain.store.ArchivalObjectStore;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.dto.StorageStateDto;
import cz.cas.lib.arcstorage.security.user.UserStore;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storagesync.newstorage.StorageSyncPhase;
import cz.cas.lib.arcstorage.storagesync.newstorage.StorageSyncService;
import cz.cas.lib.arcstorage.storagesync.newstorage.StorageSyncStatus;
import cz.cas.lib.arcstorage.storagesync.newstorage.StorageSyncStatusStore;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.CantCreateDataspaceException;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.StorageStillProcessObjectsException;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.SynchronizationInProgressException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class StorageAdministrationService {
    private StorageStore storageStore;
    private StorageSyncStatusStore syncStatusStore;
    private StorageSyncService storageSyncService;
    private StorageProvider storageProvider;
    private SystemStateService systemStateService;
    private UserStore userStore;
    private ArchivalObjectStore archivalObjectStore;
    private int transactionTimeoutSeconds;
    private int synchronizationInitTimeoutSeconds;

    public Collection<Storage> getAll() {
        return storageStore.findAll();
    }

    /**
     * Starts synchronization process of a new storage.
     * 1) tests that new storage is reachable
     * 2) sets whole archival storage to read-only mode
     * 3) checks availability of the new storage
     * 4) waits {@link StorageAdministrationService#transactionTimeoutSeconds} for already started DB transactions to finish
     * (to make sure that there is no transaction which would successfully add package while the sync is initializing)
     * 5) tries to start the synchronization - waits max {@link StorageAdministrationService#synchronizationInitTimeoutSeconds}
     * for packages which are currently ingesting to finish
     * 6) create data spaces (i.e. user specific folder/bucket...) for all registered user accounts at the new storage
     * 7) sets whole archival storage to read-write mode
     * 8) starts synchronization phase 1 i.e. copying of all currently archived packages
     * <p>
     * It is important to have the {@link StorageSyncPhase#INIT} persisted in DB so that if other thread
     * calls {@link StorageSyncStatusStore#anyInInitialOrFinishingPhase()} it may see the INIT status.
     * That is the reason why the whole {@link #attachStorage(Storage)} is not Transactional.
     * Otherwise the {@link StorageSyncService#setReadWriteConfig(SystemState)} method could set the storage to readwrite just in the same time this method sets it to readonly, which would break the logic of this method.
     *
     * @param storage
     * @return
     * @throws SomeLogicalStoragesNotReachableException
     * @throws InterruptedException
     * @throws StorageStillProcessObjectsException
     * @throws IOStorageException
     * @throws SynchronizationInProgressException
     */
    public Storage attachStorage(Storage storage) throws SomeLogicalStoragesNotReachableException, InterruptedException,
            StorageStillProcessObjectsException, IOStorageException, SynchronizationInProgressException, CantCreateDataspaceException {
        storage.setSynchronizing(true);

        log.info("attaching " + storage);
        SystemState systemState = systemStateService.get();
        if (systemState.isReadOnly())
            throw new ReadOnlyStateException();
        log.debug("checking config validity and reachability of " + storage);

        StorageService destinationStorageService;
        try {
            destinationStorageService = storageProvider.createAdapter(storage, false);
        } catch (Exception e) {
            log.error("Could not create storage service for  " + storage);
            throw e;
        }
        storage.setReachable(destinationStorageService.testConnection());
        if (!storage.isReachable()) {
            log.error("Storage " + storage + " not reachable.");
            throw new SomeLogicalStoragesNotReachableException(destinationStorageService.getStorage());
        }
        log.debug(storage + " reachable, starting preparation for synchronization");
        systemState.setReadOnly(true);
        systemStateService.save(systemState);
        log.info("system set to read-only mode");
        storageStore.save(storage);
        StorageSyncStatus status = new StorageSyncStatus(storage);
        syncStatusStore.save(status);

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
                setReadWriteConfig(systemState);
                throw new StorageStillProcessObjectsException(processingObjects);
            }
            Thread.sleep(1000);
            waitedSeconds++;
            processingObjects = archivalObjectStore.findProcessingObjects();
        }
        Set<String> dataSpaces = userStore.findAll().stream().map(User::getDataSpace).filter(Objects::nonNull).collect(Collectors.toSet());
        String currentDataspace = "";
        try {
            log.debug("creating dataspaces for storage " + storage);
            for (String dataSpace : dataSpaces) {
                currentDataspace = dataSpace;
                log.debug("creating dataspace:" + dataSpace);
                destinationStorageService.createNewDataSpace(dataSpace);
            }
        } catch (IOStorageException e) {
            logInitError(storage);
            storageStore.delete(storage);
            syncStatusStore.delete(status);
            setReadWriteConfig(systemState);
            throw new CantCreateDataspaceException(currentDataspace, e);
        }
        status.setCreated(Instant.now());
        status.setPhase(StorageSyncPhase.COPYING_ARCHIVED_OBJECTS);
        syncStatusStore.save(status);
        setReadWriteConfig(systemState);
        log.info(StorageSyncPhase.INIT + " synchronization phase of " + storage + " has ended");
        synchronizeStorage(status, true);
        return storage;
    }

    public StorageStateDto getStorageState(String storageId) {
        StorageService adapter = storageProvider.createAdapter(storageId);
        Storage storage = adapter.getStorage();
        if (!storage.isReachable())
            return new StorageStateDto(storage, Collections.singletonMap("state", "unreachable"));
        try {
            return adapter.getStorageState();
        } catch (Exception e) {
            log.error("Error occurred during state retrieval", e);
            return new StorageStateDto(storage, Collections.singletonMap("error", "Error occurred during state retrieval: " + e.toString() + " see log for more information"));
        }
    }

    /**
     * @param syncStatus
     * @param firstTime  true if this is called from {@link StorageAdministrationService#attachStorage(Storage)} i.e. this is first attempt to synchronize (in that case phase is always {@link StorageSyncPhase#COPYING_ARCHIVED_OBJECTS}),
     *                   false if this is called from {@link cz.cas.lib.arcstorage.api.StorageAdministrationApi#continueSync(String)} i.e. there
     *                   was synchronization error and this is attempt to continue with synchronization started earlier
     * @throws SomeLogicalStoragesNotReachableException
     * @throws SynchronizationInProgressException
     * @throws InterruptedException
     */
    public void synchronizeStorage(StorageSyncStatus syncStatus, boolean firstTime) throws SomeLogicalStoragesNotReachableException,
            SynchronizationInProgressException, InterruptedException {
        if (syncStatus.getPhase() == StorageSyncPhase.DONE || syncStatus.getPhase() == null) {
            log.debug("attempt to continue with synchronization of storage " + syncStatus.getStorage().getId() + " silently skipped - the storage is already synced");
            return;
        }
        if (!firstTime && syncStatus.getStuckAt() == null)
            throw new SynchronizationInProgressException(syncStatus);

        StorageService destinationStorageService = storageProvider.createAdapter(syncStatus.getStorage().getId());
        if (!destinationStorageService.getStorage().isReachable()) {
            SomeLogicalStoragesNotReachableException ex = new SomeLogicalStoragesNotReachableException(destinationStorageService.getStorage());
            syncStatus.setExceptionInfo(ex);
            log.error("Storage " + syncStatus.getStorage().getId() + " not reachable. Synchronization failed.");
            syncStatusStore.save(syncStatus);
            throw ex;
        }

        switch (syncStatus.getPhase()) {
            case COPYING_ARCHIVED_OBJECTS:
                storageSyncService.copyArchivedObjects(destinationStorageService, syncStatus);
                break;
            case PROPAGATING_OPERATIONS:
                storageSyncService.propagateOperationsOfModification(destinationStorageService, syncStatus, syncStatus.getStuckAt());
                break;
            case POST_SYNC_CHECK:
                storageSyncService.postSyncCheck(destinationStorageService, syncStatus);
        }
    }

    private void setReadWriteConfig(SystemState systemState) {
        if (!syncStatusStore.anyInInitialOrFinishingPhase()) {
            systemState.setReadOnly(false);
            systemStateService.save(systemState);
            log.info("system set to read-write mode");
        }
    }

    private void logInitError(Storage storage) {
        log.error("error occurred during initial phase of synchronizing storage: " + storage + " the storage and its status entities will be deleted");
    }

    @Autowired
    public void setSyncStatusStore(StorageSyncStatusStore syncStatusStore) {
        this.syncStatusStore = syncStatusStore;
    }

    @Autowired
    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }

    @Autowired
    public void setStorageSyncService(StorageSyncService storageSyncService) {
        this.storageSyncService = storageSyncService;
    }

    @Autowired
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    @Autowired
    public void setUserStore(UserStore userStore) {
        this.userStore = userStore;
    }

    @Autowired
    public void setSystemStateService(SystemStateService systemStateService) {
        this.systemStateService = systemStateService;
    }

    @Autowired
    public void setArchivalObjectStore(ArchivalObjectStore archivalObjectStore) {
        this.archivalObjectStore = archivalObjectStore;
    }

    @Autowired
    public void setTransactionTimeoutSeconds(@Value("${arcstorage.stateChangeTransactionTimeout}") int transactionTimeoutSeconds) {
        this.transactionTimeoutSeconds = transactionTimeoutSeconds;
    }

    @Autowired
    public void setSynchronizationInitTimeoutSeconds(@Value("${arcstorage.synchronizationInitTimeout}") int synchronizationInitTimeoutSeconds) {
        this.synchronizationInitTimeoutSeconds = synchronizationInitTimeoutSeconds;
    }
}
