package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.domain.store.ArchivalObjectLightweightViewStore;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.domain.views.ArchivalObjectLightweightView;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.StorageStateDto;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.jms.JmsHealthCheckException;
import cz.cas.lib.arcstorage.jms.JmsQueueManager;
import cz.cas.lib.arcstorage.jms.JmsQueueNotEmptyException;
import cz.cas.lib.arcstorage.jms.JmsSender;
import cz.cas.lib.arcstorage.jms.context.StoragesContextRegistry;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.security.user.UserStore;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storagesync.CommonSyncService;
import cz.cas.lib.arcstorage.storagesync.newstorage.StorageSyncStatus;
import cz.cas.lib.arcstorage.storagesync.newstorage.StorageSyncStatusStore;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.CantCreateDataspaceException;
import jakarta.jms.JMSException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.util.Utils.executeAfterTransactionCommits;
import static cz.cas.lib.arcstorage.util.Utils.notNull;

@Service
@Slf4j
public class StorageAdministrationService {
    private StorageStore storageStore;
    private StorageProvider storageProvider;
    private SystemStateService systemStateService;
    private UserStore userStore;
    private TransactionTemplate rrTransactionTemplate;
    private JmsQueueManager storageQueueManager;
    private ArchivalObjectLightweightViewStore archivalObjectLightweightViewStore;
    private JmsSender jmsSender;
    private StoragesContextRegistry registry;
    private StorageSyncStatusStore storageSyncStatusStore;
    private CommonSyncService commonSyncService;
    private ArcstorageMailCenter arcstorageMailCenter;

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
     * for packages which are currently processing to finish
     * 6) create data spaces (i.e. user specific folder/bucket...) for all registered user accounts at the new storage
     * 7) moves all existing objects in their state to queue of the new storage
     * 8) sets whole archival storage to read-write mode
     * <p>
     *
     * @param storage
     * @return
     * @throws SomeLogicalStoragesNotReachableException
     * @throws InterruptedException
     */
    public Storage attachNewStorage(Storage storage) throws SomeLogicalStoragesNotReachableException, InterruptedException, CantCreateDataspaceException, JmsQueueNotEmptyException, JmsHealthCheckException {

        log.info("attaching " + storage);
        SystemState systemState = systemStateService.get();
        if (systemState.isReadOnly())
            throw new ReadOnlyStateException();

        jmsSender.healthCheck();

        log.debug("checking config validity and reachability of " + storage);
        StorageService destinationStorageService = commonSyncService.createStorageServiceInReadonlyVacuum(systemState, storage);
        storageStore.save(storage);

        Set<String> dataSpaces = userStore.findAll().stream().map(User::getDataSpace).filter(Objects::nonNull).collect(Collectors.toSet());
        String currentDataspace = "";
        try {
            log.debug("creating dataspaces for storage " + storage);
            for (String dataSpace : dataSpaces) {
                currentDataspace = dataSpace;
                log.debug("creating dataspace:" + dataSpace);
                destinationStorageService.createNewDataSpace(dataSpace);
            }
            log.debug("creating queue for storage " + storage);
            storageQueueManager.startListener(storage.getId());
        } catch (IOStorageException e) {
            logInitError(storage);
            deleteStorage(storage);
            systemStateService.setReadWrite(systemState);

            throw new CantCreateDataspaceException(currentDataspace, e);
        }
        String primaryStorage = storageProvider.getPrimaryStorage().getId();
        registry.registerStorage(storage.getId(), Objects.equals(storage.getId(), primaryStorage));
        Instant objectStateObtainedAt = Instant.now();

        log.debug("moving objects to queue");
        List<ArchivalObjectLightweightView> objs = archivalObjectLightweightViewStore.findObjectsForNewStorage(null, null);
        List<ArchivalObjectDto> archivalObjectDtos = objs.stream().map(ArchivalObjectLightweightView::toDto).toList();

        StorageSyncStatus ss = new StorageSyncStatus(storage);
        ss.setTotal(objs.size());
        storageSyncStatusStore.save(ss);

        for (ArchivalObjectDto object : archivalObjectDtos) {
            jmsSender.copyObject(storage.getId(), object, objectStateObtainedAt);
        }
        log.info("{} objects moved to queue, setting readwrite mode", archivalObjectDtos.size());
        systemStateService.setReadWrite(systemState);
        return storage;
    }

    /**
     * Detaches storage in new transaction, no more data will be written to it.
     * <p>
     * If the storage is already detached, there is no action.
     * </p>
     *
     * @param storageId storage to detach
     * @param error     null if this is managed detach, exception if this is detach fired by error
     */
    synchronized public void detachStorage(String storageId, Throwable error) {
        Storage storage = storageStore.find(storageId);
        rrTransactionTemplate.executeWithoutResult(t -> {
            Storage storageInDb = storageStore.find(storage.getId());
            if (error != null) {
                if (storageInDb.getDetachedByError() == null) {
                    storage.setDetachedByError(Instant.now());
                    storageStore.save(storage);
                    StorageSyncStatus status = storageSyncStatusStore.findSyncStatusOfStorage(storageId);
                    if (status != null && !status.isFinished()) {
                        status.setExceptionInfo(error);
                        storageSyncStatusStore.save(status);
                        executeAfterTransactionCommits(() -> arcstorageMailCenter.sendStorageSynchronizationError(status));
                    } else {
                        executeAfterTransactionCommits(() -> arcstorageMailCenter.sendStorageDetachedByError(storage, error));
                    }
                }
            } else {
                if (storageInDb.getDetachedByAdmin() == null) {
                    storage.setDetachedByAdmin(Instant.now());
                    storageStore.save(storage);
                }
            }
            storageQueueManager.stopListener(storage.getId());
        });
    }

    /**
     * Attaches existing storage in new transaction.
     * <p>
     * If the storage is already attached, there is no action.
     * </p>
     *
     * @param storageId storage to attach
     */
    public void attachStorage(String storageId) {
        rrTransactionTemplate.executeWithoutResult(t -> {

            boolean primary = Objects.equals(storageId, storageProvider.getPrimaryStorage().getId());
            Storage storageInDb = storageStore.find(storageId);
            notNull(storageInDb, () -> new MissingObject(Storage.class, storageId));
            storageInDb.setDetachedByAdmin(null);
            storageInDb.setDetachedByError(null);
            storageStore.save(storageInDb);

            StorageSyncStatus status = storageSyncStatusStore.findSyncStatusOfStorage(storageId);
            if (status != null && !status.isFinished()) {
                status.clearExceptionInfo();
                storageSyncStatusStore.save(status);
            }

            registry.registerStorage(storageId, primary);
            storageQueueManager.startListener(storageId);
        });
    }

    public void deleteStorage(Storage entity) {
        try {
            storageQueueManager.deleteQueue(entity.getId());
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
        storageStore.delete(entity);
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


    private void logInitError(Storage storage) {
        log.error("error occurred during initial phase of synchronizing storage: " + storage + " the storage and its status entities will be deleted");
    }

    @Autowired
    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
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
    public void setRrTransactionTemplate(PlatformTransactionManager transactionManager) {
        this.rrTransactionTemplate = new TransactionTemplate(transactionManager);
        rrTransactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        rrTransactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        rrTransactionTemplate.afterPropertiesSet();
    }

    @Autowired
    public void setStorageQueueManager(JmsQueueManager storageQueueManager) {
        this.storageQueueManager = storageQueueManager;
    }

    @Autowired
    public void setArchivalObjectLightweightViewStore(ArchivalObjectLightweightViewStore archivalObjectLightweightViewStore) {
        this.archivalObjectLightweightViewStore = archivalObjectLightweightViewStore;
    }

    @Autowired
    public void setJmsSender(JmsSender jmsSender) {
        this.jmsSender = jmsSender;
    }

    @Autowired
    public void setRegistry(StoragesContextRegistry registry) {
        this.registry = registry;
    }

    @Autowired
    public void setStorageSyncStatusStore(StorageSyncStatusStore storageSyncStatusStore) {
        this.storageSyncStatusStore = storageSyncStatusStore;
    }

    @Autowired
    public void setCommonSyncService(CommonSyncService commonSyncService) {
        this.commonSyncService = commonSyncService;
    }

    @Autowired
    public void setArcstorageMailCenter(ArcstorageMailCenter arcstorageMailCenter) {
        this.arcstorageMailCenter = arcstorageMailCenter;
    }
}
