package cz.cas.lib.arcstorage.storagesync;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.Configuration;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.store.ArchivalObjectStore;
import cz.cas.lib.arcstorage.domain.store.ConfigurationStore;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.ObjectRetrievalResource;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.service.ArchivalDbService;
import cz.cas.lib.arcstorage.service.ArchivalService;
import cz.cas.lib.arcstorage.service.exception.state.FailedStateException;
import cz.cas.lib.arcstorage.service.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.service.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.ObjectCouldNotBeRetrievedException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Service
@Slf4j
public class StorageSyncService {

    private ObjectAuditStore objectAuditStore;
    private ArchivalService archivalService;
    private ArchivalObjectStore archivalObjectStore;
    private ConfigurationStore configurationStore;
    private ArcstorageMailCenter arcstorageMailCenter;
    private ArchivalDbService archivalDbService;
    private StorageStore storageStore;
    private StorageSyncStatusStore syncStatusStore;
    private int transactionTimeoutSeconds;
    private Path tmpFolder;

    /**
     * tries to transit from {@link StorageSyncPhase#INIT} through {@link StorageSyncPhase#COPYING_ARCHIVED_OBJECTS} to {@link StorageSyncPhase#PROPAGATING_OPERATIONS}
     *
     * @param destinationStorage
     * @param status
     */
    @Async
    public void copyStoragePhase1(StorageService destinationStorage, StorageSyncStatus status) throws InterruptedException {
        List<ArchivalObject> objectsForNewStorage = archivalObjectStore.findObjectsForNewStorage(status.getStuckAt(), status.getCreated());
        List<ArchivalObjectDto> archivalObjectDtos = objectsForNewStorage.stream().map(ArchivalObject::toDto).collect(Collectors.toList());
        status.setTotalInThisPhase(archivalObjectDtos.size());
        status.setDoneInThisPhase(0);
        syncStatusStore.save(status);
        String logPrefix = status.getStuckAt() == null ? "Starting " : "Continuing ";
        log.debug(logPrefix + StorageSyncPhase.COPYING_ARCHIVED_OBJECTS + " sync phase of " + destinationStorage.getStorage() + " " + status.getTotalInThisPhase() + " objects need to be synced");

        for (ArchivalObjectDto object : archivalObjectDtos) {
            boolean success = copyObject(object, status, destinationStorage);
            if (!success)
                return;
        }
        copyStoragePhase2(destinationStorage, status);
    }

    /**
     * tries to transit from {@link StorageSyncPhase#COPYING_ARCHIVED_OBJECTS} through {@link StorageSyncPhase#PROPAGATING_OPERATIONS to {@link StorageSyncPhase#DONE}
     * <p>
     * If there are no more operations to be synced, the system is converted to the read only mode and waits <i>state-change-transaction-timeout</i>
     * seconds for already started transactions to finish. After that it is known that no new write will be allowed (because system is in read-only mode) and the old ones are synced (because the transaction is either commited or rolled back due to timeout). The <i>state-change-transaction-timeout</i> is configuration property for {@link org.springframework.transaction.support.TransactionTemplate} used in {@link ArchivalDbService}.
     * </p>
     *
     * @param storageService
     * @param status
     */
    @Async
    public void copyStoragePhase2(StorageService destinationStorage, StorageSyncStatus status) throws InterruptedException {
        Instant nextTimeStartAt = Instant.now();
        Instant from = status.getPhase() == StorageSyncPhase.COPYING_ARCHIVED_OBJECTS ? status.getCreated() : status.getStuckAt();
        List<ObjectAudit> operationsToBeSynced = objectAuditStore.findOperationsToBeSyncedInPhase2(from);
        status.setPhase(StorageSyncPhase.PROPAGATING_OPERATIONS);
        status.setDoneInThisPhase(0);
        status.setTotalInThisPhase(operationsToBeSynced.size());
        String logPrefix = status.getStuckAt() == null ? "Starting " : "Continuing ";
        status.setStuckAt(null);
        syncStatusStore.save(status);
        log.debug(logPrefix + StorageSyncPhase.PROPAGATING_OPERATIONS + " sync phase of " + destinationStorage.getStorage() + " " + status.getTotalInThisPhase() + " operations need to be synced");
        if (operationsToBeSynced.isEmpty()) {
            log.debug("no new operations registered, trying to finish synchronization of " + destinationStorage.getStorage());
            status.setPhase(StorageSyncPhase.FINISHING);
            syncStatusStore.save(status);
            Configuration configuration = configurationStore.get();
            configuration.setReadOnly(true);
            configurationStore.save(configuration);
            log.info("setting system to read-only state");
            Thread.sleep(transactionTimeoutSeconds * 1000);
            operationsToBeSynced = objectAuditStore.findOperationsToBeSyncedInPhase2(nextTimeStartAt);
            //no more operations came in last seconds and no are coming now (because of readonly mode)
            if (operationsToBeSynced.isEmpty()) {
                status.setPhase(StorageSyncPhase.DONE);
                status.clearExeptionInfo();
                long objectsTotalCount = archivalDbService.getObjectsTotalCount();
                status.setDoneInThisPhase(objectsTotalCount);
                status.setTotalInThisPhase(objectsTotalCount);
                syncStatusStore.save(status);
                Storage storage = destinationStorage.getStorage();
                storage.setWriteOnly(false);
                storageStore.save(storage);
                log.info("sync of " + destinationStorage.getStorage() + " finished");
                setReadWriteConfig(configuration);
                return;
            }
            //some operations came since last check, repeat the sync phase from the last check time
            log.debug("some new operations registered during finishing of sync of " + destinationStorage.getStorage() + " , continuing with sync");
            status.setStuckAt(nextTimeStartAt);
            copyStoragePhase2(destinationStorage, status);
            //some operations came since last check, repeat the sync phase from the last check time
        } else {
            status.setStuckAt(nextTimeStartAt);
            for (ObjectAudit objectAudit : operationsToBeSynced) {
                log.debug("propagating operation " + objectAudit);
                try {
                    switch (objectAudit.getOperation()) {
                        case REMOVAL:
                            destinationStorage.remove(objectAudit.getObjectId(), objectAudit.getUser().getDataSpace());
                            break;
                        case RENEWAL:
                            destinationStorage.renew(objectAudit.getObjectId(), objectAudit.getUser().getDataSpace());
                            break;
                        case DELETION:
                            destinationStorage.delete(objectAudit.getObjectId(), objectAudit.getUser().getDataSpace());
                            break;
                    }
                    status.clearExeptionInfo();
                    status.setDoneInThisPhase(status.getDoneInThisPhase() + 1);
                    syncStatusStore.save(status);
                } catch (Exception e) {
                    status.setExceptionInfo(e.getClass(), e.toString(), objectAudit.getCreated());
                    syncStatusStore.save(status);
                    log.error("sync of " + destinationStorage.getStorage() + " failed during propagating operation " + objectAudit + " status: " + status);
                    arcstorageMailCenter.sendStorageSynchronizationError(status);
                    return;
                }
            }
            status.setStuckAt(operationsToBeSynced.get(operationsToBeSynced.size() - 1).getCreated().plusMillis(1));
            copyStoragePhase2(destinationStorage, status);
        }
    }


    private boolean copyObject(ArchivalObjectDto object, StorageSyncStatus status, StorageService destinationStorage) {
        try {
            switch (object.getState()) {
                case ARCHIVAL_FAILURE:
                case PROCESSING:
                    throw new IllegalArgumentException("can't copy object " + object.getStorageId() + " because it is in " + object.getState() + " state");
                case DELETED:
                case DELETION_FAILURE:
                case ROLLED_BACK:
                    log.debug("copying " + object);
                    destinationStorage.storeObject(object, new AtomicBoolean(false), object.getOwner().getDataSpace());
                    break;
                case ARCHIVED:
                case REMOVED:
                    log.debug("copying " + object);
                    ObjectRetrievalResource objectRetrievalResource = archivalService.getObject(object);
                    try (InputStream is = new BufferedInputStream(objectRetrievalResource.getInputStream())) {
                        object.setInputStream(is);
                        destinationStorage.storeObject(object, new AtomicBoolean(false), object.getOwner().getDataSpace());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } finally {
                        objectRetrievalResource.close();
                        tmpFolder.resolve(objectRetrievalResource.getId()).toFile().delete();
                    }
                    break;
            }
            status.clearExeptionInfo();
            status.setDoneInThisPhase(status.getDoneInThisPhase() + 1);
            syncStatusStore.save(status);
            return true;
        } catch (StorageException | StillProcessingStateException | ObjectCouldNotBeRetrievedException | FailedStateException | NoLogicalStorageAttachedException | NoLogicalStorageReachableException | RollbackStateException e) {
            status.setExceptionInfo(e.getClass(), e.toString(), object.getCreated());
            syncStatusStore.save(status);
            log.error("sync of " + destinationStorage.getStorage() + " failed during copying " + object + " status: " + status);
            arcstorageMailCenter.sendStorageSynchronizationError(status);
            return false;
        }
    }

    private void setReadWriteConfig(Configuration configuration) {
        if (!syncStatusStore.anyInInitialOrFinishingPhase()) {
            configuration.setReadOnly(false);
            configurationStore.save(configuration);
            log.info("system set to read-write mode");
        }
    }

    @Inject
    public void setSyncStatusStore(StorageSyncStatusStore syncStatusStore) {
        this.syncStatusStore = syncStatusStore;
    }

    @Inject
    public void setObjectAuditStore(ObjectAuditStore objectAuditStore) {
        this.objectAuditStore = objectAuditStore;
    }

    @Inject
    public void setArchivalService(ArchivalService archivalService) {
        this.archivalService = archivalService;
    }

    @Inject
    public void setArchivalDbService(ArchivalDbService archivalDbService) {
        this.archivalDbService = archivalDbService;
    }

    @Inject
    public void setConfigurationStore(ConfigurationStore configurationStore) {
        this.configurationStore = configurationStore;
    }

    @Inject
    public void setTransactionTimeoutSeconds(@Value("${arcstorage.state-change-transaction-timeout}") int transactionTimeoutSeconds) {
        this.transactionTimeoutSeconds = transactionTimeoutSeconds;
    }

    @Inject
    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }

    @Inject
    public void setArchivalObjectStore(ArchivalObjectStore archivalObjectStore) {
        this.archivalObjectStore = archivalObjectStore;
    }

    @Inject
    public void setArcstorageMailCenter(ArcstorageMailCenter arcstorageMailCenter) {
        this.arcstorageMailCenter = arcstorageMailCenter;
    }

    @Inject
    public void setTmpFolder(@Value("${arcstorage.tmp-folder}") String path) {
        this.tmpFolder = Paths.get(path);
    }
}
