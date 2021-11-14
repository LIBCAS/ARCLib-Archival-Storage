package cz.cas.lib.arcstorage.storagesync.newstorage;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.domain.store.ArchivalObjectLightweightViewStore;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.domain.views.ArchivalObjectLightweightView;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.service.ArchivalDbService;
import cz.cas.lib.arcstorage.service.SystemStateService;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storagesync.CommonSyncService;
import cz.cas.lib.arcstorage.storagesync.ObjectAudit;
import cz.cas.lib.arcstorage.storagesync.ObjectAuditStore;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.PostSyncCheckException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
@Slf4j
public class StorageSyncService {

    private ObjectAuditStore objectAuditStore;
    private SystemStateService systemStateService;
    private ArcstorageMailCenter arcstorageMailCenter;
    private ArchivalDbService archivalDbService;
    private StorageStore storageStore;
    private StorageSyncStatusStore syncStatusStore;
    private int transactionTimeoutSeconds;
    private ArchivalObjectLightweightViewStore archivalObjectLightweightViewStore;
    private CommonSyncService commonSyncService;

    /**
     * tries to transit from {@link StorageSyncPhase#INIT} through {@link StorageSyncPhase#COPYING_ARCHIVED_OBJECTS} to {@link StorageSyncPhase#PROPAGATING_OPERATIONS}
     * copies data and/or metadata of packages which are in particular states (ARCHIVED, DELETED ...)
     *
     * @param destinationStorage
     * @param status
     */
    @Async
    public void copyArchivedObjects(StorageService destinationStorage, StorageSyncStatus status) throws InterruptedException {
        List<ArchivalObjectLightweightView> objectsForNewStorage = archivalObjectLightweightViewStore.findObjectsForNewStorage(status.getStuckAt(), status.getCreated());
        List<ArchivalObjectDto> archivalObjectDtos = objectsForNewStorage.stream().map(ArchivalObjectLightweightView::toDto).collect(Collectors.toList());
        status.setTotalInThisPhase(archivalObjectDtos.size());
        status.setDoneInThisPhase(0);
        status.clearExeptionInfo();
        syncStatusStore.save(status);
        log.debug("Syncing: " + StorageSyncPhase.COPYING_ARCHIVED_OBJECTS + " sync phase of " + destinationStorage.getStorage() + ", " + status.getTotalInThisPhase() + " objects need to be synced");

        for (ArchivalObjectDto object : archivalObjectDtos) {
            boolean success = copyObjectAndUpdateStatus(object, status, destinationStorage);
            if (!success)
                return;
        }
        propagateOperationsOfModification(destinationStorage, status, status.getCreated());
    }

    /**
     * tries to transit from {@link StorageSyncPhase#COPYING_ARCHIVED_OBJECTS} through {@link StorageSyncPhase#PROPAGATING_OPERATIONS to {@link StorageSyncPhase#DONE}
     * <p>
     * If there are no more operations to be synced, the system is set to read only mode and waits <i>stateChangeTransactionTimeout</i>
     * seconds for already started transactions to finish. After that it is known that no new write will be allowed (because system is in read-only mode) and the old ones are synced (because the transaction is either commited or rolled back due to timeout). The <i>stateChangeTransactionTimeout</i> is configuration property for {@link org.springframework.transaction.support.TransactionTemplate} used in {@link ArchivalDbService}.
     * </p>
     *
     * @param storageService
     * @param status
     * @param startAt        this is set to {@link StorageSyncStatus#getCreated()} when called first time from {@link #copyArchivedObjects(StorageService, StorageSyncStatus)},
     *                       or set to timestamp if this is called recursively from {@link #propagateOperationsOfModification(StorageService, StorageSyncStatus, Instant)}
     */
    @Async
    public void propagateOperationsOfModification(StorageService destinationStorage, StorageSyncStatus status, Instant from) throws InterruptedException {
        String logPrefix = status.getPhase() == StorageSyncPhase.COPYING_ARCHIVED_OBJECTS ? "Starting " : "Continuing ";
        Instant nextTimeStartAt = Instant.now();
        List<ObjectAudit> operationsToBeSynced = objectAuditStore.findAuditsForSync(from, null);
        Map<String, ArchivalObjectLightweightView> objectsInDb = archivalObjectLightweightViewStore.findAllInList(
                        operationsToBeSynced.stream().map(ObjectAudit::getIdInDatabase).collect(Collectors.toList()))
                .stream().collect(Collectors.toMap(ArchivalObjectLightweightView::getId, v -> v));
        status.setPhase(StorageSyncPhase.PROPAGATING_OPERATIONS);
        status.setDoneInThisPhase(0);
        status.setTotalInThisPhase(operationsToBeSynced.size());
        status.clearExeptionInfo();
        syncStatusStore.save(status);
        log.debug(logPrefix + StorageSyncPhase.PROPAGATING_OPERATIONS + " sync phase of " + destinationStorage.getStorage() + " " + status.getTotalInThisPhase() + " operations need to be synced");
        if (!operationsToBeSynced.isEmpty()) {
            for (ObjectAudit objectAudit : operationsToBeSynced) {
                try {
                    commonSyncService.propagateModification(objectAudit, objectsInDb.get(objectAudit.getIdInDatabase()), destinationStorage, false);
                    status.clearExeptionInfo();
                    status.setDoneInThisPhase(status.getDoneInThisPhase() + 1);
                    syncStatusStore.save(status);
                } catch (Exception e) {
                    status.setExceptionInfo(e, objectAudit.getCreated());
                    syncStatusStore.save(status);
                    log.error("sync of " + destinationStorage.getStorage() + " failed during propagating operation " + objectAudit + " status: " + status);
                    arcstorageMailCenter.sendStorageSynchronizationError(status);
                    return;
                }
            }
            propagateOperationsOfModification(destinationStorage, status, nextTimeStartAt);
        } else {
            log.debug("no new operations registered, running towards " + StorageSyncPhase.POST_SYNC_CHECK + " of " + destinationStorage.getStorage());
            SystemState systemState = systemStateService.get();
            systemState.setReadOnly(true);
            log.info("setting system to read-only state");
            systemStateService.save(systemState);
            Thread.sleep(transactionTimeoutSeconds * 1000L);
            operationsToBeSynced = objectAuditStore.findAuditsForSync(nextTimeStartAt, null);
            //no more operations came in last seconds and no are coming now (because of readonly mode)
            //this condition will be false very rarely
            if (operationsToBeSynced.isEmpty()) {
                postSyncCheck(destinationStorage, status);
                return;
            }
            //some operations came since last check, repeat the sync phase from the last check time
            log.debug("some last operations registered during finishing of sync of " + destinationStorage.getStorage() + " , continuing with sync");
            propagateOperationsOfModification(destinationStorage, status, nextTimeStartAt);
        }
    }

    @Async
    public void postSyncCheck(StorageService destinationStorage, StorageSyncStatus status) {
        String logPrefix = status.getPhase() == StorageSyncPhase.PROPAGATING_OPERATIONS ? "Starting " : "Continuing ";
        List<ArchivalObjectDto> objectsToBeChecked = archivalObjectLightweightViewStore.findAllCreatedWithinTimeRange(status.getStuckAt(), null).stream().map(ArchivalObjectLightweightView::toDto).collect(Collectors.toList());
        log.debug(logPrefix + StorageSyncPhase.POST_SYNC_CHECK + " sync phase of " + destinationStorage.getStorage() + " " + status.getTotalInThisPhase() + " objects need to be checked");
        status.setPhase(StorageSyncPhase.POST_SYNC_CHECK);
        status.clearExeptionInfo();
        status.setTotalInThisPhase(objectsToBeChecked.size());
        status.setDoneInThisPhase(0);
        status.clearExeptionInfo();
        syncStatusStore.save(status);
        ArchivalObjectDto failedObject;
        AtomicLong counter = new AtomicLong(0);

        AtomicBoolean loopStopped = new AtomicBoolean(false);
        new Thread(() -> {
            while (!loopStopped.get()) {
                log.debug("successfully checked " + counter.get() + " of " + objectsToBeChecked.size() + " objects");
                status.setDoneInThisPhase(counter.get());
                syncStatusStore.save(status);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    loopStopped.set(true);
                    e.printStackTrace();
                }
            }
        }).start();

        try {
            failedObject = destinationStorage.verifyStateOfObjects(objectsToBeChecked, counter);
        } catch (Exception e) {
            loopStopped.set(true);
            status.setExceptionInfo(e, null);
            syncStatusStore.save(status);
            log.error("sync of " + destinationStorage.getStorage() + " failed during post sync check, status: " + status);
            arcstorageMailCenter.sendStorageSynchronizationError(status);
            return;
        }
        if (failedObject != null) {
            loopStopped.set(true);
            PostSyncCheckException postSyncCheckException = new PostSyncCheckException(failedObject);
            status.setExceptionInfo(postSyncCheckException, failedObject.getCreated());
            syncStatusStore.save(status);
            log.error(postSyncCheckException.toString());
            arcstorageMailCenter.sendStorageSynchronizationError(status);
            return;
        }
        loopStopped.set(true);
        status.setPhase(StorageSyncPhase.DONE);
        long objectsTotalCount = archivalDbService.getObjectsTotalCount();
        status.setDoneInThisPhase(objectsTotalCount);
        status.setTotalInThisPhase(objectsTotalCount);
        syncStatusStore.save(status);
        Storage storage = destinationStorage.getStorage();
        storage.setSynchronizing(false);
        storageStore.save(storage);
        log.debug("successfully checked " + counter.get() + " of " + objectsToBeChecked.size() + " objects");
        log.info("sync of " + destinationStorage.getStorage() + " finished");
        SystemState systemState = systemStateService.get();
        setReadWriteConfig(systemState);
    }


    private boolean copyObjectAndUpdateStatus(ArchivalObjectDto object, StorageSyncStatus status, StorageService destinationStorage) {
        try {
            commonSyncService.copyObject(object, destinationStorage);
            status.clearExeptionInfo();
            status.setDoneInThisPhase(status.getDoneInThisPhase() + 1);
            syncStatusStore.save(status);
            return true;
        } catch (Exception e) {
            status.setExceptionInfo(e, object.getCreated());
            syncStatusStore.save(status);
            log.error("sync of " + destinationStorage.getStorage() + " failed during copying " + object + " status: " + status);
            arcstorageMailCenter.sendStorageSynchronizationError(status);
            return false;
        }
    }

    private void setReadWriteConfig(SystemState systemState) {
        if (!syncStatusStore.anyInInitialOrFinishingPhase()) {
            systemState.setReadOnly(false);
            systemStateService.save(systemState);
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
    public void setArchivalDbService(ArchivalDbService archivalDbService) {
        this.archivalDbService = archivalDbService;
    }

    @Inject
    public void setSystemStateService(SystemStateService systemStateService) {
        this.systemStateService = systemStateService;
    }

    @Inject
    public void setTransactionTimeoutSeconds(@Value("${arcstorage.stateChangeTransactionTimeout}") int transactionTimeoutSeconds) {
        this.transactionTimeoutSeconds = transactionTimeoutSeconds;
    }

    @Inject
    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }

    @Inject
    public void setArchivalObjectLightweightViewStore(ArchivalObjectLightweightViewStore archivalObjectLightweightViewStore) {
        this.archivalObjectLightweightViewStore = archivalObjectLightweightViewStore;
    }

    @Inject
    public void setArcstorageMailCenter(ArcstorageMailCenter arcstorageMailCenter) {
        this.arcstorageMailCenter = arcstorageMailCenter;
    }

    @Inject
    public void setCommonSyncService(CommonSyncService commonSyncService) {
        this.commonSyncService = commonSyncService;
    }
}
