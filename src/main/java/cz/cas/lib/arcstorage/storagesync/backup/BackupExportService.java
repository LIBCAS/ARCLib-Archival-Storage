package cz.cas.lib.arcstorage.storagesync.backup;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.store.ArchivalObjectLightweightViewStore;
import cz.cas.lib.arcstorage.domain.views.ArchivalObjectLightweightView;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.StorageType;
import cz.cas.lib.arcstorage.exception.ForbiddenByConfigException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.fs.LocalFsProcessor;
import cz.cas.lib.arcstorage.storagesync.CommonSyncService;
import cz.cas.lib.arcstorage.storagesync.ObjectAudit;
import cz.cas.lib.arcstorage.storagesync.ObjectAuditStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
@Slf4j
public class BackupExportService {

    public static final String EXPORT_FINISHED_FILE_NAME = "BACKUP_EXPORT_FINISHED";

    private ArchivalObjectLightweightViewStore archivalObjectLightweightViewStore;
    private ObjectAuditStore objectAuditStore;
    private Path backupDir;
    private ExecutorService executor;
    private CommonSyncService commonSyncService;
    private boolean incrementalBackupAllowed;

    public void exportDataForBackup(@Nullable Instant since, @Nullable Instant to) throws BackupProcessException, ForbiddenByConfigException {
        boolean incrementalBackup = since != null;
        if (incrementalBackup && !incrementalBackupAllowed) {
            throw new ForbiddenByConfigException("incremental backup is not allowed, use since=NULL or reconfigure the app");
        }
        log.info("Backup export has started: export of new/modified objects in time range: [" + since + ", " + to + "] to backup directory at path: " + backupDir);
        Storage backupStorage = new Storage();
        backupStorage.setName("backup storage");
        backupStorage.setStorageType(StorageType.FS);
        backupStorage.setHost("localhost");
        LocalFsProcessor backupStorageService = new LocalFsProcessor(backupStorage, backupDir.toAbsolutePath().toString());
        boolean reachable = backupStorageService.testConnection();
        if (!reachable)
            throw new BackupProcessException("backup directory: " + backupDir.toAbsolutePath() + " not reachable for R/W");
        List<ArchivalObjectLightweightView> processedObjects = archivalObjectLightweightViewStore.findObjectsForNewStorage(since, to);
        Set<String> idsOfProcessedObjects = new HashSet<>();
        List<String> idsOfNewlyModifiedObjects = new ArrayList<>();

        CompletableFuture.runAsync(() -> {

            log.debug("First phase (copying new objects) has begun.");
            for (ArchivalObjectLightweightView obj : processedObjects) {
                ArchivalObjectDto objDto = obj.toDto();
                copyObject(objDto, backupStorageService);
                idsOfProcessedObjects.add(obj.getId());
            }
            log.debug("First phase completed. Copied " + idsOfProcessedObjects.size() + " new objects.");

            log.debug("Second phase (propagating operations) has begun.");
            int skipCount = 0;
            List<ObjectAudit> modifyOpsAudits = objectAuditStore.findAuditsForSync(since, to);
            Map<String, ArchivalObjectLightweightView> objectsInDb = archivalObjectLightweightViewStore.findAllInList(
                            modifyOpsAudits.stream().map(ObjectAudit::getIdInDatabase).collect(Collectors.toList()))
                    .stream().collect(Collectors.toMap(ArchivalObjectLightweightView::getId, v -> v));
            for (ObjectAudit objectAudit : modifyOpsAudits) {
                if (idsOfProcessedObjects.contains(objectAudit.getIdInStorage())) {
                    skipCount++;
                    continue;
                }
                try {
                    commonSyncService.propagateModification(objectAudit, objectsInDb.get(objectAudit.getIdInDatabase()), backupStorageService, incrementalBackup);
                } catch (Exception e) {
                    throw new BackupProcessException("sync of " + backupStorageService.getStorage() + " failed during propagating operation " + objectAudit, e);
                }
                idsOfNewlyModifiedObjects.add(objectAudit.getIdInStorage());
            }
            log.debug("Second phase completed. Propagated " + idsOfNewlyModifiedObjects.size() + " new operations, " +
                    skipCount + " were skipped because were already propagated during first phase.");

            processedObjects.addAll(archivalObjectLightweightViewStore.findAllInList(idsOfNewlyModifiedObjects));
            log.debug("Third phase (verification) has begun.");
            //processed objects does not contain forgotten objects
            verifyStateOfAllExported(backupStorageService, processedObjects);
            log.debug("Third phase completed. " + processedObjects.size() + " objects were successfully created/updated in backup directory.");
            log.info("Backup export has successfully ended: copied " + idsOfProcessedObjects.size() + " new objects, propagated " + idsOfNewlyModifiedObjects.size() + " modify operations");
            backupStorageService.createControlFile(EXPORT_FINISHED_FILE_NAME);
        }, executor).exceptionally(e -> {
            log.error("BACKUP FAILED", e);
            return null;
        });
    }

    private void copyObject(ArchivalObjectDto object, StorageService destinationStorage) throws BackupProcessException {
        try {
            commonSyncService.copyObject(object, destinationStorage);
        } catch (Exception e) {
            throw new BackupProcessException("sync of " + destinationStorage.getStorage() + " failed during copying " + object, e);
        }
    }

    private void verifyStateOfAllExported(StorageService backupStorageService, List<ArchivalObjectLightweightView> objectsToCheck) throws BackupProcessException {
        AtomicLong counter = new AtomicLong(0);
        List<ArchivalObjectDto> dtos = objectsToCheck.stream().map(ArchivalObjectLightweightView::toDto).collect(Collectors.toList());
        AtomicBoolean loopStopped = new AtomicBoolean(false);
        ArchivalObjectDto failedObject;
        new Thread(() -> {
            while (!loopStopped.get()) {
                log.trace("successfully checked " + counter.get() + " of " + objectsToCheck.size() + " objects");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    loopStopped.set(true);
                    e.printStackTrace();
                }
            }
        }).start();
        try {
            failedObject = backupStorageService.verifyStateOfObjects(dtos, counter);
        } catch (Exception e) {
            loopStopped.set(true);
            throw new BackupProcessException("sync of " + backupStorageService.getStorage() + " failed during post sync check", e);
        }
        if (failedObject != null) {
            loopStopped.set(true);
            throw new BackupProcessException("sync of " + backupStorageService.getStorage() + " failed during post sync check of object: " + failedObject);
        }
        loopStopped.set(true);
    }

    @Inject
    public void setArchivalObjectLightweightViewStore(ArchivalObjectLightweightViewStore archivalObjectLightweightViewStore) {
        this.archivalObjectLightweightViewStore = archivalObjectLightweightViewStore;
    }

    @Inject
    public void setBackupDir(@Value("${arcstorage.backupDirPath}") String path) {
        this.backupDir = Paths.get(path);
    }

    @Inject
    public void setObjectAuditStore(ObjectAuditStore objectAuditStore) {
        this.objectAuditStore = objectAuditStore;
    }

    @Inject
    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    @Inject
    public void setCommonSyncService(CommonSyncService commonSyncService) {
        this.commonSyncService = commonSyncService;
    }

    @Inject
    public void setIncrementalBackupAllowed(@Value("${arcstorage.optionalFeatures.incrementalBackup}") boolean incrementalBackupAllowed) {
        this.incrementalBackupAllowed = incrementalBackupAllowed;
    }
}
