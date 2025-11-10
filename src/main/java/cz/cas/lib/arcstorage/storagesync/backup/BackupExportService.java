package cz.cas.lib.arcstorage.storagesync.backup;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.store.ArchivalObjectLightweightViewStore;
import cz.cas.lib.arcstorage.domain.views.ArchivalObjectLightweightView;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.ObjectRetrievalResource;
import cz.cas.lib.arcstorage.dto.StorageType;
import cz.cas.lib.arcstorage.exception.ForbiddenByConfigException;
import cz.cas.lib.arcstorage.service.ArchivalService;
import cz.cas.lib.arcstorage.service.FileLocationResolver;
import cz.cas.lib.arcstorage.service.exception.state.FailedStateException;
import cz.cas.lib.arcstorage.service.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.service.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.ObjectCouldNotBeRetrievedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.storage.fs.LocalFsProcessor;
import cz.cas.lib.arcstorage.storagesync.ObjectAudit;
import cz.cas.lib.arcstorage.storagesync.ObjectAuditStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
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
    private static final int EXPORTED_COUNT_LOGGER_INTERVAL_MS = 5000;

    private ArchivalService archivalService;
    private FileLocationResolver fileLocationResolver;
    private ArchivalObjectLightweightViewStore archivalObjectLightweightViewStore;
    private ObjectAuditStore objectAuditStore;
    private Path backupDir;
    private ExecutorService executor;
    private boolean incrementalBackupAllowed;
    private boolean forgetFeatureAllowed;

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
        Instant objectStateObtainedAt = Instant.now();
        List<ArchivalObjectLightweightView> processedObjects = archivalObjectLightweightViewStore.findObjectsForNewStorage(since, to);
        Set<String> idsOfProcessedObjects = new HashSet<>();
        List<String> idsOfNewlyModifiedObjects = new ArrayList<>();

        CompletableFuture.runAsync(() -> {

            log.debug("First phase (copying new objects) has begun.");
            for (ArchivalObjectLightweightView obj : processedObjects) {
                ArchivalObjectDto objDto = obj.toDto();
                copyObject(objDto, backupStorageService, objectStateObtainedAt);
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
                    propagateModification(objectAudit, objectsInDb.get(objectAudit.getIdInDatabase()), backupStorageService);
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

    private void verifyStateOfAllExported(StorageService backupStorageService, List<ArchivalObjectLightweightView> objectsToCheck) throws BackupProcessException {
        AtomicLong counter = new AtomicLong(0);
        List<ArchivalObjectDto> dtos = objectsToCheck.stream().map(ArchivalObjectLightweightView::toDto).collect(Collectors.toList());
        AtomicBoolean loopStopped = new AtomicBoolean(false);
        ArchivalObjectDto failedObject;
        new Thread(() -> {
            while (!loopStopped.get()) {
                log.trace("successfully checked " + counter.get() + " of " + objectsToCheck.size() + " objects");
                try {
                    Thread.sleep(EXPORTED_COUNT_LOGGER_INTERVAL_MS);
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

    private void copyObjectData(ArchivalObjectDto object, StorageService targetStorage, @NonNull Instant operationTimestamp) throws StorageException, NoLogicalStorageAttachedException, SomeLogicalStoragesNotReachableException, ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, RollbackStateException, StillProcessingStateException, FailedStateException {
        String objectRetrievalResourceId = null;
        try (ObjectRetrievalResource objectRetrievalResource = archivalService.getObject(object);
             InputStream is = new BufferedInputStream(objectRetrievalResource.getInputStream())) {
            objectRetrievalResourceId = objectRetrievalResource.getId();
            object.setInputStream(is);
            targetStorage.storeObject(object, new AtomicBoolean(false), object.getDataSpace(), operationTimestamp);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (objectRetrievalResourceId != null) {
                fileLocationResolver.getFileTmpPath(objectRetrievalResourceId).toFile().delete();
            }
        }
    }

    private void copyObject(ArchivalObjectDto object, StorageService targetStorage, @NonNull Instant objectStateObtainedAt) throws BackupProcessException {
        try {
            switch (object.getState()) {
                case DELETED:
                    targetStorage.delete(object, object.getDataSpace(), objectStateObtainedAt);
                    break;
                case DELETION_FAILURE:
                case ROLLED_BACK:
                case ROLLBACK_FAILURE:
                case ARCHIVAL_FAILURE:
                    log.trace("copying metadata of object " + object);
                    targetStorage.storeObject(object, new AtomicBoolean(false), object.getDataSpace(), objectStateObtainedAt);
                    break;
                case ARCHIVED:
                    log.trace("copying {}", object);
                    copyObjectData(object, targetStorage, objectStateObtainedAt);
                    break;
                case REMOVED:
                    log.trace("copying {}", object);
                    copyObjectData(object, targetStorage, objectStateObtainedAt);
                    log.trace("removing {}", object);
                    targetStorage.remove(object, object.getDataSpace(), objectStateObtainedAt);
                    break;
                case PRE_PROCESSING:
                case PROCESSING:
                case FORGOT: //forgotten objects are not event present in DB, this should not occur
                default:
                    throw new IllegalArgumentException("can't copy object " + object.getStorageId() + " because it is in " + object.getState() + " state");
            }
        } catch (Exception e) {
            throw new BackupProcessException("sync of " + targetStorage.getStorage() + " failed during copying " + object, e);
        }
    }

    /**
     * @param objectAudit   operation to propagate
     * @param objectInDb    object in DB
     * @param targetStorage storage to which operations are propagated
     */
    private void propagateModification(ObjectAudit objectAudit, ArchivalObjectLightweightView objectInDb, StorageService targetStorage) throws StorageException, NoLogicalStorageAttachedException, ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, RollbackStateException, StillProcessingStateException, FailedStateException, ForbiddenByConfigException, SomeLogicalStoragesNotReachableException {
        log.trace("propagating " + objectAudit);
        switch (objectAudit.getOperation()) {
            case REMOVAL:
                targetStorage.remove(objectInDb.toDto(), objectAudit.getUser().getDataSpace(), objectAudit.getCreated());
                break;
            case RENEWAL:
                targetStorage.renew(objectInDb.toDto(), objectAudit.getUser().getDataSpace(), objectAudit.getCreated());
                break;
            case DELETION:
                targetStorage.delete(objectInDb.toDto(), objectAudit.getUser().getDataSpace(), objectAudit.getCreated());
                break;
            case ROLLBACK:
                targetStorage.rollbackObject(objectInDb.toDto(), objectAudit.getUser().getDataSpace(), objectAudit.getCreated());
                break;
            case ARCHIVED:
                //those are propagated other way, see COPYING_ARCHIVED_OBJECTS
                break;
            case ARCHIVAL_RETRY:
                if (!objectInDb.getState().isProcessing()) {
                    copyObject(objectInDb.toDto(), targetStorage, objectAudit.getCreated());
                }
                break;
            case FORGET:
                if (!forgetFeatureAllowed) {
                    throw new ForbiddenByConfigException("forget feature not allowed");
                }
                //objectInDb is always null as forgotten data are not present in DB
                targetStorage.forgetObject(objectAudit.getIdInStorage(), objectAudit.getUser().getDataSpace(), objectAudit.getCreated());
                break;
            default:
                throw new IllegalArgumentException("unknown operation: " + objectAudit.getOperation());
        }
    }

    @Autowired
    public void setArchivalObjectLightweightViewStore(ArchivalObjectLightweightViewStore archivalObjectLightweightViewStore) {
        this.archivalObjectLightweightViewStore = archivalObjectLightweightViewStore;
    }

    @Autowired
    public void setBackupDir(@Value("${arcstorage.backupDirPath}") String path) {
        this.backupDir = Paths.get(path);
    }

    @Autowired
    public void setObjectAuditStore(ObjectAuditStore objectAuditStore) {
        this.objectAuditStore = objectAuditStore;
    }

    @Autowired
    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    @Autowired
    public void setIncrementalBackupAllowed(@Value("${arcstorage.optionalFeatures.incrementalBackup}") boolean incrementalBackupAllowed) {
        this.incrementalBackupAllowed = incrementalBackupAllowed;
    }

    @Autowired
    public void setForgetFeatureAllowed(@Value("${arcstorage.optionalFeatures.forgetObject}") boolean forgetFeatureAllowed) {
        this.forgetFeatureAllowed = forgetFeatureAllowed;
    }

    @Autowired
    public void setArchivalService(ArchivalService archivalService) {
        this.archivalService = archivalService;
    }

    @Autowired
    public void setFileLocationResolver(FileLocationResolver fileLocationResolver) {
        this.fileLocationResolver = fileLocationResolver;
    }
}
