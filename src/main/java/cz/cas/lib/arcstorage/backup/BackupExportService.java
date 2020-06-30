package cz.cas.lib.arcstorage.backup;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.store.ArchivalObjectStore;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.ObjectRetrievalResource;
import cz.cas.lib.arcstorage.dto.StorageType;
import cz.cas.lib.arcstorage.service.ArchivalService;
import cz.cas.lib.arcstorage.service.exception.state.FailedStateException;
import cz.cas.lib.arcstorage.service.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.service.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.ObjectCouldNotBeRetrievedException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.storage.fs.LocalFsProcessor;
import cz.cas.lib.arcstorage.storagesync.ObjectAudit;
import cz.cas.lib.arcstorage.storagesync.ObjectAuditStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
@Slf4j
public class BackupExportService {
    private ArchivalObjectStore archivalObjectStore;
    private ObjectAuditStore objectAuditStore;
    private ArchivalService archivalService;
    private Path backupDir;
    private Path tmpFolder;
    private ExecutorService executor;

    public void exportDataForBackup(Instant since, Instant to) throws BackupProcessException {
        log.info("Backup export has started: export of new/modified objects in time range: [" + since + ", " + to + "] to backup directory at path: " + backupDir);
        Storage backupStorage = new Storage();
        backupStorage.setName("backup storage");
        backupStorage.setStorageType(StorageType.FS);
        backupStorage.setHost("localhost");
        LocalFsProcessor backupStorageService = new LocalFsProcessor(backupStorage, backupDir.toAbsolutePath().toString());
        boolean reachable = backupStorageService.testConnection();
        if (!reachable)
            throw new BackupProcessException("backup directory: " + backupDir.toAbsolutePath().toString() + " not reachable for R/W");
        List<ArchivalObject> processedObjects = archivalObjectStore.findObjectsForNewStorage(since, to);
        Set<String> idsOfProcessedObjects = new HashSet<>();
        List<ObjectAudit> modifyOpsAudits = objectAuditStore.findAuditsForSync(since, to);
        List<String> idsOfNewlyModifiedObjects = new ArrayList<>();
        CompletableFuture.runAsync(() -> {
            log.debug("First phase (copying new objects) has begun.");
            for (ArchivalObject obj : processedObjects) {
                copyObject(obj.toDto(), backupStorageService);
                idsOfProcessedObjects.add(obj.getId());
            }
            log.debug("First phase completed. Copied " + idsOfProcessedObjects.size() + " new objects.");
            log.debug("Second phase (propagating operations) has begun.");
            int skipCount = 0;
            for (ObjectAudit objectAudit : modifyOpsAudits) {
                if (idsOfProcessedObjects.contains(objectAudit.getIdInStorage())) {
                    skipCount++;
                    continue;
                }
                propagateModifyOperation(objectAudit, backupStorageService);
                idsOfNewlyModifiedObjects.add(objectAudit.getIdInStorage());
            }
            log.debug("Second phase completed. Propagated " + idsOfNewlyModifiedObjects.size() + " new operations, " +
                    skipCount + " were skipped because were already propagated during first phase.");
            processedObjects.addAll(archivalObjectStore.findAllInList(idsOfNewlyModifiedObjects));
            log.debug("Third phase (verification) has begun.");
            verifyStateOfAllExported(backupStorageService, processedObjects);
            log.debug("Third phase completed. " + processedObjects.size() + " objects were successfully created/updated in backup directory.");
            log.info("Backup export has successfully ended: copied " + idsOfProcessedObjects.size() + " new objects, propagated " + idsOfNewlyModifiedObjects.size() + " modify operations");
        }, executor);
    }

    private void propagateModifyOperation(ObjectAudit objectAudit, StorageService backupStorageService) throws BackupProcessException {
        try {
            log.trace("propagating " + objectAudit);
            switch (objectAudit.getOperation()) {
                case REMOVAL:
                    backupStorageService.remove(objectAudit.getIdInStorage(), objectAudit.getUser().getDataSpace());
                    break;
                case RENEWAL:
                    backupStorageService.renew(objectAudit.getIdInStorage(), objectAudit.getUser().getDataSpace());
                    break;
                case DELETION:
                    backupStorageService.delete(objectAudit.getIdInStorage(), objectAudit.getUser().getDataSpace());
                    break;
                case ROLLBACK:
                    ArchivalObject archivalObject = archivalObjectStore.find(objectAudit.getIdInDatabase());
                    backupStorageService.rollbackObject(archivalObject.toDto(), objectAudit.getUser().getDataSpace());
                    break;
                case ARCHIVAL_RETRY:
                    archivalObject = archivalObjectStore.find(objectAudit.getIdInDatabase());
                    copyObject(archivalObject.toDto(), backupStorageService);
                    break;
            }
        } catch (Exception e) {
            throw new BackupProcessException("sync of " + backupStorageService.getStorage() + " failed during propagating operation " + objectAudit, e);
        }
    }

    private void copyObject(ArchivalObjectDto object, StorageService destinationStorage) throws BackupProcessException {
        try {
            log.trace("copying " + object);
            switch (object.getState()) {
                case PROCESSING:
                    throw new IllegalArgumentException("can't copy object " + object.getStorageId() + " because it is in " + object.getState() + " state");
                case DELETED:
                case DELETION_FAILURE:
                case ROLLED_BACK:
                case ROLLBACK_FAILURE:
                case ARCHIVAL_FAILURE:
                    destinationStorage.storeObject(object, new AtomicBoolean(false), object.getOwner().getDataSpace());
                    break;
                case ARCHIVED:
                case REMOVED:
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
        } catch (StorageException | StillProcessingStateException | ObjectCouldNotBeRetrievedException | FailedStateException | NoLogicalStorageAttachedException | NoLogicalStorageReachableException | RollbackStateException e) {
            throw new BackupProcessException("sync of " + destinationStorage.getStorage() + " failed during copying " + object, e);
        }
    }

    private void verifyStateOfAllExported(StorageService backupStorageService, List<ArchivalObject> objectsToCheck) throws BackupProcessException {
        AtomicLong counter = new AtomicLong(0);
        List<ArchivalObjectDto> dtos = objectsToCheck.stream().map(ArchivalObject::toDto).collect(Collectors.toList());
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
    public void setArchivalObjectStore(ArchivalObjectStore archivalObjectStore) {
        this.archivalObjectStore = archivalObjectStore;
    }

    @Inject
    public void setBackupDir(@Value("${arcstorage.backupDirPath}") String path) {
        this.backupDir = Paths.get(path);
    }

    @Inject
    public void setTmpFolder(@Value("${arcstorage.tmpFolder}") String path) {
        this.tmpFolder = Paths.get(path);
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
    public void setArchivalService(ArchivalService archivalService) {
        this.archivalService = archivalService;
    }
}
