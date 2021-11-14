package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateRequiredException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.storagesync.newstorage.StorageSyncStatus;
import cz.cas.lib.arcstorage.storagesync.newstorage.StorageSyncStatusStore;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.SynchronizationInProgressException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static cz.cas.lib.arcstorage.util.Utils.asList;

@Service
@Slf4j
public class SystemAdministrationService {

    private StorageSyncStatusStore storageSyncStatusStore;
    private StorageProvider storageProvider;
    private ArchivalAsyncService async;
    private ArchivalDbService archivalDbService;
    private Path tmpFolder;
    private SystemStateService systemStateService;

    /**
     * Cleans up the storage.
     * <li>Rollbacks files which are in {@link ObjectState#ARCHIVAL_FAILURE} or {@link ObjectState#ROLLBACK_FAILURE} state.</li>
     * <li>if {@param cleanAlsoProcessing} is set to true, rollbacks also files which are in
     * {@link ObjectState#PROCESSING}/{@link ObjectState#PRE_PROCESSING} state and also cleans tmp folder.</li>
     * <li>Deletes files which are in {@link ObjectState#DELETION_FAILURE} state.</li>
     *
     * @param cleanAlsoProcessing whether objects with state {@link ObjectState#PROCESSING}/{@link ObjectState#PRE_PROCESSING}
     *                            should be rolled back and tmp folder should be cleaned..
     * @return list of objects for clean up
     * @throws SomeLogicalStoragesNotReachableException if any storage is unreachable before the process starts
     * @throws NoLogicalStorageAttachedException        if no logical storage is attached
     * @throws SynchronizationInProgressException       if some storage is synchronizing at the moment
     */
    public List<ArchivalObject> cleanup(boolean cleanAlsoProcessing) throws SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException, IOException, SynchronizationInProgressException {
        StorageSyncStatus storageSyncStatus = storageSyncStatusStore.anySynchronizing();
        if (storageSyncStatus != null)
            throw new SynchronizationInProgressException(storageSyncStatus);
        log.info("cleanup started, cleaning also processing=" + cleanAlsoProcessing);
        List<StorageService> storageServices = storageProvider.createAdaptersForWriteOperation(false);
        List<ArchivalObject> objectsForCleanup = archivalDbService.findObjectsForCleanup(cleanAlsoProcessing);
        if (cleanAlsoProcessing)
            FileUtils.cleanDirectory(tmpFolder.toFile());
        async.cleanUp(objectsForCleanup, storageServices);
        return objectsForCleanup;
    }

    /**
     * Cleans up the storage.
     * <li>Rollbacks files which are in {@link ObjectState#ARCHIVAL_FAILURE} or {@link ObjectState#ROLLBACK_FAILURE} state.</li>
     * <li>also rollbacks files which are in {@link ObjectState#PROCESSING}/{@link ObjectState#PRE_PROCESSING}</li>
     * <li>Deletes files which are in {@link ObjectState#DELETION_FAILURE} state.</li>
     *
     * @throws SomeLogicalStoragesNotReachableException if any storage is unreachable before the process starts
     * @throws NoLogicalStorageAttachedException        if no logical storage is attached
     * @throws SynchronizationInProgressException       if some storage is synchronizing at the moment
     */
    public void cleanupOne(String objId) throws SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException, IOException, SynchronizationInProgressException {
        StorageSyncStatus storageSyncStatus = storageSyncStatusStore.anySynchronizing();
        if (storageSyncStatus != null)
            throw new SynchronizationInProgressException(storageSyncStatus);
        log.info("cleaning up object: " + objId);
        List<StorageService> storageServices = storageProvider.createAdaptersForWriteOperation(false);
        ArchivalObject objectForCleanup = archivalDbService.getObject(objId);
        async.cleanUp(asList(objectForCleanup), storageServices);
    }

    public void recoverDb(String storageId, boolean override) throws StorageException {
        SystemState systemState = systemStateService.get();
        if (!systemState.isReadOnly())
            throw new ReadOnlyStateRequiredException();
        StorageService adapter = storageProvider.createAdapter(storageId);
        archivalDbService.recoverDbDataFromStorage(adapter, override);
    }


    @Inject
    public void setAsync(ArchivalAsyncService async) {
        this.async = async;
    }

    @Inject
    public void setTmpFolder(@Value("${arcstorage.tmpFolder}") String path) {
        this.tmpFolder = Paths.get(path);
    }

    @Inject
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    @Inject
    public void setStorageSyncStatusStore(StorageSyncStatusStore storageSyncStatusStore) {
        this.storageSyncStatusStore = storageSyncStatusStore;
    }

    @Inject
    public void setArchivalDbService(ArchivalDbService archivalDbService) {
        this.archivalDbService = archivalDbService;
    }

    @Inject
    public void setSystemStateService(SystemStateService systemStateService) {
        this.systemStateService = systemStateService;
    }
}
