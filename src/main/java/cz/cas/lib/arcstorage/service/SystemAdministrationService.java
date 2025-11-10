package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.jms.JmsHealthCheckException;
import cz.cas.lib.arcstorage.jms.JmsQueueNotEmptyException;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateRequiredException;
import cz.cas.lib.arcstorage.service.exception.state.StateException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.storagesync.CommonSyncService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static cz.cas.lib.arcstorage.util.Utils.asList;
import static cz.cas.lib.arcstorage.util.Utils.notNull;

@Service
@Slf4j
public class SystemAdministrationService {

    private StorageProvider storageProvider;
    private StorageStore storageStore;
    private ArchivalDbService archivalDbService;
    private Path tmpFolder;
    private SystemStateService systemStateService;
    private CommonSyncService commonSyncService;
    private ArchivalService archivalService;

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
     */
    public List<ArchivalObject> cleanup(boolean cleanAlsoProcessing) throws SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException, IOException, JmsHealthCheckException {
        log.info("cleanup started, cleaning also processing=" + cleanAlsoProcessing);
        Instant now = Instant.now();
        List<ArchivalObject> objectsForCleanup = archivalDbService.findObjectsForCleanup(cleanAlsoProcessing);
        if (cleanAlsoProcessing)
            FileUtils.cleanDirectory(tmpFolder.toFile());
        if (objectsForCleanup.isEmpty()) {
            log.info("no objects for cleanup found");
        } else {
            archivalService.cleanUp(objectsForCleanup, now);
        }
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
     */
    public void cleanupOne(String objId) throws ReadOnlyStateException, JmsHealthCheckException, StateException {
        log.info("cleaning up object: " + objId);
        Instant now = Instant.now();
        ArchivalObject objectForCleanup = archivalDbService.getObject(objId);
        if (!ArchivalService.CLEANUP_ALLOWED_STATES.contains(objectForCleanup.getState())) {
            throw new StateException(objectForCleanup);
        }
        archivalService.cleanUp(asList(objectForCleanup), now);
    }

    public void recoverDb(String storageId, boolean override) throws StorageException {
        SystemState systemState = systemStateService.get();
        if (!systemState.isReadOnly())
            throw new ReadOnlyStateRequiredException();
        StorageService adapter = storageProvider.createAdapter(storageId);
        archivalDbService.recoverDbDataFromStorage(adapter, override);
    }

    public void switchPrimaryStorage(String id) throws SomeLogicalStoragesNotReachableException, InterruptedException, JmsQueueNotEmptyException {
        SystemState systemState = systemStateService.get();
        if (systemState.getPrimaryStorage() != null && Objects.equals(id, systemState.getPrimaryStorage().getId())) {
            return;
        }
        Storage targetStorage = storageStore.find(id);
        notNull(targetStorage, () -> new MissingObject(Storage.class, id));

        commonSyncService.createStorageServiceInReadonlyVacuum(systemState, targetStorage);
        systemState.setPrimaryStorage(targetStorage);
        systemState.setReadOnly(false);
        systemStateService.save(systemState);
    }

    @Autowired
    public void setTmpFolder(@Value("${spring.servlet.multipart.location}") String path) {
        this.tmpFolder = Paths.get(path);
    }

    @Autowired
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    @Autowired
    public void setArchivalDbService(ArchivalDbService archivalDbService) {
        this.archivalDbService = archivalDbService;
    }

    @Autowired
    public void setSystemStateService(SystemStateService systemStateService) {
        this.systemStateService = systemStateService;
    }

    @Autowired
    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }

    @Autowired
    public void setCommonSyncService(CommonSyncService commonSyncService) {
        this.commonSyncService = commonSyncService;
    }

    @Autowired
    public void setArchivalService(ArchivalService archivalService) {
        this.archivalService = archivalService;
    }
}
