package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.store.ConfigurationStore;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.dto.StorageUpdateDto;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import cz.cas.lib.arcstorage.exception.ForbiddenByConfigException;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.security.Roles;
import cz.cas.lib.arcstorage.service.StorageAdministrationService;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storagesync.StorageStillProcessObjectsException;
import cz.cas.lib.arcstorage.storagesync.StorageSyncStatus;
import cz.cas.lib.arcstorage.storagesync.StorageSyncStatusStore;
import cz.cas.lib.arcstorage.storagesync.SynchronizationInProgressException;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.web.bind.annotation.*;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.validation.Valid;
import java.util.Collection;

import static cz.cas.lib.arcstorage.util.Utils.notNull;

@RestController
@RequestMapping("/api/administration/storage")
@RolesAllowed(Roles.ADMIN)
public class StorageAdministrationApi {

    private StorageStore storageStore;
    private ConfigurationStore configurationStore;
    private StorageAdministrationService storageAdministrationService;
    private StorageSyncStatusStore storageSyncStatusStore;

    @ApiOperation(value = "Returns all attached logical storages.", response = Storage.class, responseContainer = "list")
    @Transactional
    @RequestMapping(method = RequestMethod.GET)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "successful response")
    })
    public Collection<Storage> getAll() {
        return storageStore.findAll();
    }

    @ApiOperation(value = "Returns logical storage with specified ID.", response = Storage.class)
    @Transactional
    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "successful response"),
            @ApiResponse(code = 404, message = "storage with the id is missing")
    })
    public Storage getOne(
            @ApiParam(value = "id of the logical storage", required = true) @PathVariable("id") String id) {

        Storage storage = storageStore.find(id);
        notNull(storage, () -> new MissingObject(Storage.class, id));
        return storage;
    }

    @ApiOperation(value = "Attaches new logical storage and starts synchronization.", response = Storage.class, notes = "" +
            "* Local FS/ZFS or FS/ZFS over NFS storage configuration:" +
            "* {\"name\":\"local storage\",\"host\":\"localhost\",\"port\":0,\"priority\":10,\"storageType\":\"FS\",\"note\":null,\"config\":\"{\\\"rootDirPath\\\":\\\"d:\\\\\\\\\\\\\\\\testfolder\\\"}\"}" +
            "* Ceph storage configuration:" +
            "* {\"name\":\"ceph\",\"host\":\"192.168.10.61\",\"port\":7480,\"priority\":1,\"storageType\":\"CEPH\",\"note\":null,\"config\":\"{\\\"adapterType\\\":\\\"S3\\\", \\\"userKey\\\":\\\"SKGKKYQ50UU04XS4TA4O\\\",\\\"userSecret\\\":\\\"TrLjA3jdlzKcvyN1vWnGqiLGDwCB90bNF71rwA5D\\\"}\"}" +
            "* Remote FS/ZFS over SFTP configuration:" +
            "* {\"name\":\"sftp storage\",\"host\":\"192.168.10.60\",\"port\":22,\"priority\":1,\"storageType\":\"ZFS\",\"note\":null,\"config\":\"{\\\"rootDirPath\\\":\\\"/arcpool/test\\\"}\"}" +
            "* In order to produce the right JSON, Windows paths separators has to be escaped (see rootDirPath)" +
            "* The reachable attribute is managed by the application itself.")
    @RequestMapping(method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "successful response"),
            @ApiResponse(code = 503, message = "storage to be synchronized is not reachable"),
            @ApiResponse(code = 566, message = "synchronization initialization timeout because there are still processing objects at the archival storage"),
            @ApiResponse(code = 409, message = "storage already exists")})
    public Storage attachStorage(
            @ApiParam(value = "logical storage entity", required = true) @RequestBody @Valid Storage storage)
            throws SomeLogicalStoragesNotReachableException, SynchronizationInProgressException, InterruptedException,
            IOStorageException, StorageStillProcessObjectsException {
        if (storage.getId() != null && storageStore.find(storage.getId()) != null)
            throw new ConflictObject(Storage.class, storage.getId());
        return storageAdministrationService.attachStorage(storage);
    }

    @ApiOperation(value = "Continues with failed (stopped) synchronization.")
    @RequestMapping(value = "/sync/{id}", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "successful response"),
            @ApiResponse(code = 403, message = "synchronization is already in progress or not authorized"),
            @ApiResponse(code = 503, message = "storage to be synchronized is not reachable")
    })
    public void continueSync(
            @ApiParam(value = "id of the synchronization status entity", required = true) @PathVariable("id") String id)
            throws SomeLogicalStoragesNotReachableException, SynchronizationInProgressException, InterruptedException {
        StorageSyncStatus storageSyncStatus = storageSyncStatusStore.find(id);
        notNull(storageSyncStatus, () -> new MissingObject(StorageSyncStatus.class, id));
        storageAdministrationService.synchronizeStorage(storageSyncStatus, false);
    }

    @ApiOperation(value = "Retrieves sync status entity for the storage.", response = StorageSyncStatus.class)
    @RequestMapping(value = "/sync/{id}", method = RequestMethod.GET)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "successful response"),
            @ApiResponse(code = 404, message = "no sync status for this storage")
    })
    public StorageSyncStatus getSyncStatusOfStorage(
            @ApiParam(value = "id of the storage", required = true) @PathVariable("id") String id) {
        StorageSyncStatus syncStatusOfStorage = storageSyncStatusStore.findSyncStatusOfStorage(id);
        notNull(syncStatusOfStorage, () -> new MissingObject("storage sync status of configuration with specified id", id));
        return syncStatusOfStorage;
    }

    @ApiOperation(value = "Updates a logical storage.", response = Storage.class)
    @Transactional
    @RequestMapping(value = "/update", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "successful response"),
            @ApiResponse(code = 404, message = "storage with the id is missing")
    })
    public Storage update(
            @ApiParam(value = "update DTO of the logical storage entity", required = true)
            @RequestBody @Valid StorageUpdateDto storageUpdateDto) {
        Storage storage = storageStore.find(storageUpdateDto.getId());
        notNull(storage, () -> new MissingObject(Storage.class, storageUpdateDto.getId()));
        storage.setName(storageUpdateDto.getName());
        storage.setPriority(storageUpdateDto.getPriority());
        storage.setNote(storageUpdateDto.getNote());
        storage.setWriteOnly(storageUpdateDto.isWriteOnly());
        storageStore.save(storage);
        return storage;
    }

    @ApiOperation(value = "Removes a logical storage.")
    @Transactional
    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "successful response"),
            @ApiResponse(code = 404, message = "storage with the id is missing")
    })
    public void delete(
            @ApiParam(value = "id of the logical storage", required = true) @PathVariable("id") String id) throws ForbiddenByConfigException {
        int minStorageCount = configurationStore.get().getMinStorageCount();
        Storage storage = storageStore.find(id);
        notNull(storage, () -> new MissingObject(Storage.class, id));
        if (!(storageStore.getCount() > minStorageCount))
            throw new ForbiddenByConfigException("cant delete storage - actual number of storages (" + minStorageCount +
                    ") is the configured minimum");
        storageStore.delete(storage);
    }

    @Inject
    public void setConfigurationStore(ConfigurationStore configurationStore) {
        this.configurationStore = configurationStore;
    }

    @Inject
    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }

    @Inject
    public void setStorageAdministrationService(StorageAdministrationService storageAdministrationService) {
        this.storageAdministrationService = storageAdministrationService;
    }

    @Inject
    public void setStorageSyncStatusStore(StorageSyncStatusStore storageSyncStatusStore) {
        this.storageSyncStatusStore = storageSyncStatusStore;
    }
}
