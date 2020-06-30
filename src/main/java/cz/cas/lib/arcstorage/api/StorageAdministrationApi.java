package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.dto.StorageStateDto;
import cz.cas.lib.arcstorage.dto.StorageUpdateDto;
import cz.cas.lib.arcstorage.exception.BadRequestException;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import cz.cas.lib.arcstorage.exception.ForbiddenByConfigException;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.security.Roles;
import cz.cas.lib.arcstorage.service.StorageAdministrationService;
import cz.cas.lib.arcstorage.service.StorageProvider;
import cz.cas.lib.arcstorage.service.SystemStateService;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storagesync.StorageSyncStatus;
import cz.cas.lib.arcstorage.storagesync.StorageSyncStatusStore;
import cz.cas.lib.arcstorage.storagesync.exception.CantCreateDataspaceException;
import cz.cas.lib.arcstorage.storagesync.exception.StorageStillProcessObjectsException;
import cz.cas.lib.arcstorage.storagesync.exception.SynchronizationInProgressException;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.web.bind.annotation.*;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.validation.Valid;
import java.util.Collection;

import static cz.cas.lib.arcstorage.util.Utils.checkUUID;
import static cz.cas.lib.arcstorage.util.Utils.notNull;

@RestController
@RequestMapping("/api/administration/storage")
@RolesAllowed(Roles.ADMIN)
public class StorageAdministrationApi {

    private StorageStore storageStore;
    private SystemStateService systemStateService;
    private StorageAdministrationService storageAdministrationService;
    private StorageSyncStatusStore storageSyncStatusStore;
    private StorageProvider storageProvider;

    @ApiOperation(value = "Returns all attached logical storages.", response = Storage.class, responseContainer = "list")
    @RequestMapping(method = RequestMethod.GET)
    public Collection<Storage> getAll() {
        return storageStore.findAll();
    }

    @ApiOperation(value = "Returns logical storage with specified ID.", response = Storage.class)
    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public Storage getOne(
            @ApiParam(value = "id of the logical storage", required = true) @PathVariable("id") String id) {
        Storage storage = storageStore.find(id);
        notNull(storage, () -> new MissingObject(Storage.class, id));
        return storage;
    }

    @ApiOperation(value = "Returns state of logical storage.", notes = "Data returned depends on the logical storage. E.g. self-healing data are returned only if self-healing is supported by the storage.", response = StorageStateDto.class)
    @RequestMapping(value = "/{id}/state", method = RequestMethod.GET)
    public StorageStateDto getStorageState(
            @ApiParam(value = "id of the logical storage", required = true) @PathVariable("id") String id) throws BadRequestException {
        checkUUID(id);
        return storageAdministrationService.getStorageState(id);
    }

    @ApiOperation(value = "Check reachability of all storages", response = SystemState.class)
    @RequestMapping(value = "/check_reachability", method = RequestMethod.POST)
    public void checkReachability() {
        storageProvider.checkReachabilityOfAllStorages();
    }

    @ApiOperation(value = "Attaches new logical storage and starts synchronization.", response = Storage.class, notes = "After the initial checks are done, process continues asynchronously. See documentation for example JSON configurations")
    @RequestMapping(method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "invalid configuration"),
            @ApiResponse(code = 503, message = "storage to be synchronized is not reachable"),
            @ApiResponse(code = 566, message = "synchronization initialization timeout because there are still processing objects at the archival storage"),
            @ApiResponse(code = 567, message = "cant create some dataspace at the storage"),
            @ApiResponse(code = 409, message = "storage already exists")})
    public Storage attachStorage(
            @ApiParam(value = "logical storage entity", required = true) @RequestBody @Valid Storage storage)
            throws SomeLogicalStoragesNotReachableException, SynchronizationInProgressException, InterruptedException,
            IOStorageException, StorageStillProcessObjectsException, CantCreateDataspaceException, BadRequestException {
//        if (storage.getHost() != null && !storage.getHost().equals("localhost"))
//            checkIpv4(storage.getHost());
        if (storage.getId() != null && storageStore.find(storage.getId()) != null)
            throw new ConflictObject(Storage.class, storage.getId());
        return storageAdministrationService.attachStorage(storage);
    }

    @ApiOperation(value = "Continues with failed (stopped) synchronization.", notes = "After the initial checks are done, system asynchronously continues from the last successfully synced object/operation.")
    @RequestMapping(value = "/sync/{id}", method = RequestMethod.POST)
    @ApiResponses(value = {
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
            @ApiResponse(code = 404, message = "no sync status for this storage")
    })
    public StorageSyncStatus getSyncStatusOfStorage(
            @ApiParam(value = "id of the storage", required = true) @PathVariable("id") String id) {
        StorageSyncStatus syncStatusOfStorage = storageSyncStatusStore.findSyncStatusOfStorage(id);
        notNull(syncStatusOfStorage, () -> new MissingObject("storage sync status of configuration with specified id", id));
        return syncStatusOfStorage;
    }

    @ApiOperation(value = "Updates a logical storage.", notes = "Only name, priority and note flags are updatable, others are ignored.", response = Storage.class)
    @Transactional
    @RequestMapping(value = "/update", method = RequestMethod.POST)
    public Storage update(
            @ApiParam(value = "update DTO of the logical storage entity", required = true)
            @RequestBody @Valid StorageUpdateDto storageUpdateDto) {
        Storage storage = storageStore.find(storageUpdateDto.getId());
        notNull(storage, () -> new MissingObject(Storage.class, storageUpdateDto.getId()));
        storage.setName(storageUpdateDto.getName());
        storage.setPriority(storageUpdateDto.getPriority());
        storage.setNote(storageUpdateDto.getNote());
        storageStore.save(storage);
        return storage;
    }

    @ApiOperation(value = "Removes a logical storage.")
    @Transactional
    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "not authorized or can't be deleted because count of logical storage would be less than the configured minimum")
    })
    public void delete(
            @ApiParam(value = "id of the logical storage", required = true) @PathVariable("id") String id) throws ForbiddenByConfigException {
        int minStorageCount = systemStateService.get().getMinStorageCount();
        Storage storage = storageStore.find(id);
        notNull(storage, () -> new MissingObject(Storage.class, id));
        if (!(storageStore.getCount() > minStorageCount))
            throw new ForbiddenByConfigException("cant delete storage - actual number of storages (" + minStorageCount +
                    ") is the configured minimum");
        storageStore.delete(storage);
    }

    @Inject
    public void setSystemStateService(SystemStateService systemStateService) {
        this.systemStateService = systemStateService;
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

    @Inject
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }
}
