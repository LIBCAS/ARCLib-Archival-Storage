package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.dto.StorageBasicDto;
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
import cz.cas.lib.arcstorage.storagesync.newstorage.StorageSyncStatus;
import cz.cas.lib.arcstorage.storagesync.newstorage.StorageSyncStatusStore;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.CantCreateDataspaceException;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.StorageStillProcessObjectsException;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.SynchronizationInProgressException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import jakarta.annotation.security.RolesAllowed;
import jakarta.validation.Valid;
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

    @Operation(summary = "Returns all attached logical storages.")
    @RequestMapping(method = RequestMethod.GET)
    public Collection<Storage> getAll() {
        return storageStore.findAll();
    }

    @Operation(summary = "Returns simple DTOs of all attached logical storages.")
    @RequestMapping(value = "/basic", method = RequestMethod.GET)
    public Collection<StorageBasicDto> getAllAsDtos() {
        return storageStore.getAllAsDtos();
    }

    @Operation(summary = "Returns logical storage with specified ID.")
    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public Storage getOne(
            @Parameter(description = "id of the logical storage", required = true) @PathVariable("id") String id) {
        Storage storage = storageStore.find(id);
        notNull(storage, () -> new MissingObject(Storage.class, id));
        return storage;
    }

    @Operation(summary = "Returns state of logical storage.", description = "Data returned depends on the logical storage. E.g. self-healing data are returned only if self-healing is supported by the storage.")
    @RequestMapping(value = "/{id}/state", method = RequestMethod.GET)
    public StorageStateDto getStorageState(
            @Parameter(description = "id of the logical storage", required = true) @PathVariable("id") String id) throws BadRequestException {
        checkUUID(id);
        return storageAdministrationService.getStorageState(id);
    }

    @Operation(summary = "Check reachability of all storages")
    @RequestMapping(value = "/check_reachability", method = RequestMethod.POST)
    public void checkReachability() {
        storageProvider.checkReachabilityOfAllStorages();
    }

    @Operation(summary = "Attaches new logical storage and starts synchronization.", description = "After the initial checks are done, process continues asynchronously. See documentation for example JSON configurations")
    @RequestMapping(method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "400", description = "invalid configuration"),
            @ApiResponse(responseCode = "503", description = "storage to be synchronized is not reachable"),
            @ApiResponse(responseCode = "566", description = "synchronization initialization timeout because there are still processing objects at the archival storage"),
            @ApiResponse(responseCode = "567", description = "cant create some dataspace at the storage"),
            @ApiResponse(responseCode = "409", description = "storage already exists")})
    public Storage attachStorage(
            @Parameter(description = "logical storage entity", required = true) @RequestBody @Valid Storage storage)
            throws SomeLogicalStoragesNotReachableException, SynchronizationInProgressException, InterruptedException,
            IOStorageException, StorageStillProcessObjectsException, CantCreateDataspaceException, BadRequestException {
//        if (storage.getHost() != null && !storage.getHost().equals("localhost"))
//            checkIpv4(storage.getHost());
        if (storage.getId() != null && storageStore.find(storage.getId()) != null)
            throw new ConflictObject(Storage.class, storage.getId());
        return storageAdministrationService.attachStorage(storage);
    }

    @Operation(summary = "Continues with failed (stopped) synchronization.", description = "After the initial checks are done, system asynchronously continues from the last successfully synced object/operation.")
    @RequestMapping(value = "/sync/{id}", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "synchronization is already in progress or not authorized"),
            @ApiResponse(responseCode = "503", description = "storage to be synchronized is not reachable")
    })
    public void continueSync(
            @Parameter(description = "id of the synchronization status entity", required = true) @PathVariable("id") String id)
            throws SomeLogicalStoragesNotReachableException, SynchronizationInProgressException, InterruptedException {
        StorageSyncStatus storageSyncStatus = storageSyncStatusStore.find(id);
        notNull(storageSyncStatus, () -> new MissingObject(StorageSyncStatus.class, id));
        storageAdministrationService.synchronizeStorage(storageSyncStatus, false);
    }

    @Operation(summary = "Retrieves sync status entity for the storage.")
    @RequestMapping(value = "/sync/{id}", method = RequestMethod.GET)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "404", description = "no sync status for this storage")
    })
    public StorageSyncStatus getSyncStatusOfStorage(
            @Parameter(description = "id of the storage", required = true) @PathVariable("id") String id) {
        StorageSyncStatus syncStatusOfStorage = storageSyncStatusStore.findSyncStatusOfStorage(id);
        notNull(syncStatusOfStorage, () -> new MissingObject("storage sync status of configuration with specified id", id));
        return syncStatusOfStorage;
    }

    @Operation(summary = "Updates a logical storage.", description = "Only name, priority and note flags are updatable, others are ignored.")
    @Transactional
    @RequestMapping(value = "/update", method = RequestMethod.POST)
    public Storage update(
            @Parameter(description = "update DTO of the logical storage entity", required = true)
            @RequestBody @Valid StorageUpdateDto storageUpdateDto) {
        Storage storage = storageStore.find(storageUpdateDto.getId());
        notNull(storage, () -> new MissingObject(Storage.class, storageUpdateDto.getId()));
        storage.setName(storageUpdateDto.getName());
        storage.setPriority(storageUpdateDto.getPriority());
        storage.setNote(storageUpdateDto.getNote());
        storageStore.save(storage);
        return storage;
    }

    @Operation(summary = "Removes a logical storage.")
    @Transactional
    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "not authorized or can't be deleted because count of logical storage would be less than the configured minimum")
    })
    public void delete(
            @Parameter(description = "id of the logical storage", required = true) @PathVariable("id") String id) throws ForbiddenByConfigException {
        int minStorageCount = systemStateService.get().getMinStorageCount();
        Storage storage = storageStore.find(id);
        notNull(storage, () -> new MissingObject(Storage.class, id));
        if (!(storageStore.getCount() > minStorageCount))
            throw new ForbiddenByConfigException("cant delete storage - actual number of storages (" + minStorageCount +
                    ") is the configured minimum");
        storageStore.delete(storage);
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
    public void setStorageAdministrationService(StorageAdministrationService storageAdministrationService) {
        this.storageAdministrationService = storageAdministrationService;
    }

    @Autowired
    public void setStorageSyncStatusStore(StorageSyncStatusStore storageSyncStatusStore) {
        this.storageSyncStatusStore = storageSyncStatusStore;
    }

    @Autowired
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }
}
