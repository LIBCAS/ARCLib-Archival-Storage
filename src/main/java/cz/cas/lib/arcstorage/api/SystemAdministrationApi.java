package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.exception.ForbiddenByConfigException;
import cz.cas.lib.arcstorage.security.Roles;
import cz.cas.lib.arcstorage.service.SystemAdministrationService;
import cz.cas.lib.arcstorage.service.SystemStateService;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.storagesync.backup.BackupExportService;
import cz.cas.lib.arcstorage.storagesync.backup.BackupProcessException;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.SynchronizationInProgressException;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.validation.Valid;
import java.io.IOException;
import java.time.Instant;

@Slf4j
@RestController
@RequestMapping("/api/administration")
@RolesAllowed(Roles.ADMIN)
public class SystemAdministrationApi {

    private SystemStateService systemStateService;
    private BackupExportService backupExportService;
    private SystemAdministrationService systemAdministrationService;

    @ApiOperation(value = "Updates systemState of the Archival Storage.", response = SystemState.class)
    @RequestMapping(value = "/config", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(code = 400, message = "the config breaks the basic LTP policy (e.g. count of storages)"),
            @ApiResponse(code = 409, message = "provided config has different id than that stored in DB (only one config object is allowed)")
    })
    public SystemState save(
            @ApiParam(value = "systemState object", required = true) @RequestBody @Valid SystemState systemState
    ) {
        log.info("Saving new or updating an existing systemState of the Archival Storage.");
        return systemStateService.save(systemState);
    }

    @ApiOperation(value = "Returns configuration", response = SystemState.class)
    @RequestMapping(value = "/config", method = RequestMethod.GET)
    public SystemState get() {
        return systemStateService.get();
    }

    @ApiOperation(value = "Cleans storage. In order to succeed, all storages must be reachable.", notes = "By default only failed package are cleaned (i.e. rolled back/deleted from all storages). " +
            "If 'all' is set to true, also currently processing/stucked packages and tmp folder is cleaned.")
    @RequestMapping(value = "/cleanup", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(code = 503, message = "some logical storage unreachable"),
            @ApiResponse(code = 403, message = "some logical storage is synchronizing")
    })
    public void cleanup(
            @ApiParam(value = "all") @RequestParam(value = "all", defaultValue = "false") boolean all) throws SomeLogicalStoragesNotReachableException, IOException, NoLogicalStorageAttachedException, SynchronizationInProgressException {
        systemAdministrationService.cleanup(all);
    }

    @ApiOperation(value = "Cleans one particular object. In order to succeed, all storages must be reachable.",
            notes = "Object must be in failed or processing state. If the object is in processing state, " +
                    "caller must ensure that the object is not actually processing but the state was left processing because" +
                    "some unexpected error has occurred (e.g. system crashed).")
    @RequestMapping(value = "/cleanup/object/{objId}", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(code = 503, message = "some logical storage unreachable"),
            @ApiResponse(code = 403, message = "some logical storage is synchronizing")
    })
    public void cleanupOne(
            @ApiParam(value = "database id of the object") @PathVariable(value = "objId") String objId) throws SomeLogicalStoragesNotReachableException, IOException, NoLogicalStorageAttachedException, SynchronizationInProgressException {
        systemAdministrationService.cleanupOne(objId);
    }

    @ApiOperation(value = "Recovers database from the storage.", notes = " Synchronous call. System has to be set to read-only mode. Currently the recovery is implemented only for local FS/ZFS. If the object metadata is present in DB and also at storage, the metadata are compared and if not equal, system logs (warn) the conflict and if override parameter is set to true, DB metadata are overridden by storage metadata. System also logs (error) all objects which are in DB but not at storage. ")
    @RequestMapping(value = "/recover_db", method = RequestMethod.POST)
    @Transactional
    @ApiResponses(value = {
            @ApiResponse(code = 403, message = "Not authorized or system is in read-write mode"),
            @ApiResponse(code = 503, message = "logical storage unreachable")
    })
    public void recoverDbFromStorage(
            @ApiParam(value = "storage id") @RequestParam("storageId") String storageId,
            @ApiParam(value = "whether to override conflicting metadata in DB with those from storage") @RequestParam(value = "override", required = false, defaultValue = "false") boolean overrideConflict) throws StorageException {
        systemAdministrationService.recoverDb(storageId, overrideConflict);
    }

    @ApiOperation(value = "Exports all objects which have changed in the specified time range.", notes = "Once the reachability of export location is verified, the response is returned and process continues asynchronously. If since/until is not filled, MIN/MAX is used.")
    @RequestMapping(value = "/backup", method = RequestMethod.GET)
    @ApiResponses(value = {
            @ApiResponse(code = 500, message = "Export location unreachable or other internal error.")
    })
    public void backup(
            @ApiParam(value = "since") @RequestParam(value = "since", required = false) Instant since,
            @ApiParam(value = "until") @RequestParam(value = "until", required = false) Instant until) throws BackupProcessException, ForbiddenByConfigException {
        backupExportService.exportDataForBackup(since, until);
    }

    @Inject
    public void setBackupExportService(BackupExportService backupExportService) {
        this.backupExportService = backupExportService;
    }

    @Inject
    public void setsystemStateService(SystemStateService systemStateService) {
        this.systemStateService = systemStateService;
    }

    @Inject
    public void setSystemAdministrationService(SystemAdministrationService systemAdministrationService) {
        this.systemAdministrationService = systemAdministrationService;
    }
}
