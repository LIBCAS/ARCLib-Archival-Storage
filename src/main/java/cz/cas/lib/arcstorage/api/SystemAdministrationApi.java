package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.dto.SystemStateUpdateDto;
import cz.cas.lib.arcstorage.exception.ForbiddenByConfigException;
import cz.cas.lib.arcstorage.jms.JmsHealthCheckException;
import cz.cas.lib.arcstorage.jms.JmsQueueNotEmptyException;
import cz.cas.lib.arcstorage.security.Roles;
import cz.cas.lib.arcstorage.service.IntervalJobService;
import cz.cas.lib.arcstorage.service.SystemAdministrationService;
import cz.cas.lib.arcstorage.service.SystemStateService;
import cz.cas.lib.arcstorage.service.exception.state.StateException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.storagesync.backup.BackupExportService;
import cz.cas.lib.arcstorage.storagesync.backup.BackupProcessException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.annotation.security.RolesAllowed;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

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
    private IntervalJobService intervalJobService;

    @Operation(summary = "Cleans storage. In order to succeed, all storages must be reachable.", description = "By default only failed package are cleaned (i.e. rolled back/deleted from all storages). " +
            "If 'all' is set to true, also currently processing/stucked packages and tmp folder is cleaned.")
    @PostMapping("/cleanup")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "503", description = "some logical storage unreachable"),
    })
    public void cleanup(
            @Parameter(description = "all") @RequestParam(value = "all", defaultValue = "false") boolean all) throws SomeLogicalStoragesNotReachableException, IOException, NoLogicalStorageAttachedException, JmsHealthCheckException {
        systemAdministrationService.cleanup(all);
    }

    @Operation(summary = "Cleans one particular object. In order to succeed, all storages must be reachable.",
            description = "Object must be in failed or processing state. If the object is in processing state, " +
                    "caller must ensure that the object is not actually processing but the state was left processing because" +
                    "some unexpected error has occurred (e.g. system crashed).")
    @PostMapping("/cleanup/object/{objId}")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "503", description = "some logical storage unreachable"),
    })
    public void cleanupOne(
            @Parameter(description = "database id of the object") @PathVariable(value = "objId") String objId) throws JmsHealthCheckException, StateException {
        systemAdministrationService.cleanupOne(objId);
    }

    @Operation(summary = "Recovers database from the storage.", description = " Synchronous call. System has to be set to read-only mode. Currently the recovery is implemented only for local FS/ZFS. If the object metadata is present in DB and also at storage, the metadata are compared and if not equal, system logs (warn) the conflict and if override parameter is set to true, DB metadata are overridden by storage metadata. System also logs (error) all objects which are in DB but not at storage. ")
    @PostMapping("/recover_db")
    @Transactional
    @ApiResponses(value = {
            @ApiResponse(responseCode = "403", description = "Not authorized or system is in read-write mode"),
            @ApiResponse(responseCode = "503", description = "logical storage unreachable")
    })
    public void recoverDbFromStorage(
            @Parameter(description = "storage id") @RequestParam("storageId") String storageId,
            @Parameter(description = "whether to override conflicting metadata in DB with those from storage") @RequestParam(value = "override", required = false, defaultValue = "false") boolean overrideConflict) throws StorageException {
        systemAdministrationService.recoverDb(storageId, overrideConflict);
    }

    @Operation(summary = "Exports all objects which have changed in the specified time range.", description = "Once the reachability of export location is verified, the response is returned and process continues asynchronously. If since/until is not filled, MIN/MAX is used.")
    @GetMapping("/backup")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "500", description = "Export location unreachable or other internal error.")
    })
    public void backup(
            @Parameter(description = "since") @RequestParam(value = "since", required = false) Instant since,
            @Parameter(description = "until") @RequestParam(value = "until", required = false) Instant until) throws BackupProcessException, ForbiddenByConfigException {
        backupExportService.exportDataForBackup(since, until);
    }

    @Operation(summary = "Returns configuration")
    @GetMapping("/config")
    public SystemState get() {
        return systemStateService.get();
    }

    @Operation(summary = "Switches primary storage.",
            description = "In order to succeed, all storage queues must be empty.")
    @PostMapping("/config/storage/{id}/primary")
    public void switchPrimaryStorage(
            @Parameter(description = "ID of new primary storage") @PathVariable(value = "id") String id) throws SomeLogicalStoragesNotReachableException, InterruptedException, JmsQueueNotEmptyException {
        systemAdministrationService.switchPrimaryStorage(id);
    }

    @Operation(summary = "Sets system to read-only mode")
    @PostMapping("/config/read_only")
    public void setReadOnly() {
        systemStateService.setReadOnly(systemStateService.get(), null);
    }

    @Operation(summary = "Sets system to read-write mode")
    @PostMapping("/config/read_write")
    public void setReadWrite() {
        systemStateService.setReadWrite(systemStateService.get());
    }

    @Operation(summary = "Updates system properties")
    @PostMapping("/config")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "400", description = "the config breaks the basic LTP policy (e.g. count of storages)"),
    })
    public SystemState update(@Parameter(description = "dto", required = true) @RequestBody @Valid SystemStateUpdateDto systemStateDto) {
        SystemState stateInDb = systemStateService.get();
        boolean reachabilityIntervalChanged = systemStateDto.getReachabilityCheckIntervalInMinutes() != stateInDb.getReachabilityCheckIntervalInMinutes();
        stateInDb.setReachabilityCheckIntervalInMinutes(systemStateDto.getReachabilityCheckIntervalInMinutes());
        stateInDb.setMinStorageCount(systemStateDto.getMinStorageCount());
        systemStateService.save(stateInDb);
        if (reachabilityIntervalChanged)
            intervalJobService.scheduleReachabilityChecks(stateInDb.getReachabilityCheckIntervalInMinutes());
        return stateInDb;
    }

    @Autowired
    public void setBackupExportService(BackupExportService backupExportService) {
        this.backupExportService = backupExportService;
    }

    @Autowired
    public void setSystemStateService(SystemStateService systemStateService) {
        this.systemStateService = systemStateService;
    }

    @Autowired
    public void setSystemAdministrationService(SystemAdministrationService systemAdministrationService) {
        this.systemAdministrationService = systemAdministrationService;
    }

    @Autowired
    public void setIntervalJobService(IntervalJobService intervalJobService) {
        this.intervalJobService = intervalJobService;
    }
}
