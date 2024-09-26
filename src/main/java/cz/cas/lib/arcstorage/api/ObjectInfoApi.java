package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.store.ArchivalObjectStore;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.BadRequestException;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.security.Role;
import cz.cas.lib.arcstorage.security.Roles;
import cz.cas.lib.arcstorage.security.user.UserDetails;
import cz.cas.lib.arcstorage.service.AipService;
import cz.cas.lib.arcstorage.service.ArchivalDbService;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storagesync.ObjectAudit;
import cz.cas.lib.arcstorage.storagesync.ObjectAuditStore;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.SynchronizationInProgressException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.annotation.security.RolesAllowed;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.util.Utils.*;

@Slf4j
@RestController
@RequestMapping("/api/storage")
public class ObjectInfoApi {

    private AipService aipService;
    private ArchivalDbService archivalDbService;
    private ArchivalObjectStore archivalObjectStore;
    private ObjectAuditStore objectAuditStore;
    private UserDetails userDetails;

    @Operation(summary = "Verifies AIP consistency at given storage and retrieves result.", description = "If the AIP is not in some final, consistent state even in DB the storage is not checked and only DB data are returned.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "AIP successfully deleted"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "403", description = "not authorized or the storage is just synchronizing"),
            @ApiResponse(responseCode = "500", description = "internal server error"),
            @ApiResponse(responseCode = "503", description = "storage is not reachable"),
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    @RequestMapping(value = "/{aipId}/info", method = RequestMethod.GET)
    public AipConsistencyVerificationResultDto getAipInfo(
            @Parameter(description = "AIP id", required = true) @PathVariable("aipId") String aipId,
            @Parameter(description = "id of the logical storage", required = true) @RequestParam(value = "storageId") String storageId)
            throws BadRequestException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException, SomeLogicalStoragesNotReachableException, SynchronizationInProgressException {
        checkUUID(aipId);
        AipSip aip = archivalDbService.getAip(aipId);
        List<AipConsistencyVerificationResultDto> aipStateAtStorageDtos = aipService.verifyAipsAtStorage(asList(aip), storageId);
        eq(aipStateAtStorageDtos.size(), 1, () -> new GeneralException("Internal server error, expected exactly one result but was: " +
                Arrays.toString(aipStateAtStorageDtos.toArray())
        ));
        return aipStateAtStorageDtos.get(0);
    }

    @Operation(description = "Retrieves the state of AIP stored in database.",
            summary = "State of AIP.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "state of AIP successfully retrieved"),
            @ApiResponse(responseCode = "404", description = "AIP with the id not found"),
    })
    @RequestMapping(value = "/{aipId}/state", method = RequestMethod.GET)
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public ObjectState getAipState(@Parameter(description = "AIP id", required = true) @PathVariable("aipId") String aipId) {
        return aipService.getAipState(aipId);
    }

    @Operation(description = "Retrieves the state of XML stored in database.",
            summary = "State of XML.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "state of XML successfully retrieved"),
            @ApiResponse(responseCode = "404", description = "XML not found"),
    })
    @RequestMapping(value = "/{aipId}/xml/{xmlVersion}/state", method = RequestMethod.GET)
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public ObjectState getXmlState(
            @Parameter(description = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @Parameter(description = "XML version", required = true) @PathVariable("xmlVersion") int xmlVersion) {
        return aipService.getXmlState(aipId, xmlVersion);
    }

    @Operation(description = "Sorted by creation date ASC. For non-admin users, only records belonging to the user's dataspace are retrieved.",
            summary = "Retrieves audit of objects.")
    @RequestMapping(value = "/audit", method = RequestMethod.POST)
    @RolesAllowed({Roles.READ, Roles.READ_WRITE, Roles.ADMIN})
    public List<ObjectAuditDto> getAudit(
            @Parameter(description = "specification of the record set", required = true) @RequestBody TimestampOffsetLimitDto req
    ) {
        String dataSpace = userDetails.getRole() == Role.ROLE_ADMIN ? null : userDetails.getDataSpace();
        List<ObjectAudit> audits = objectAuditStore.findAll(req.getFrom(), req.getCount(), dataSpace);
        return audits.stream().map(a -> new ObjectAuditDto(a.getId(), a.getIdInDatabase(), a.getIdInStorage(), a.getCreated(), a.getUser().getDataSpace(), a.getOperation())).collect(Collectors.toList());
    }

    @Operation(description = "Sorted by creation date ASC. For non-admin users, only records belonging to the user's dataspace are retrieved.",
            summary = "Retrieves metadata of objects.")
    @RequestMapping(value = "/object/info", method = RequestMethod.POST)
    @RolesAllowed({Roles.READ, Roles.READ_WRITE, Roles.ADMIN})
    public List<ObjectInfoDto> getObjectsMeta(
            @Parameter(description = "specification of the record set", required = true) @RequestBody TimestampOffsetLimitDto req
    ) {
        String dataSpace = userDetails.getRole() == Role.ROLE_ADMIN ? null : userDetails.getDataSpace();
        List<ArchivalObject> objects = archivalObjectStore.findAll(req.getFrom(), req.getCount(), dataSpace);
        return objects.stream().map(a -> ObjectInfoDto.of(a.toDto())).collect(Collectors.toList());
    }

    @Operation(description = "For non-admin users, the metadata are retrieved only if the object belongs to the users's dataspace.",
            summary = "Retrieves metadata of object.")
    @RequestMapping(value = "/object/{id}/info", method = RequestMethod.GET)
    @RolesAllowed({Roles.READ, Roles.READ_WRITE, Roles.ADMIN})
    public ObjectInfoDto getObjectMeta(
            @Parameter(description = "DB ID", required = true) @PathVariable("id") String id
    ) {
        ArchivalObject object = archivalObjectStore.find(id);
        if (object == null || (userDetails.getRole() != Role.ROLE_ADMIN && !object.getOwner().getDataSpace().equals(userDetails.getDataSpace()))) {
            throw new MissingObject(ArchivalObjectDto.class, id);
        }
        return ObjectInfoDto.of(object.toDto());
    }

    @Autowired
    public void setAipService(AipService aipService) {
        this.aipService = aipService;
    }

    @Autowired
    public void setArchivalDbService(ArchivalDbService archivalDbService) {
        this.archivalDbService = archivalDbService;
    }

    @Autowired
    public void setArchivalObjectStore(ArchivalObjectStore archivalObjectStore) {
        this.archivalObjectStore = archivalObjectStore;
    }

    @Autowired
    public void setObjectAuditStore(ObjectAuditStore objectAuditStore) {
        this.objectAuditStore = objectAuditStore;
    }

    @Autowired
    public void setUserDetails(UserDetails userDetails) {
        this.userDetails = userDetails;
    }
}