package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.dto.AipDto;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ChecksumType;
import cz.cas.lib.arcstorage.exception.BadRequestException;
import cz.cas.lib.arcstorage.exception.ForbiddenByConfigException;
import cz.cas.lib.arcstorage.security.Roles;
import cz.cas.lib.arcstorage.security.user.UserDetails;
import cz.cas.lib.arcstorage.service.AipService;
import cz.cas.lib.arcstorage.service.ArchivalDbService;
import cz.cas.lib.arcstorage.service.ArchivalService;
import cz.cas.lib.arcstorage.service.exception.BadXmlVersionProvidedException;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.state.*;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import java.io.IOException;
import java.util.UUID;

import static cz.cas.lib.arcstorage.util.Utils.checkChecksumFormat;
import static cz.cas.lib.arcstorage.util.Utils.checkUUID;

@Slf4j
@RestController
@RequestMapping("/api/storage")
public class ObjectWriteApi {

    private AipService aipService;
    private UserDetails userDetails;
    private ArchivalDbService archivalDbService;
    private ArchivalService archivalService;
    private boolean forgetFeatureAllowed;

    @Operation(summary = "Stores AIP parts (SIP and AIP XML) into Archival Storage and returns the AIP ID.")
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "AIP successfully stored"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "422", description = "the checksum computed after the transfer does not match the provided checksum"),
            @ApiResponse(responseCode = "503", description = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(responseCode = "500", description = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public String saveAip(
            @Parameter(description = "SIP file", required = true) @RequestParam("sip") MultipartFile sip,
            @Parameter(description = "AIP XML file", required = true) @RequestParam("aipXml") MultipartFile aipXml,
            @Parameter(description = "value of the SIP checksum", required = true) @RequestParam("sipChecksumValue") String sipChecksumValue,
            @Parameter(description = "type of the SIP checksum", required = true) @RequestParam("sipChecksumType") ChecksumType sipChecksumType,
            @Parameter(description = "value of the AIP XML checksum", required = true) @RequestParam("aipXmlChecksumValue") String aipXmlChecksumValue,
            @Parameter(description = "type of the AIP XML checksum", required = true) @RequestParam("aipXmlChecksumType") ChecksumType aipXmlChecksumType,
            @Parameter(description = "UUID of the AIP, generated if not specifies") @RequestParam(value = "UUID", defaultValue = "") String id)
            throws IOException, SomeLogicalStoragesNotReachableException, BadRequestException, NoLogicalStorageAttachedException, ReadOnlyStateException {
        String aipId;
        if (id == null || id.isEmpty())
            aipId = UUID.randomUUID().toString();
        else {
            checkUUID(id);
            aipId = id;
        }

        Checksum sipChecksum = new Checksum(sipChecksumType, sipChecksumValue);
        checkChecksumFormat(sipChecksum);

        Checksum aipXmlChecksum = new Checksum(aipXmlChecksumType, aipXmlChecksumValue);
        checkChecksumFormat(aipXmlChecksum);

        AipDto aipDto = new AipDto(userDetails.getId(), aipId, sip.getInputStream(), sipChecksum, aipXml.getInputStream(), aipXmlChecksum);
        aipService.saveAip(aipDto);
        return aipId;
    }

    @Operation(summary = "Versioning of the AIP XML: stores new AIP XML into Archival Storage.", description =
            "Sync flag is set to false for batch AIP XML updates from Ingest. Sync flag is set to true for AIP XML" +
                    " updates invoked from user's interaction with the AIP XML editor in GUI.")
    @RequestMapping(value = "/{aipId}/update", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "AIP XML successfully stored"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(responseCode = "409", description = "bad XML version number provided (not following the sequence)"),
            @ApiResponse(responseCode = "422", description = "the checksum computed after the transfer does not match the provided checksum"),
            @ApiResponse(responseCode = "503", description = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(responseCode = "500", description = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void saveXml(
            @Parameter(description = "AIP id", required = true) @PathVariable("aipId") String aipId, @RequestParam("xml") MultipartFile xml,
            @Parameter(description = "AIP XML checksum value", required = true) @RequestParam("checksumValue") String checksumValue,
            @Parameter(description = "AIP XML checksum type", required = true) @RequestParam("checksumType") ChecksumType checksumType,
            @Parameter(description = "synchronous/asynchronous processing flag") @RequestParam(value = "sync", defaultValue = "false") boolean sync,
            @Parameter(description = "version number of the AIP XML, is automatically set to the lastVersion+1 if not specified")
            @RequestParam(value = "v", defaultValue = "") Integer version) throws IOException, SomeLogicalStoragesNotReachableException,
            BadRequestException, NoLogicalStorageAttachedException, StillProcessingStateException, FailedStateException, RollbackStateException,
            DeletedStateException, BadXmlVersionProvidedException, ReadOnlyStateException {
        checkUUID(aipId);
        Checksum checksum = new Checksum(checksumType, checksumValue);
        checkChecksumFormat(checksum);
        aipService.saveXml(aipId, xml.getInputStream(), checksum, version, sync);
    }

    @Operation(summary = "Logically removes object by setting its state to REMOVED.", description = "Not allowed for AIP XML objects.")
    @RequestMapping(value = "/{objId}/remove", method = RequestMethod.PUT)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "object successfully removed"),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current object state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(responseCode = "500", description = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void remove(
            @Parameter(description = "object id", required = true) @PathVariable("objId") String objId) throws DeletedStateException, StillProcessingStateException,
            RollbackStateException, StorageException, FailedStateException, SomeLogicalStoragesNotReachableException,
            BadRequestException, NoLogicalStorageAttachedException, ReadOnlyStateException {
        checkUUID(objId);
        archivalService.removeObject(objId);
    }

    @Operation(summary = "Renews logically removed object by setting its state to ARCHIVED.", description = "Not allowed for AIP XML objects.")
    @RequestMapping(value = "/{objId}/renew", method = RequestMethod.PUT)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "object successfully renewed"),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current object state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(responseCode = "500", description = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void renew(
            @Parameter(description = "object id", required = true) @PathVariable("objId") String objId) throws DeletedStateException, StillProcessingStateException,
            RollbackStateException, StorageException, FailedStateException, SomeLogicalStoragesNotReachableException,
            BadRequestException, NoLogicalStorageAttachedException, ReadOnlyStateException {
        checkUUID(objId);
        archivalService.renewObject(objId);
    }

    @Operation(summary = "Physically deletes object and sets its state to DELETED.", description = "Not allowed for AIP XML objects.")
    @RequestMapping(value = "/{objId}", method = RequestMethod.DELETE)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "object successfully deleted"),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current object state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(responseCode = "500", description = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void delete(
            @Parameter(description = "object id", required = true) @PathVariable("objId") String objId) throws StillProcessingStateException, RollbackStateException,
            FailedStateException, SomeLogicalStoragesNotReachableException, BadRequestException, NoLogicalStorageAttachedException, ReadOnlyStateException {
        checkUUID(objId);
        archivalService.deleteObject(objId);
    }

    @Operation(summary = "Deletes the object data.",
            description = "If the object is AIP, it has to contain exactly one AIP XML which is also rolled back. " +
                    "Metadata and DB records remains persistent with state set to ROLLED_BACK. " +
                    "If the object to rollback does not exists, then do nothing.")
    @RequestMapping(value = "/{objId}/rollback", method = RequestMethod.DELETE)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "object successfully rolled back or rollback skipped because the object was not found"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "403", description = "trying to rollback AIP XML (there is a special endpoint for that case) OR trying to rollback AIP which has more than one AIP XML linked"),
            @ApiResponse(responseCode = "503", description = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(responseCode = "500", description = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void rollback(
            @Parameter(description = "object id", required = true) @PathVariable("objId") String objId) throws BadRequestException, SomeLogicalStoragesNotReachableException, NoLogicalStorageAttachedException {
        checkUUID(objId);
        ArchivalObject object = archivalDbService.lookForObject(objId);
        if (object == null) {
            log.debug("Skipped rollback of object " + objId + " as it is not even registered in Archival Storage DB");
            return;
        }
        if (object instanceof AipSip && ((AipSip) object).getXmls().size() > 1)
            throw new UnsupportedOperationException("AIP can be rolled back only if it contains exactly one AIP XML");
        if (object instanceof AipXml) {
            throw new UnsupportedOperationException("AIP XML has to be rolled back through special endpoint");
        }
        archivalService.rollbackObject(object);
    }

    @Operation(summary = "Forgets the object.",
            description = "If the object is AIP, it has to contain exactly one AIP XML which is also forget. " +
                    "All Metadata and DB records are deleted together with file content." +
                    "If the object to forget does not exists, then do nothing.")
    @RequestMapping(value = "/{objId}/forget", method = RequestMethod.DELETE)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "object successfully forgotten or forget call skipped because the object was not found"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "403", description = "forget feature not allowed or trying to forget AIP XML (there is a special endpoint for that case) OR trying to forget AIP which has more than one AIP XML linked"),
            @ApiResponse(responseCode = "503", description = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(responseCode = "500", description = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void forget(
            @Parameter(description = "object id", required = true) @PathVariable("objId") String objId) throws BadRequestException, SomeLogicalStoragesNotReachableException, NoLogicalStorageAttachedException, ForbiddenByConfigException, StorageException, StateException {
        if (!forgetFeatureAllowed) {
            throw new ForbiddenByConfigException("forget feature not allowed");
        }
        checkUUID(objId);
        ArchivalObject object = archivalDbService.lookForObject(objId);
        if (object == null) {
            log.debug("Skipped forget of object " + objId + " as it is not even registered in Archival Storage DB");
            return;
        }
        if (object instanceof AipSip && ((AipSip) object).getXmls().size() > 1)
            throw new UnsupportedOperationException("AIP can be forgotten only if it contains exactly one AIP XML");
        if (object instanceof AipXml) {
            throw new UnsupportedOperationException("AIP XML has to be forgotten through special endpoint");
        }
        archivalService.forgetObject(object);
    }

    @Operation(summary = "Deletes data of the latest AIP XML. Version set in the path variable must be the latest AIP XML of the AIP.",
            description = "If the AIP or XML does not exists, Archival storage silently skips the request- ")
    @RequestMapping(value = "/{aipId}/rollbackXml/{xmlVersion}", method = RequestMethod.DELETE)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "object successfully rolled back or rollback skipped because the object was not found"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "403", description = "if the XML is version 1 (whole AIP should be rolled back instead) or if this is not the latest XML version of the AIP (newer versions exist)"),
            @ApiResponse(responseCode = "503", description = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(responseCode = "500", description = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void rollbackXml(
            @Parameter(description = "AIP id", required = true) @PathVariable("aipId") String aipId,
            @Parameter(description = "XML version", required = true) @PathVariable("xmlVersion") int xmlVersion) throws BadRequestException, SomeLogicalStoragesNotReachableException, NoLogicalStorageAttachedException, StateException, StorageException {
        checkUUID(aipId);
        if (xmlVersion < 2) {
            throw new UnsupportedOperationException("Only XMLs created as metadata update (version 2 and higher) can be rolled back through this endpoint");
        }
        aipService.rollbackOrForgetXml(aipId, xmlVersion, false);
    }

    @Operation(summary = "Deletes the latest AIP XML. Version set in the path variable must be the latest AIP XML of the AIP.",
            description = "If the AIP or XML does not exists, Archival storage silently skips the request- ")
    @RequestMapping(value = "/{aipId}/forgetXml/{xmlVersion}", method = RequestMethod.DELETE)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "object successfully forgotten or forget call skipped because the object was not found"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "403", description = "forget feature not allowed or the XML is version 1 (whole AIP should be forgotten instead) or if this is not the latest XML version of the AIP (newer versions exist)"),
            @ApiResponse(responseCode = "503", description = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(responseCode = "500", description = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void forgetXml(
            @Parameter(description = "AIP id", required = true) @PathVariable("aipId") String aipId,
            @Parameter(description = "XML version", required = true) @PathVariable("xmlVersion") int xmlVersion) throws BadRequestException, SomeLogicalStoragesNotReachableException, NoLogicalStorageAttachedException, StateException, ForbiddenByConfigException, StorageException {
        if (!forgetFeatureAllowed) {
            throw new ForbiddenByConfigException("forget feature not allowed");
        }
        checkUUID(aipId);
        if (xmlVersion < 2) {
            throw new UnsupportedOperationException("Only XMLs created as metadata update (version 2 and higher) can be forgotten through this endpoint");
        }
        aipService.rollbackOrForgetXml(aipId, xmlVersion, true);
    }

    @Inject
    public void setAipService(AipService aipService) {
        this.aipService = aipService;
    }

    @Inject
    public void setUserDetails(UserDetails userDetails) {
        this.userDetails = userDetails;
    }

    @Inject
    public void setArchivalDbService(ArchivalDbService archivalDbService) {
        this.archivalDbService = archivalDbService;
    }

    @Inject
    public void setArchivalService(ArchivalService archivalService) {
        this.archivalService = archivalService;
    }

    @Inject
    public void setForgetFeatureAllowed(@Value("${arcstorage.optionalFeatures.forgetObject}") boolean forgetFeatureAllowed) {
        this.forgetFeatureAllowed = forgetFeatureAllowed;
    }
}