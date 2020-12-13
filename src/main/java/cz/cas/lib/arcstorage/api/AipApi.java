package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.BadRequestException;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.security.Roles;
import cz.cas.lib.arcstorage.security.user.UserDetails;
import cz.cas.lib.arcstorage.service.AipService;
import cz.cas.lib.arcstorage.service.ArchivalDbService;
import cz.cas.lib.arcstorage.service.ArchivalService;
import cz.cas.lib.arcstorage.service.exception.BadXmlVersionProvidedException;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.state.*;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.ObjectCouldNotBeRetrievedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.storagesync.exception.SynchronizationInProgressException;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static cz.cas.lib.arcstorage.util.Utils.*;

@Slf4j
@RestController
@RequestMapping("/api/storage")
public class AipApi {

    private AipService aipService;
    private Path tmpFolder;
    private UserDetails userDetails;
    private ArchivalDbService archivalDbService;
    private ArchivalService archivalService;

    @ApiOperation(value = "Return specified AIP as a ZIP package")
    @RequestMapping(value = "/{aipId}", method = RequestMethod.GET)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP successfully returned"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 503, message = "all attached logical storages are currently unreachable"),
            @ApiResponse(code = 500, message = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ,Roles.READ_WRITE})
    public void getAip(
            @ApiParam(value = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @ApiParam(value = "true to return all XMLs, otherwise only the latest is returned") @RequestParam(value = "all", defaultValue = "false") boolean all,
            HttpServletResponse response)
            throws IOException, RollbackStateException, DeletedStateException, StillProcessingStateException,
            FailedStateException, ObjectCouldNotBeRetrievedException, BadRequestException, RemovedStateException,
            NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        checkUUID(aipId);

        AipRetrievalResource aipRetrievalResource = aipService.getAip(aipId, all);
        response.setContentType("application/zip");
        response.setStatus(200);
        response.addHeader("Content-Disposition", "attachment; filename=aip_" + aipId + ".zip");

        try (ZipOutputStream zipOut = new ZipOutputStream(new BufferedOutputStream(response.getOutputStream()));
             InputStream sipIs = new BufferedInputStream(aipRetrievalResource.getSip())) {
            zipOut.putNextEntry(new ZipEntry(aipId + ".zip"));
            IOUtils.copyLarge(sipIs, zipOut);
            zipOut.closeEntry();
            for (Integer xmlVersion : aipRetrievalResource.getXmls().keySet()) {
                try (InputStream xmlIs = new BufferedInputStream(aipRetrievalResource.getXmls().get(xmlVersion))) {
                    zipOut.putNextEntry(new ZipEntry(toXmlId(aipId, xmlVersion) + ".xml"));
                    IOUtils.copyLarge(xmlIs, zipOut);
                    zipOut.closeEntry();
                }
            }
        } finally {
            aipRetrievalResource.close();
            String tmpFileId = aipRetrievalResource.getId();
            tmpFolder.resolve(tmpFileId).toFile().delete();
            for (Integer v : aipRetrievalResource.getXmls().keySet()) {
                tmpFolder.resolve(toXmlId(tmpFileId, v)).toFile().delete();
            }
        }
    }

    @ApiOperation(value = "Return specified AIP as a ZIP package with specified files")
    @RequestMapping(value = "/{aipId}/files-specified", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP successfully returned"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 503, message = "all attached logical storages are currently unreachable"),
            @ApiResponse(code = 500, message = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public void getAipSpecified(
            @ApiParam(value = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @ApiParam(value = "Set of wanted files paths sent as RequestBody", required = true) @RequestBody Set<String> filePaths,
            HttpServletResponse response)
            throws IOException, RollbackStateException, DeletedStateException, StillProcessingStateException,
            FailedStateException, BadRequestException,
            NoLogicalStorageReachableException, NoLogicalStorageAttachedException, StorageException {
        checkUUID(aipId);

        response.setContentType("application/zip");
        response.setStatus(200);
        response.addHeader("Content-Disposition", "attachment; filename=aip_" + aipId + "_partial.zip");
        aipService.getAipSpecifiedFiles(aipId, filePaths, response.getOutputStream());
    }

    @ApiOperation(value = "Return specified AIP XML")
    @RequestMapping(value = "/{aipId}/xml", method = RequestMethod.GET)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP XML successfully returned"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 503, message = "all attached logical storages are currently unreachable"),
            @ApiResponse(code = 500, message = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ,Roles.READ_WRITE})
    public void getXml(
            @ApiParam(value = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @ApiParam(value = "version number of XML, if not set the latest version is returned") @RequestParam(value = "v", defaultValue = "") Integer version,
            HttpServletResponse response) throws StillProcessingStateException,
            RollbackStateException, IOException, FailedStateException, ObjectCouldNotBeRetrievedException,
            BadRequestException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        checkUUID(aipId);
        Pair<Integer, ObjectRetrievalResource> retrievedXml = aipService.getXml(aipId, version);
        response.setContentType("application/xml");
        response.setStatus(200);
        response.addHeader("Content-Disposition", "attachment; filename=" + toXmlId(aipId, retrievedXml.getLeft()) + ".xml");
        try (InputStream is = new BufferedInputStream(retrievedXml.getRight().getInputStream())) {
            IOUtils.copyLarge(is, response.getOutputStream());
        } finally {
            tmpFolder.resolve(retrievedXml.getRight().getId()).toFile().delete();
        }
    }

    @ApiOperation(value = "Stores AIP parts (SIP and AIP XML) into Archival Storage and returns the AIP ID.", response = String.class)
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP successfully stored"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 422, message = "the checksum computed after the transfer does not match the provided checksum"),
            @ApiResponse(code = 503, message = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(code = 500, message = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public String saveAip(
            @ApiParam(value = "SIP file", required = true) @RequestParam("sip") MultipartFile sip,
            @ApiParam(value = "AIP XML file", required = true) @RequestParam("aipXml") MultipartFile aipXml,
            @ApiParam(value = "value of the SIP checksum", required = true) @RequestParam("sipChecksumValue") String sipChecksumValue,
            @ApiParam(value = "type of the SIP checksum", required = true) @RequestParam("sipChecksumType") ChecksumType sipChecksumType,
            @ApiParam(value = "value of the AIP XML checksum", required = true) @RequestParam("aipXmlChecksumValue") String aipXmlChecksumValue,
            @ApiParam(value = "type of the AIP XML checksum", required = true) @RequestParam("aipXmlChecksumType") ChecksumType aipXmlChecksumType,
            @ApiParam(value = "UUID of the AIP, generated if not specifies") @RequestParam(value = "UUID", defaultValue = "") String id)
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

    @ApiOperation(value = "Versioning of the AIP XML: stores new AIP XML into Archival Storage.", notes =
            "Sync flag is set to false for batch AIP XML updates from Ingest. Sync flag is set to true for AIP XML" +
                    " updates invoked from user's interaction with the AIP XML editor in GUI.")
    @RequestMapping(value = "/{aipId}/update", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP XML successfully stored"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(code = 409, message = "bad XML version number provided (not following the sequence)"),
            @ApiResponse(code = 422, message = "the checksum computed after the transfer does not match the provided checksum"),
            @ApiResponse(code = 503, message = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(code = 500, message = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void saveXml(
            @ApiParam(value = "AIP id", required = true) @PathVariable("aipId") String aipId, @RequestParam("xml") MultipartFile xml,
            @ApiParam(value = "AIP XML checksum value", required = true) @RequestParam("checksumValue") String checksumValue,
            @ApiParam(value = "AIP XML checksum type", required = true) @RequestParam("checksumType") ChecksumType checksumType,
            @ApiParam(value = "synchronous/asynchronous processing flag") @RequestParam(value = "sync", defaultValue = "false") boolean sync,
            @ApiParam(value = "version number of the AIP XML, is automatically set to the lastVersion+1 if not specified")
            @RequestParam(value = "v", defaultValue = "") Integer version) throws IOException, SomeLogicalStoragesNotReachableException,
            BadRequestException, NoLogicalStorageAttachedException, StillProcessingStateException, FailedStateException, RollbackStateException,
            DeletedStateException, BadXmlVersionProvidedException, ReadOnlyStateException {
        checkUUID(aipId);
        Checksum checksum = new Checksum(checksumType, checksumValue);
        checkChecksumFormat(checksum);
        aipService.saveXml(aipId, xml.getInputStream(), checksum, version, sync);
    }

    @ApiOperation(value = "Logically removes object by setting its state to REMOVED.", notes = "Not allowed for AIP XML objects.")
    @RequestMapping(value = "/{objId}/remove", method = RequestMethod.PUT)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "object successfully removed"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current object state"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 503, message = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(code = 500, message = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void remove(
            @ApiParam(value = "object id", required = true) @PathVariable("objId") String objId) throws DeletedStateException, StillProcessingStateException,
            RollbackStateException, StorageException, FailedStateException, SomeLogicalStoragesNotReachableException,
            BadRequestException, NoLogicalStorageAttachedException, ReadOnlyStateException {
        checkUUID(objId);
        archivalService.removeObject(objId);
    }

    @ApiOperation(value = "Renews logically removed object by setting its state to ARCHIVED.", notes = "Not allowed for AIP XML objects.")
    @RequestMapping(value = "/{objId}/renew", method = RequestMethod.PUT)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "object successfully renewed"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current object state"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 503, message = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(code = 500, message = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void renew(
            @ApiParam(value = "object id", required = true) @PathVariable("objId") String objId) throws DeletedStateException, StillProcessingStateException,
            RollbackStateException, StorageException, FailedStateException, SomeLogicalStoragesNotReachableException,
            BadRequestException, NoLogicalStorageAttachedException, ReadOnlyStateException {
        checkUUID(objId);
        archivalService.renewObject(objId);
    }

    @ApiOperation(value = "Physically deletes object and sets its state to DELETED.", notes = "Not allowed for AIP XML objects.")
    @RequestMapping(value = "/{objId}", method = RequestMethod.DELETE)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "object successfully deleted"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current object state"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 503, message = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(code = 500, message = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void delete(
            @ApiParam(value = "object id", required = true) @PathVariable("objId") String objId) throws StillProcessingStateException, RollbackStateException,
            FailedStateException, SomeLogicalStoragesNotReachableException, BadRequestException, NoLogicalStorageAttachedException, ReadOnlyStateException {
        checkUUID(objId);
        archivalService.deleteObject(objId);
    }

    @ApiOperation(value = "Deletes the object data.",
            notes = "If the object is AIP, it has to contain exactly one AIP XML which is also rolled back. " +
                    "Metadata and DB records remains persistent with state set to ROLLED_BACK. " +
                    "If the object to rollback does not exists, then do nothing.")
    @RequestMapping(value = "/{objId}/rollback", method = RequestMethod.DELETE)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "object successfully rolled back or rollback skipped because the object was not found"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 403, message = "trying to rollback AIP XML (there is a special endpoint for that case) OR trying to rollback AIP which has more than one AIP XML linked"),
            @ApiResponse(code = 503, message = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(code = 500, message = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void rollback(
            @ApiParam(value = "object id", required = true) @PathVariable("objId") String objId) throws BadRequestException, SomeLogicalStoragesNotReachableException, NoLogicalStorageAttachedException {
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

    @ApiOperation(value = "Deletes data of the latest AIP XML. Version set in the path variable must be the latest AIP XML of the AIP.")
    @RequestMapping(value = "/{aipId}/rollbackXml/{xmlVersion}", method = RequestMethod.DELETE)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "object successfully rolled back or rollback skipped because the object was not found"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 403, message = "if the XML is version 1 (whole AIP should be rolled back instead) or if this is not the latest XML version of the AIP (newer versions exist)"),
            @ApiResponse(code = 503, message = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(code = 500, message = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void rollbackXml(
            @ApiParam(value = "AIP id", required = true) @PathVariable("aipId") String aipId,
            @ApiParam(value = "XML version", required = true) @PathVariable("xmlVersion") int xmlVersion) throws BadRequestException, SomeLogicalStoragesNotReachableException, NoLogicalStorageAttachedException, StateException {
        checkUUID(aipId);
        if (xmlVersion < 2) {
            throw new UnsupportedOperationException("Only XMLs created as metadata update (version 2 and higher) can be rolled back through this endpoint");
        }
        aipService.rollbackXml(aipId, xmlVersion);
    }

    @ApiOperation(value = "Verifies AIP consistency at given storage and retrieves result.", notes = "If the AIP is not in some final, consistent state even in DB the storage is not checked and only DB data are returned.", response = AipConsistencyVerificationResultDto.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP successfully deleted"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 403, message = "not authorized or the storage is just synchronizing"),
            @ApiResponse(code = 500, message = "internal server error"),
            @ApiResponse(code = 503, message = "storage is not reachable"),
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    @RequestMapping(value = "/{aipId}/info", method = RequestMethod.GET)
    public AipConsistencyVerificationResultDto getAipInfo(
            @ApiParam(value = "AIP id", required = true) @PathVariable("aipId") String aipId,
            @ApiParam(value = "id of the logical storage", required = true) @RequestParam(value = "storageId") String storageId)
            throws BadRequestException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException, SomeLogicalStoragesNotReachableException, SynchronizationInProgressException {
        checkUUID(aipId);
        AipSip aip = archivalDbService.getAip(aipId);
        List<AipConsistencyVerificationResultDto> aipStateAtStorageDtos = aipService.verifyAipsAtStorage(asList(aip), storageId);
        eq(aipStateAtStorageDtos.size(), 1, () -> new GeneralException("Internal server error, expected exactly one result but was: " +
                Arrays.toString(aipStateAtStorageDtos.toArray())
        ));
        return aipStateAtStorageDtos.get(0);
    }

    @ApiOperation(notes = "Retrieves the state of AIP stored in database.",
            value = "State of AIP.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "state of AIP successfully retrieved"),
            @ApiResponse(code = 404, message = "AIP with the id not found"),
    })
    @RequestMapping(value = "/{aipId}/state", method = RequestMethod.GET)
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public ObjectState getAipState(@ApiParam(value = "AIP id", required = true) @PathVariable("aipId") String aipId) {
        return aipService.getAipState(aipId);
    }

    @ApiOperation(notes = "Retrieves the state of XML stored in database.",
            value = "State of XML.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "state of XML successfully retrieved"),
            @ApiResponse(code = 404, message = "XML not found"),
    })
    @RequestMapping(value = "/{aipId}/xml/{xmlVersion}/state", method = RequestMethod.GET)
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public ObjectState getXmlState(
            @ApiParam(value = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @ApiParam(value = "XML version", required = true) @PathVariable("xmlVersion") int xmlVersion) {
        return aipService.getXmlState(aipId, xmlVersion);
    }

    @Inject
    public void setAipService(AipService aipService) {
        this.aipService = aipService;
    }

    @Inject
    public void setTmpFolder(@Value("${arcstorage.tmpFolder}") String path) {
        this.tmpFolder = Paths.get(path);
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
}