package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.BadRequestException;
import cz.cas.lib.arcstorage.security.Roles;
import cz.cas.lib.arcstorage.security.user.UserDetails;
import cz.cas.lib.arcstorage.service.ArchivalService;
import cz.cas.lib.arcstorage.service.exception.BadXmlVersionProvidedException;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.state.*;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.ObjectCouldNotBeRetrievedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.util.Utils;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
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
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static cz.cas.lib.arcstorage.util.Utils.checkChecksumFormat;
import static cz.cas.lib.arcstorage.util.Utils.checkUUID;

@Slf4j
@RestController
@RequestMapping("/api/storage")
public class AipApi {

    private ArchivalService archivalService;
    private Path tmpFolder;
    private UserDetails userDetails;

    @ApiOperation(value = "Return specified AIP as a ZIP package")
    @RequestMapping(value = "/{aipId}", method = RequestMethod.GET)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP successfully returned"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 503, message = "all attached logical storages are currently unreachable"),
            @ApiResponse(code = 500, message = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void getAip(
            @ApiParam(value = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @ApiParam(value = "true to return all XMLs, otherwise only the latest is returned") @RequestParam(value = "all", defaultValue = "false") boolean all,
            HttpServletResponse response)
            throws IOException, RollbackStateException, DeletedStateException, StillProcessingStateException,
            FailedStateException, ObjectCouldNotBeRetrievedException, BadRequestException, RemovedStateException,
            NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        checkUUID(aipId);

        AipRetrievalResource aipRetrievalResource = archivalService.getAip(aipId, all);
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

    @ApiOperation(value = "Return specified AIP XML")
    @RequestMapping(value = "/{aipId}/xml", method = RequestMethod.GET)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP XML successfully returned"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 503, message = "all attached logical storages are currently unreachable"),
            @ApiResponse(code = 500, message = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void getXml(
            @ApiParam(value = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @ApiParam(value = "version number of XML, if not set the latest version is returned") @RequestParam(value = "v", defaultValue = "") Integer version,
            HttpServletResponse response) throws StillProcessingStateException,
            RollbackStateException, IOException, FailedStateException, ObjectCouldNotBeRetrievedException,
            BadRequestException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        checkUUID(aipId);
        Utils.Pair<Integer, ObjectRetrievalResource> retrievedXml = archivalService.getXml(aipId, version);
        response.setContentType("application/xml");
        response.setStatus(200);
        response.addHeader("Content-Disposition", "attachment; filename=" + toXmlId(aipId, retrievedXml.getL()) + ".xml");
        try (InputStream is = new BufferedInputStream(retrievedXml.getR().getInputStream())) {
            IOUtils.copyLarge(is, response.getOutputStream());
        } finally {
            tmpFolder.resolve(retrievedXml.getR().getId()).toFile().delete();
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
    public String save(
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
        archivalService.saveAip(aipDto);
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
        if (sync)
            archivalService.saveXmlSynchronously(aipId, xml.getInputStream(), checksum, version);
        else
            archivalService.saveXmlAsynchronously(aipId, xml.getInputStream(), checksum, version);
    }

    @ApiOperation(value = "Logically removes AIP by setting its state to REMOVED.")
    @RequestMapping(value = "/{aipId}/remove", method = RequestMethod.PUT)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP successfully removed"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 503, message = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(code = 500, message = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void remove(
            @ApiParam(value = "AIP id", required = true) @PathVariable("aipId") String aipId) throws DeletedStateException, StillProcessingStateException,
            RollbackStateException, StorageException, FailedStateException, SomeLogicalStoragesNotReachableException,
            BadRequestException, NoLogicalStorageAttachedException, ReadOnlyStateException {
        checkUUID(aipId);
        archivalService.removeObject(aipId);
    }

    @ApiOperation(value = "Renews logically removed AIP by setting its state to ARCHIVED.")
    @RequestMapping(value = "/{aipId}/renew", method = RequestMethod.PUT)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP successfully renewed"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 503, message = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(code = 500, message = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void renew(
            @ApiParam(value = "AIP id", required = true) @PathVariable("aipId") String aipId) throws DeletedStateException, StillProcessingStateException,
            RollbackStateException, StorageException, FailedStateException, SomeLogicalStoragesNotReachableException,
            BadRequestException, NoLogicalStorageAttachedException, ReadOnlyStateException {
        checkUUID(aipId);
        archivalService.renew(aipId);
    }

    @ApiOperation(value = "Physically deletes AIP and sets its state to DELETED. Only the SIP part is deleted, but the package is not accessible through the API anymore.")
    @RequestMapping(value = "/{aipId}", method = RequestMethod.DELETE)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP successfully deleted"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 503, message = "some attached logical storage is currently not reachable or system is in readonly state"),
            @ApiResponse(code = 500, message = "no logical storage attached, or other internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    public void delete(
            @ApiParam(value = "AIP id", required = true) @PathVariable("aipId") String aipId) throws StillProcessingStateException, RollbackStateException,
            FailedStateException, SomeLogicalStoragesNotReachableException, BadRequestException, NoLogicalStorageAttachedException, ReadOnlyStateException {
        checkUUID(aipId);
        archivalService.delete(aipId);
    }

    @ApiOperation(notes = "If the AIP is in PROCESSING state or the storage is not reachable, the storage checksums are not filled and the consistent flag is set to false", value = "Retrieves information about AIP containing state, whether is consistent etc from the given storage.", response = AipStateInfoDto.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP successfully deleted"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 500, message = "internal server error")
    })
    @RolesAllowed(Roles.READ_WRITE)
    @RequestMapping(value = "/{aipId}/info", method = RequestMethod.GET)
    public AipStateInfoDto getAipInfo(
            @ApiParam(value = "AIP id", required = true) @PathVariable("aipId") String aipId,
            @ApiParam(value = "id of the logical storage", required = true) @RequestParam(value = "storageId") String storageId)
            throws BadRequestException, StorageException {
        checkUUID(aipId);
        return archivalService.getAipInfo(aipId, storageId);
    }

    @ApiOperation(notes = "Retrieves the state of AIP stored in database.",
            value = "State of AIP.", response = Boolean.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "state of AIP successfully retrieved"),
            @ApiResponse(code = 404, message = "AIP with the id not found"),
    })
    @RequestMapping(value = "/{aipId}/state", method = RequestMethod.GET)
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public ObjectState getAipState(@ApiParam(value = "AIP id", required = true) @PathVariable("aipId") String aipId) {
        return archivalService.getAipState(aipId);
    }

    /**
     * Retrieves state of Archival Storage.
     *
     * @return
     */
    //@RequestMapping(value = "/state", method = RequestMethod.GET)
    public List<StorageStateDto> getStorageState() throws NoLogicalStorageAttachedException {
        return archivalService.getStorageState();
    }

    @Inject
    public void setArchivalService(ArchivalService archivalService) {
        this.archivalService = archivalService;
    }

    @Inject
    public void setTmpFolder(@Value("${arcstorage.tmp-folder}") String path) {
        this.tmpFolder = Paths.get(path);
    }

    @Inject
    public void setUserDetails(UserDetails userDetails) {
        this.userDetails = userDetails;
    }
}
