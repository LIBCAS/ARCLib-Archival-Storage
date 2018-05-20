package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.BadRequestException;
import cz.cas.lib.arcstorage.service.ArchivalService;
import cz.cas.lib.arcstorage.service.exception.state.*;
import cz.cas.lib.arcstorage.service.exception.storage.FilesCorruptedAtStoragesException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

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

@RestController
@RequestMapping("/api/storage")
public class AipApi {

    private ArchivalService archivalService;
    private Path tmpFolder;

    @ApiOperation(value = "Return specified AIP as a ZIP package")
    @RequestMapping(value = "/{aipId}", method = RequestMethod.GET)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP successfully returned"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 503, message = "all attached logical storages are currently unreachable"),
            @ApiResponse(code = 500, message = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    public void getAip(
            @ApiParam(value = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @ApiParam(value = "true to return all XMLs, otherwise only the latest is returned") @RequestParam(value = "all", defaultValue = "false") boolean all,
            HttpServletResponse response)
            throws IOException, RollbackStateException, DeletedStateException, StillProcessingStateException,
            FailedStateException, FilesCorruptedAtStoragesException, BadRequestException, RemovedStateException,
            NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        checkUUID(aipId);

        AipRetrievalResource aipRetrievalResource = archivalService.get(aipId, all);
        response.setContentType("application/zip");
        response.setStatus(200);
        response.addHeader("Content-Disposition", "attachment; filename=aip_" + aipId + ".zip");

        try (ZipOutputStream zipOut = new ZipOutputStream(new BufferedOutputStream(response.getOutputStream()));
             InputStream sipIs = new BufferedInputStream(aipRetrievalResource.getSip())) {
            zipOut.putNextEntry(new ZipEntry(aipId));
            IOUtils.copyLarge(sipIs, zipOut);
            zipOut.closeEntry();
            for (Integer xmlVersion : aipRetrievalResource.getXmls().keySet()) {
                zipOut.putNextEntry(new ZipEntry(toXmlId(aipId, xmlVersion)));
                IOUtils.copyLarge(new BufferedInputStream(aipRetrievalResource.getXmls().get(xmlVersion)), zipOut);
                zipOut.closeEntry();
            }
        } finally {
            aipRetrievalResource.close();
            tmpFolder.resolve(aipRetrievalResource.getId()).toFile().delete();
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
    public void getXml(
            @ApiParam(value = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @ApiParam(value = "version number of XML, if not set the latest version is returned") @RequestParam(value = "v", defaultValue = "") Integer version,
            HttpServletResponse response) throws StillProcessingStateException,
            RollbackStateException, IOException, FailedStateException, FilesCorruptedAtStoragesException,
            BadRequestException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        checkUUID(aipId);
        ArchivalObjectDto xml = archivalService.getXml(aipId, version);
        response.setContentType("application/xml");
        response.setStatus(200);
        response.addHeader("Content-Disposition", "attachment; filename=" + xml.getStorageId() + ".xml");
        IOUtils.copyLarge(xml.getInputStream(), response.getOutputStream());
    }

    @ApiOperation(value = "Stores AIP parts (SIP and AIP XML) into Archival Storage and returns the AIP ID.", response = String.class)
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP successfully stored"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 422, message = "the checksum computed after the transfer does not match the provided checksum"),
            @ApiResponse(code = 503, message = "some attached logical storage is currently not reachable"),
            @ApiResponse(code = 500, message = "no logical storage attached, or other internal server error")
    })
    public String save(
            @ApiParam(value = "SIP file", required = true) @RequestParam("sip") MultipartFile sip,
            @ApiParam(value = "AIP XML file", required = true) @RequestParam("aipXml") MultipartFile aipXml,
            @ApiParam(value = "value of the SIP checksum", required = true) @RequestParam("sipChecksumValue") String sipChecksumValue,
            @ApiParam(value = "type of the SIP checksum", required = true) @RequestParam("sipChecksumType") ChecksumType sipChecksumType,
            @ApiParam(value = "value of the AIP XML checksum", required = true) @RequestParam("aipXmlChecksumValue") String aipXmlChecksumValue,
            @ApiParam(value = "type of the AIP XML checksum", required = true) @RequestParam("aipXmlChecksumType") ChecksumType aipXmlChecksumType,
            @ApiParam(value = "UUID of the AIP, generated if not specifies") @RequestParam(value = "UUID", defaultValue = "") String id)
            throws IOException, SomeLogicalStoragesNotReachableException, BadRequestException, NoLogicalStorageAttachedException {
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

        AipDto aipDto = new AipDto(aipId, sip.getInputStream(), sipChecksum, aipXml.getInputStream(), aipXmlChecksum);
        archivalService.save(aipDto);
        return aipId;
    }

    @ApiOperation(value = "Versioning of the AIP XML: stores new AIP XML into Archival Storage.")
    @RequestMapping(value = "/{aipId}/update", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP XML successfully stored"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 422, message = "the checksum computed after the transfer does not match the provided checksum"),
            @ApiResponse(code = 503, message = "some attached logical storage is currently not reachable"),
            @ApiResponse(code = 500, message = "no logical storage attached, or other internal server error")
    })
    public void saveXml(
            @ApiParam(value = "AIP id", required = true) @PathVariable("aipId") String aipId, @RequestParam("xml") MultipartFile xml,
            @ApiParam(value = "AIP XML checksum value", required = true) @RequestParam("checksumValue") String checksumValue,
            @ApiParam(value = "AIP XML checksum type", required = true) @RequestParam("checksumType") ChecksumType checksumType,
            @ApiParam(value = "version number of the AIP XML, is automatically set to the lastVersion+1 if not specified") @RequestParam(value = "v", defaultValue = "") Integer version) throws IOException,
            SomeLogicalStoragesNotReachableException, BadRequestException, NoLogicalStorageAttachedException {
        checkUUID(aipId);
        Checksum checksum = new Checksum(checksumType, checksumValue);
        checkChecksumFormat(checksum);

        archivalService.saveXml(aipId, xml.getInputStream(), checksum, version);
    }

    @ApiOperation(value = "Logically removes AIP by setting its state to REMOVED.")
    @RequestMapping(value = "/{aipId}/remove", method = RequestMethod.PUT)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP successfully removed"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 503, message = "some attached logical storage is currently not reachable"),
            @ApiResponse(code = 500, message = "no logical storage attached, or other internal server error")
    })
    public void remove(
            @ApiParam(value = "AIP id", required = true) @PathVariable("aipId") String aipId) throws DeletedStateException, StillProcessingStateException,
            RollbackStateException, StorageException, FailedStateException, SomeLogicalStoragesNotReachableException,
            BadRequestException, NoLogicalStorageAttachedException {
        checkUUID(aipId);
        archivalService.remove(aipId);
    }

    @ApiOperation(value = "Renews logically removed AIP by setting its state to ARCHIVED.")
    @RequestMapping(value = "/{aipId}/renew", method = RequestMethod.PUT)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP successfully renewed"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 503, message = "some attached logical storage is currently not reachable"),
            @ApiResponse(code = 500, message = "no logical storage attached, or other internal server error")
    })
    public void renew(
            @ApiParam(value = "AIP id", required = true) @PathVariable("aipId") String aipId) throws DeletedStateException, StillProcessingStateException,
            RollbackStateException, StorageException, FailedStateException, SomeLogicalStoragesNotReachableException,
            BadRequestException, NoLogicalStorageAttachedException {
        checkUUID(aipId);
        archivalService.renew(aipId);
    }

    @ApiOperation(value = "Physically deletes AIP and sets its state to DELETED. Only the SIP part is deleted, but the package is not accessible through the API anymore.")
    @RequestMapping(value = "/{aipId}", method = RequestMethod.DELETE)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP successfully deleted"),
            @ApiResponse(code = 403, message = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 503, message = "some attached logical storage is currently not reachable"),
            @ApiResponse(code = 500, message = "no logical storage attached, or other internal server error")
    })
    public void delete(
            @ApiParam(value = "AIP id", required = true) @PathVariable("aipId") String aipId) throws StillProcessingStateException, RollbackStateException,
            StorageException, FailedStateException, SomeLogicalStoragesNotReachableException, BadRequestException, NoLogicalStorageAttachedException {
        checkUUID(aipId);
        archivalService.delete(aipId);
    }

    @ApiOperation(notes = "If the AIP is in PROCESSING state or the storage is not reachable, the storage checksums are not filled and the consistent flag is set to false", value = "Retrieves information about AIP containing state, whether is consistent etc from the given storage.", response = AipStateInfoDto.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "AIP successfully deleted"),
            @ApiResponse(code = 400, message = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(code = 500, message = "internal server error")
    })
    @RequestMapping(value = "/{aipId}/state", method = RequestMethod.GET)
    public AipStateInfoDto getAipState(
            @ApiParam(value = "AIP id", required = true) @PathVariable("aipId") String aipId,
            @ApiParam(value = "id of the logical storage", required = true) @RequestParam(value = "storageId") String storageId)
            throws BadRequestException, StorageException {
        checkUUID(aipId);
        return archivalService.getAipState(aipId, storageId);
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
}
