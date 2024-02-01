package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.BadRequestException;
import cz.cas.lib.arcstorage.exception.ForbiddenByConfigException;
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
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.SynchronizationInProgressException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
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
    private boolean forgetFeatureAllowed;

    @Operation(summary = "Return specified AIP as a ZIP package")
    @RequestMapping(value = "/{aipId}", method = RequestMethod.GET)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "AIP successfully returned"),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "all attached logical storages are currently unreachable"),
            @ApiResponse(responseCode = "500", description = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public void getAip(
            @Parameter(name = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @Parameter(name = "true to return all XMLs, otherwise only the latest is returned") @RequestParam(value = "all", defaultValue = "false") boolean all,
            HttpServletResponse response)
            throws IOException, RollbackStateException, DeletedStateException, StillProcessingStateException,
            FailedStateException, ObjectCouldNotBeRetrievedException, BadRequestException, RemovedStateException,
            NoLogicalStorageReachableException, NoLogicalStorageAttachedException {

        BiFunction<AipRetrievalResource, ZipOutputStream, Void> fn = (aipRetrievalResource, outputStream) -> {
            try {
                Path aipDataInTmpDir = tmpFolder.resolve(aipRetrievalResource.getId());
                aipService.exportAipReducedByRegexes(aipId, aipDataInTmpDir, outputStream, null);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return null;
        };

        exportAipData(aipId, all, response, fn);
    }

    @Operation(summary = "Return specified files of AIP packed in ZIP", description = "DOES NOT validate the AIP.. " +
            "does not return any AIP XML, requires some local storage to be reachable")
    @RequestMapping(value = "/{aipId}/files-specified", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "AIP successfully returned"),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "all attached logical storages are currently unreachable"),
            @ApiResponse(responseCode = "500", description = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public void getAipFilesReducedByListOfPaths(
            @Parameter(name = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @Parameter(name = "Set of wanted files paths sent as RequestBody", required = true) @RequestBody Set<String> filePaths,
            HttpServletResponse response)
            throws IOException, RollbackStateException, DeletedStateException, StillProcessingStateException,
            FailedStateException, BadRequestException,
            NoLogicalStorageReachableException, NoLogicalStorageAttachedException, StorageException {
        checkUUID(aipId);

        response.setContentType("application/zip");
        response.setStatus(200);
        response.addHeader("Content-Disposition", "attachment; filename=aip_" + aipId + "_partial.zip");
        aipService.streamAipReducedByFileListFromLocalStorage(aipId, response.getOutputStream(), filePaths);
    }

    @Operation(summary = "Return specified files of AIP packed in ZIP", description = "DOES NOT validate the AIP, " +
            "does not return any AIP XML, requires some local storage to be reachable")
    @RequestMapping(value = "/{aipId}/files-reduced", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "AIP successfully returned"),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "all attached logical storages are currently unreachable"),
            @ApiResponse(responseCode = "500", description = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public void getAipFilesReducedByRegex(
            @Parameter(name = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @Parameter(name = "Set of wanted files paths sent as RequestBody", required = true) @RequestBody @Valid DataReduction dataReduction,
            HttpServletResponse response)
            throws IOException, RollbackStateException, DeletedStateException, StillProcessingStateException,
            FailedStateException, BadRequestException,
            NoLogicalStorageReachableException, NoLogicalStorageAttachedException, StorageException {
        checkUUID(aipId);

        response.setContentType("application/zip");
        response.setStatus(200);
        response.addHeader("Content-Disposition", "attachment; filename=aip_" + aipId + "_partial.zip");
        aipService.streamAipReducedByRegexesFromLocalStorage(aipId, response.getOutputStream(), dataReduction);
    }

    @Operation(summary = "Return AIP with specified files packed in ZIP", description = "validates the AIP and if it is invalid" +
            "tries to recover it from other storage.. does not return only files but also AIP XML(s)")
    @RequestMapping(value = "/{aipId}/aip-with-files-specified", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "AIP successfully returned"),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "all attached logical storages are currently unreachable"),
            @ApiResponse(responseCode = "500", description = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public void getAipReducedByListOfFiles(
            @Parameter(name = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @Parameter(name = "Set of wanted files paths sent as RequestBody", required = true) @RequestBody Set<String> filePaths,
            @Parameter(name = "true to return all XMLs, otherwise only the latest is returned") @RequestParam(value = "all", defaultValue = "false") boolean all,
            HttpServletResponse response)
            throws IOException, RollbackStateException, DeletedStateException, StillProcessingStateException,
            FailedStateException, BadRequestException,
            NoLogicalStorageReachableException, NoLogicalStorageAttachedException, ObjectCouldNotBeRetrievedException, RemovedStateException {

        BiFunction<AipRetrievalResource, ZipOutputStream, Void> fn = (aipRetrievalResource, outputStream) -> {
            try {
                Path aipDataInTmpDir = tmpFolder.resolve(aipRetrievalResource.getId());
                aipService.exportAipReducedByFileList(aipId, aipDataInTmpDir, outputStream, filePaths);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return null;
        };

        exportAipData(aipId, all, response, fn);
    }

    @Operation(summary = "Return AIP with specified files packed in ZIP", description = "validates the AIP and if it is invalid" +
            "tries to recover it from other storage.. does not return only files but also AIP XML(s)")
    @RequestMapping(value = "/{aipId}/aip-with-files-reduced", method = RequestMethod.POST)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "AIP successfully returned"),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "all attached logical storages are currently unreachable"),
            @ApiResponse(responseCode = "500", description = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public void getAipReducedByRegex(
            @Parameter(name = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @Parameter(name = "Set of wanted files paths sent as RequestBody", required = true) @RequestBody @Valid DataReduction dataReduction,
            @Parameter(name = "true to return all XMLs, otherwise only the latest is returned") @RequestParam(value = "all", defaultValue = "false") boolean all,
            HttpServletResponse response)
            throws IOException, RollbackStateException, DeletedStateException, StillProcessingStateException,
            FailedStateException, BadRequestException,
            NoLogicalStorageReachableException, NoLogicalStorageAttachedException, ObjectCouldNotBeRetrievedException, RemovedStateException {

        BiFunction<AipRetrievalResource, ZipOutputStream, Void> fn = (aipRetrievalResource, outputStream) -> {
            try {
                Path aipDataInTmpDir = tmpFolder.resolve(aipRetrievalResource.getId());
                aipService.exportAipReducedByRegexes(aipId, aipDataInTmpDir, outputStream, dataReduction);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return null;
        };

        exportAipData(aipId, all, response, fn);
    }

    @Operation(summary = "Return specified AIP XML")
    @RequestMapping(value = "/{aipId}/xml", method = RequestMethod.GET)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "AIP XML successfully returned"),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "all attached logical storages are currently unreachable"),
            @ApiResponse(responseCode = "500", description = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public void getXml(
            @Parameter(name = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @Parameter(name = "version number of XML, if not set the latest version is returned") @RequestParam(value = "v", defaultValue = "") Integer version,
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
            @Parameter(name = "SIP file", required = true) @RequestParam("sip") MultipartFile sip,
            @Parameter(name = "AIP XML file", required = true) @RequestParam("aipXml") MultipartFile aipXml,
            @Parameter(name = "value of the SIP checksum", required = true) @RequestParam("sipChecksumValue") String sipChecksumValue,
            @Parameter(name = "type of the SIP checksum", required = true) @RequestParam("sipChecksumType") ChecksumType sipChecksumType,
            @Parameter(name = "value of the AIP XML checksum", required = true) @RequestParam("aipXmlChecksumValue") String aipXmlChecksumValue,
            @Parameter(name = "type of the AIP XML checksum", required = true) @RequestParam("aipXmlChecksumType") ChecksumType aipXmlChecksumType,
            @Parameter(name = "UUID of the AIP, generated if not specifies") @RequestParam(value = "UUID", defaultValue = "") String id)
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
            @Parameter(name = "AIP id", required = true) @PathVariable("aipId") String aipId, @RequestParam("xml") MultipartFile xml,
            @Parameter(name = "AIP XML checksum value", required = true) @RequestParam("checksumValue") String checksumValue,
            @Parameter(name = "AIP XML checksum type", required = true) @RequestParam("checksumType") ChecksumType checksumType,
            @Parameter(name = "synchronous/asynchronous processing flag") @RequestParam(value = "sync", defaultValue = "false") boolean sync,
            @Parameter(name = "version number of the AIP XML, is automatically set to the lastVersion+1 if not specified")
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
            @Parameter(name = "object id", required = true) @PathVariable("objId") String objId) throws DeletedStateException, StillProcessingStateException,
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
            @Parameter(name = "object id", required = true) @PathVariable("objId") String objId) throws DeletedStateException, StillProcessingStateException,
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
            @Parameter(name = "object id", required = true) @PathVariable("objId") String objId) throws StillProcessingStateException, RollbackStateException,
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
            @Parameter(name = "object id", required = true) @PathVariable("objId") String objId) throws BadRequestException, SomeLogicalStoragesNotReachableException, NoLogicalStorageAttachedException {
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
            @Parameter(name = "object id", required = true) @PathVariable("objId") String objId) throws BadRequestException, SomeLogicalStoragesNotReachableException, NoLogicalStorageAttachedException, ForbiddenByConfigException, StorageException, StateException {
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
            @Parameter(name = "AIP id", required = true) @PathVariable("aipId") String aipId,
            @Parameter(name = "XML version", required = true) @PathVariable("xmlVersion") int xmlVersion) throws BadRequestException, SomeLogicalStoragesNotReachableException, NoLogicalStorageAttachedException, StateException, StorageException {
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
            @Parameter(name = "AIP id", required = true) @PathVariable("aipId") String aipId,
            @Parameter(name = "XML version", required = true) @PathVariable("xmlVersion") int xmlVersion) throws BadRequestException, SomeLogicalStoragesNotReachableException, NoLogicalStorageAttachedException, StateException, ForbiddenByConfigException, StorageException {
        if (!forgetFeatureAllowed) {
            throw new ForbiddenByConfigException("forget feature not allowed");
        }
        checkUUID(aipId);
        if (xmlVersion < 2) {
            throw new UnsupportedOperationException("Only XMLs created as metadata update (version 2 and higher) can be forgotten through this endpoint");
        }
        aipService.rollbackOrForgetXml(aipId, xmlVersion, true);
    }

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
            @Parameter(name = "AIP id", required = true) @PathVariable("aipId") String aipId,
            @Parameter(name = "id of the logical storage", required = true) @RequestParam(value = "storageId") String storageId)
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
    public ObjectState getAipState(@Parameter(name = "AIP id", required = true) @PathVariable("aipId") String aipId) {
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
            @Parameter(name = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @Parameter(name = "XML version", required = true) @PathVariable("xmlVersion") int xmlVersion) {
        return aipService.getXmlState(aipId, xmlVersion);
    }

    private void exportAipData(String aipId, boolean allXmls, HttpServletResponse response, BiFunction<AipRetrievalResource, ZipOutputStream, Void> aipDataExportFunction) throws BadRequestException, NoLogicalStorageAttachedException, ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, RollbackStateException, RemovedStateException, StillProcessingStateException, DeletedStateException, FailedStateException, IOException {
        checkUUID(aipId);

        AipRetrievalResource aipRetrievalResource = aipService.getAip(aipId, allXmls);
        response.setContentType("application/zip");
        response.setStatus(200);
        response.addHeader("Content-Disposition", "attachment; filename=aip_" + aipId + ".zip");

        try (ZipOutputStream zipOut = new ZipOutputStream(new BufferedOutputStream(response.getOutputStream()))) {
            zipOut.putNextEntry(new ZipEntry(aipId + ".zip"));

            aipDataExportFunction.apply(aipRetrievalResource, zipOut);

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

    @Inject
    public void setForgetFeatureAllowed(@Value("${arcstorage.optionalFeatures.forgetObject}") boolean forgetFeatureAllowed) {
        this.forgetFeatureAllowed = forgetFeatureAllowed;
    }
}