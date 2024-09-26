package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.dto.AipRetrievalResource;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.DataReduction;
import cz.cas.lib.arcstorage.dto.ObjectRetrievalResource;
import cz.cas.lib.arcstorage.exception.BadRequestException;
import cz.cas.lib.arcstorage.security.Roles;
import cz.cas.lib.arcstorage.service.AipService;
import cz.cas.lib.arcstorage.service.exception.state.*;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.ObjectCouldNotBeRetrievedException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static cz.cas.lib.arcstorage.util.Utils.checkUUID;

@Slf4j
@RestController
@RequestMapping("/api/storage")
public class ObjectReadApi {

    private AipService aipService;
    private Path tmpFolder;

    @Operation(summary = "Return specified AIP as a ZIP package")
    @RequestMapping(value = "/{aipId}", method = RequestMethod.GET)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "AIP successfully returned", content = @Content(mediaType = "application/zip", schema = @Schema(type = "string", format = "binary"))),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "all attached logical storages are currently unreachable"),
            @ApiResponse(responseCode = "500", description = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public void getAip(
            @Parameter(description = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @Parameter(description = "true to return all XMLs, otherwise only the latest is returned") @RequestParam(value = "all", defaultValue = "false") boolean all,
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
            @ApiResponse(responseCode = "200", description = "AIP successfully returned", content = @Content(mediaType = "application/zip", schema = @Schema(type = "string", format = "binary"))),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "all attached logical storages are currently unreachable"),
            @ApiResponse(responseCode = "500", description = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public void getAipFilesReducedByListOfPaths(
            @Parameter(description = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @Parameter(description = "Set of wanted files paths sent as RequestBody", required = true) @RequestBody Set<String> filePaths,
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
            @ApiResponse(responseCode = "200", description = "AIP successfully returned", content = @Content(mediaType = "application/zip", schema = @Schema(type = "string", format = "binary"))),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "all attached logical storages are currently unreachable"),
            @ApiResponse(responseCode = "500", description = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public void getAipFilesReducedByRegex(
            @Parameter(description = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @Parameter(description = "Set of wanted files paths sent as RequestBody", required = true) @RequestBody @Valid DataReduction dataReduction,
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
            @ApiResponse(responseCode = "200", description = "AIP successfully returned", content = @Content(mediaType = "application/zip", schema = @Schema(type = "string", format = "binary"))),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "all attached logical storages are currently unreachable"),
            @ApiResponse(responseCode = "500", description = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public void getAipReducedByListOfFiles(
            @Parameter(description = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @Parameter(description = "Set of wanted files paths sent as RequestBody", required = true) @RequestBody Set<String> filePaths,
            @Parameter(description = "true to return all XMLs, otherwise only the latest is returned") @RequestParam(value = "all", defaultValue = "false") boolean all,
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
            @ApiResponse(responseCode = "200", description = "AIP successfully returned", content = @Content(mediaType = "application/zip", schema = @Schema(type = "string", format = "binary"))),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "all attached logical storages are currently unreachable"),
            @ApiResponse(responseCode = "500", description = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public void getAipReducedByRegex(
            @Parameter(description = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @Parameter(description = "Set of wanted files paths sent as RequestBody", required = true) @RequestBody @Valid DataReduction dataReduction,
            @Parameter(description = "true to return all XMLs, otherwise only the latest is returned") @RequestParam(value = "all", defaultValue = "false") boolean all,
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
            @ApiResponse(responseCode = "200", description = "AIP XML successfully returned", content = @Content(mediaType = "application/xml", schema = @Schema(type = "string", format = "binary"))),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current AIP state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "all attached logical storages are currently unreachable"),
            @ApiResponse(responseCode = "500", description = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE})
    public void getXml(
            @Parameter(description = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @Parameter(description = "version number of XML, if not set the latest version is returned") @RequestParam(value = "v", defaultValue = "") Integer version,
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

    @Operation(summary = "Retrieves content of object.", description = "Supported only for AIP data and AIP XML objects. For non-admin users, the content is retrieved only if the object belongs to the users's dataspace.")
    @RequestMapping(value = "/object/{id}", method = RequestMethod.GET)
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "content successfully returned", content = @Content(mediaType = "*/*", schema = @Schema(type = "string", format = "binary"))),
            @ApiResponse(responseCode = "403", description = "operation forbidden with respect to the current object state"),
            @ApiResponse(responseCode = "400", description = "bad request, e.g. the specified id is not a valid UUID"),
            @ApiResponse(responseCode = "503", description = "all attached logical storages are currently unreachable"),
            @ApiResponse(responseCode = "500", description = "file is corrupted at all storages, no logical storage attached, or other internal server error")
    })
    @RolesAllowed({Roles.READ, Roles.READ_WRITE, Roles.ADMIN})
    public void getObject(
            @Parameter(description = "DB ID", required = true) @PathVariable("id") String id,
            HttpServletResponse response) throws StillProcessingStateException,
            RollbackStateException, IOException, FailedStateException, ObjectCouldNotBeRetrievedException,
            BadRequestException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        checkUUID(id);
        Pair<ArchivalObjectDto, ObjectRetrievalResource> retrievedObject = aipService.getObject(id);
        String suffix;
        switch (retrievedObject.getKey().getObjectType()) {
            default:
                //content type is not known
                throw new UnsupportedOperationException("operation not supported for generic objects");
            case XML:
                response.setContentType("application/xml");
                suffix = ".xml";
                break;
            case SIP:
                response.setContentType("application/zip");
                suffix = ".zip";
                break;
        }
        response.setStatus(200);
        response.addHeader("Content-Disposition", "attachment; filename=" + id + suffix);
        try (InputStream is = new BufferedInputStream(retrievedObject.getRight().getInputStream())) {
            IOUtils.copyLarge(is, response.getOutputStream());
        } finally {
            tmpFolder.resolve(retrievedObject.getRight().getId()).toFile().delete();
        }
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

    @Autowired
    public void setAipService(AipService aipService) {
        this.aipService = aipService;
    }

    @Autowired
    public void setTmpFolder(@Value("${spring.servlet.multipart.location}") String path) {
        this.tmpFolder = Paths.get(path);
    }
}