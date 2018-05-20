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
import java.util.Optional;
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

    /**
     * Retrieves specified AIP as ZIP package.
     *
     * @param aipId
     * @param all   if true all AIP XMLs are packed otherwise only the latest is packed
     * @throws IOException
     */

    @ApiOperation(value = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successful response")})
    @RequestMapping(value = "/{aipId}", method = RequestMethod.GET)
    public void get(
            @ApiParam(value = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @ApiParam(value = "true to return all XMLs, otherwise only the latest is returned", required = false) @RequestParam(value = "all", defaultValue = "false") boolean all,
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

    /**
     * Retrieves specified AIP XML file.
     *
     * @param aipId
     * @param version version of XML, if not specified the latest version is retrieved
     */
    @RequestMapping(value = "/{aipId}/xml", method = RequestMethod.GET)
    public void getXml(
            @ApiParam(value = "AIP ID", required = true) @PathVariable("aipId") String aipId,
            @ApiParam(value = "version number of XML, if not set the latest version is returned", required = false) @RequestParam(value = "v", defaultValue = "") Integer version,
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

    /**
     * Stores AIP parts (SIP and ARCLib XML) into Archival Storage.
     * <p>
     * Verifies that data are consistent after transfer and if not storage and database are cleared.
     * </p>
     * <p>
     * This endpoint also handles AIP versioning when whole AIP is versioned.
     * </p>
     *
     * @param sip                SIP part of AIP
     * @param aipXml             ARCLib XML part of AIP
     * @param sipChecksumType    type of SIP checksum
     * @param sipChecksumValue   value of SIP checksum
     * @param aipXmlChecksumType type of XML checksum
     * @param sipChecksumType    value of SIP checksum
     * @param id                 optional parameter, if not specified id is generated
     * @return SIP ID of created AIP
     * @throws IOException
     */
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    public String save(@RequestParam("sip") MultipartFile sip, @RequestParam("aipXml") MultipartFile aipXml,
                       @RequestParam("sipChecksumValue") String sipChecksumValue,
                       @RequestParam("sipChecksumType") ChecksumType sipChecksumType,
                       @RequestParam("aipXmlChecksumValue") String aipXmlChecksumValue,
                       @RequestParam("aipXmlChecksumType") ChecksumType aipXmlChecksumType,
                       @RequestParam(value = "UUID") Optional<String> id)
            throws IOException, SomeLogicalStoragesNotReachableException, BadRequestException, NoLogicalStorageAttachedException {
        String aipId = id.isPresent() ? id.get() : UUID.randomUUID().toString();

        Checksum sipChecksum = new Checksum(sipChecksumType, sipChecksumValue);
        checkChecksumFormat(sipChecksum);

        Checksum aipXmlChecksum = new Checksum(aipXmlChecksumType, aipXmlChecksumValue);
        checkChecksumFormat(aipXmlChecksum);

        AipDto aipDto = new AipDto(aipId, sip.getInputStream(), sipChecksum, aipXml.getInputStream(), aipXmlChecksum);
        archivalService.save(aipDto);
        return aipId;
    }

    /**
     * Stores ARCLib AIP XML into Archival Storage.
     * <p>
     * Verifies that data are consistent after transfer and if not storage and database are cleared.
     * </p>
     * <p>
     * This endpoint handles AIP versioning when AIP XML is versioned.
     * </p>
     *
     * @param aipId         Id of SIP to which XML belongs
     * @param xml           ARCLib XML
     * @param checksumValue XML checksum value
     * @param checksumType  XML checksum type
     */
    @RequestMapping(value = "/{aipId}/update", method = RequestMethod.POST)
    public void saveXml(@PathVariable("aipId") String aipId, @RequestParam("xml") MultipartFile xml,
                        @RequestParam("checksumValue") String checksumValue,
                        @RequestParam("checksumType") ChecksumType checksumType,
                        @RequestParam("version") Optional<Integer> version) throws IOException,
            SomeLogicalStoragesNotReachableException, BadRequestException, NoLogicalStorageAttachedException {
        checkUUID(aipId);
        Checksum checksum = new Checksum(checksumType, checksumValue);
        checkChecksumFormat(checksum);

        archivalService.saveXml(aipId, xml.getInputStream(), checksum, version);
    }

    /**
     * Logically removes AIP package by setting its state to {@link ObjectState#REMOVED}
     * <p>Removed package can is still retrieved when {@link AipApi#get} method is called.</p>
     *
     * @param aipId
     * @throws IOException
     * @throws DeletedStateException
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     */
    @RequestMapping(value = "/{aipId}/remove", method = RequestMethod.PUT)
    public void remove(@PathVariable("aipId") String aipId) throws DeletedStateException, StillProcessingStateException,
            RollbackStateException, StorageException, FailedStateException, SomeLogicalStoragesNotReachableException,
            BadRequestException, NoLogicalStorageAttachedException {
        checkUUID(aipId);
        archivalService.remove(aipId);
    }

    /**
     * Renews logically removed SIP.
     *
     * @param aipId
     * @throws DeletedStateException
     * @throws StillProcessingStateException
     * @throws RollbackStateException
     * @throws StorageException
     * @throws FailedStateException
     * @throws SomeLogicalStoragesNotReachableException
     * @throws BadRequestException
     */
    @RequestMapping(value = "/{aipId}/renew", method = RequestMethod.PUT)
    public void renew(@PathVariable("aipId") String aipId) throws DeletedStateException, StillProcessingStateException,
            RollbackStateException, StorageException, FailedStateException, SomeLogicalStoragesNotReachableException,
            BadRequestException, NoLogicalStorageAttachedException {
        checkUUID(aipId);
        archivalService.renew(aipId);
    }

    /**
     * Physically removes SIP part of AIP package and sets its state to
     * {@link ObjectState#DELETED}. XMLs and data in transaction database are not removed.
     * <p>Deleted package is no longer retrieved when {@link AipApi#get} method is called.</p>
     *
     * @param aipId
     * @throws IOException
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     */
    @RequestMapping(value = "/{aipId}", method = RequestMethod.DELETE)
    public void delete(@PathVariable("aipId") String aipId) throws StillProcessingStateException, RollbackStateException,
            StorageException, FailedStateException, SomeLogicalStoragesNotReachableException, BadRequestException, NoLogicalStorageAttachedException {
        checkUUID(aipId);
        archivalService.delete(aipId);
    }

    /**
     * Retrieves information about AIP containing state, whether is consistent etc...
     *
     * @param aipId
     * @throws StillProcessingStateException
     * @throws StorageException
     */
    //@RequestMapping(value = "/{uuid}/state", method = RequestMethod.GET)
    public List<AipStateInfoDto> getAipState(@PathVariable("uuid") String aipId) throws StillProcessingStateException,
            StorageException, BadRequestException, NoLogicalStorageAttachedException {
        checkUUID(aipId);
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
}
