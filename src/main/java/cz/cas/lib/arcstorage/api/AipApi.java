package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.BadRequestException;
import cz.cas.lib.arcstorage.service.ArchivalService;
import cz.cas.lib.arcstorage.service.exception.FileCorruptedAtAllStoragesException;
import cz.cas.lib.arcstorage.service.exception.InvalidChecksumException;
import cz.cas.lib.arcstorage.service.exception.StorageNotReachableException;
import cz.cas.lib.arcstorage.service.exception.state.DeletedStateException;
import cz.cas.lib.arcstorage.service.exception.state.FailedStateException;
import cz.cas.lib.arcstorage.service.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.service.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
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
     * @param sipId
     * @param all   if true all AIP XMLs are packed otherwise only the latest is packed
     * @throws IOException
     */
    @RequestMapping(value = "/{sipId}", method = RequestMethod.GET)
    public void get(@PathVariable("sipId") String sipId,
                    @RequestParam(value = "all") Optional<Boolean> all, HttpServletResponse response)
            throws IOException, RollbackStateException, DeletedStateException, StorageException, StillProcessingStateException, FailedStateException, FileCorruptedAtAllStoragesException, BadRequestException {
        checkUUID(sipId);

        AipRetrievalResource aipRetrievalResource = archivalService.get(sipId, all);
        response.setContentType("application/zip");
        response.setStatus(200);
        response.addHeader("Content-Disposition", "attachment; filename=aip_" + sipId);

        try (ZipOutputStream zipOut = new ZipOutputStream(new BufferedOutputStream(response.getOutputStream()));
             InputStream sipIs = new BufferedInputStream(aipRetrievalResource.getSip())) {
            zipOut.putNextEntry(new ZipEntry(sipId));
            IOUtils.copyLarge(sipIs, zipOut);
            zipOut.closeEntry();
            for (Integer xmlVersion : aipRetrievalResource.getXmls().keySet()) {
                zipOut.putNextEntry(new ZipEntry(toXmlId(sipId, xmlVersion)));
                IOUtils.copyLarge(new BufferedInputStream(aipRetrievalResource.getXmls().get(xmlVersion)), zipOut);
                zipOut.closeEntry();
            }
        } finally {
            tmpFolder.resolve(aipRetrievalResource.getId()).toFile().delete();
        }
    }

    /**
     * Retrieves specified AIP XML file.
     *
     * @param sipId
     * @param version version of XML, if not specified the latest version is retrieved
     */
    @RequestMapping(value = "/xml/{sipId}", method = RequestMethod.GET)
    public void getXml(@PathVariable("sipId") String sipId, @RequestParam(value = "v")
            Optional<Integer> version, HttpServletResponse response) throws StorageException, StillProcessingStateException,
            RollbackStateException, IOException, FailedStateException, FileCorruptedAtAllStoragesException, BadRequestException {
        checkUUID(sipId);
        ArchivalObjectDto xml = archivalService.getXml(sipId, version);
        response.setContentType("application/xml");
        response.setStatus(200);
        response.addHeader("Content-Disposition", "attachment; filename=" + xml.getStorageId());
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
                       @RequestParam("sipChecksumValue") String sipChecksumValue, @RequestParam("sipChecksumType") ChecksumType sipChecksumType,
                       @RequestParam("aipXmlChecksumValue") String aipXmlChecksumValue,
                       @RequestParam("aipXmlChecksumType") ChecksumType aipXmlChecksumType, @RequestParam(value = "UUID") Optional<String> id)
            throws IOException, InvalidChecksumException, StorageNotReachableException, BadRequestException {
        String sipId = id.isPresent() ? id.get() : UUID.randomUUID().toString();

        Checksum sipChecksum = new Checksum(sipChecksumType, sipChecksumValue);
        checkChecksumFormat(sipChecksum);

        Checksum aipXmlChecksum = new Checksum(aipXmlChecksumType, aipXmlChecksumValue);
        checkChecksumFormat(aipXmlChecksum);

        AipDto aipDto = new AipDto(sipId, sip.getInputStream(), sipChecksum, aipXml.getInputStream(), aipXmlChecksum);
        archivalService.save(aipDto);
        return sipId;
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
     * @param sipId         Id of SIP to which XML belongs
     * @param xml           ARCLib XML
     * @param checksumValue XML checksum value
     * @param checksumType  XML checksum type
     */
    @RequestMapping(value = "/{sipId}/update", method = RequestMethod.POST)
    public void saveXml(@PathVariable("sipId") String sipId, @RequestParam("xml") MultipartFile xml,
                        @RequestParam("checksumValue") String checksumValue,
                        @RequestParam("checksumType") ChecksumType checksumType,
                        @RequestParam("version") Optional<Integer> version) throws IOException, StorageNotReachableException, BadRequestException, InvalidChecksumException {
        checkUUID(sipId);
        Checksum checksum = new Checksum(checksumType, checksumValue);
        checkChecksumFormat(checksum);

        archivalService.saveXml(sipId, xml.getInputStream(), checksum, version);
    }

    /**
     * Logically removes AIP package by setting its state to {@link ObjectState#REMOVED}
     * <p>Removed package can is still retrieved when {@link AipApi#get} method is called.</p>
     *
     * @param sipId
     * @throws IOException
     * @throws DeletedStateException
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     */
    @RequestMapping(value = "/{sipId}", method = RequestMethod.DELETE)
    public void remove(@PathVariable("sipId") String sipId) throws DeletedStateException, StillProcessingStateException,
            RollbackStateException, StorageException, FailedStateException, StorageNotReachableException, BadRequestException {
        checkUUID(sipId);
        archivalService.remove(sipId);
    }

    /**
     * Physically removes SIP part of AIP package and sets its state to
     * {@link ObjectState#DELETED}. XMLs and data in transaction database are not removed.
     * <p>Deleted package is no longer retrieved when {@link AipApi#get} method is called.</p>
     *
     * @param sipId
     * @throws IOException
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     */
    @RequestMapping(value = "/{sipId}/hard", method = RequestMethod.DELETE)
    public void delete(@PathVariable("sipId") String sipId) throws StillProcessingStateException, RollbackStateException,
            StorageException, FailedStateException, StorageNotReachableException, BadRequestException {
        checkUUID(sipId);
        archivalService.delete(sipId);
    }

    /**
     * Retrieves information about AIP containing state, whether is consistent etc...
     *
     * @param sipId
     * @throws StillProcessingStateException
     * @throws StorageException
     */
    @RequestMapping(value = "/{uuid}/state", method = RequestMethod.GET)
    public List<AipStateInfoDto> getAipState(@PathVariable("uuid") String sipId) throws StillProcessingStateException,
            StorageException, BadRequestException {
        checkUUID(sipId);
        return archivalService.getAipState(sipId);
    }

    /**
     * Retrieves state of Archival Storage.
     *
     * @return
     */
    @RequestMapping(value = "/state", method = RequestMethod.GET)
    public List<StorageStateDto> getStorageState() {
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
