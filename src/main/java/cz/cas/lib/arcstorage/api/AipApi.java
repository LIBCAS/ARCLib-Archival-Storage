package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.gateway.exception.DeletedException;
import cz.cas.lib.arcstorage.gateway.exception.RollbackedException;
import cz.cas.lib.arcstorage.gateway.exception.StillProcessingException;
import cz.cas.lib.arcstorage.gateway.service.ArchivalService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import org.apache.commons.io.IOUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static cz.cas.lib.arcstorage.util.Utils.checkChecksum;
import static cz.cas.lib.arcstorage.util.Utils.checkUUID;

@RestController
@RequestMapping("/api/storage")
public class AipApi {

    private ArchivalService archivalService;

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
            throws IOException, RollbackedException, DeletedException, StorageException, StillProcessingException {
        checkUUID(sipId);

        AipRef aip = archivalService.get(sipId, all);
        response.setContentType("application/zip");
        response.setStatus(200);
        response.addHeader("Content-Disposition", "attachment; filename=aip_" + aip.getSip().getId());

        try (ZipOutputStream zipOut = new ZipOutputStream(new BufferedOutputStream(response.getOutputStream()))) {
            zipOut.putNextEntry(new ZipEntry(aip.getSip().getId()));
            IOUtils.copyLarge(new BufferedInputStream(aip.getSip().getInputStream()), zipOut);
            zipOut.closeEntry();
            aip.getSip().freeSources();
            for (XmlRef xml : aip.getXmls()) {
                zipOut.putNextEntry(new ZipEntry(String.format("%s_xml_%d", sipId, xml.getVersion())));
                IOUtils.copyLarge(new BufferedInputStream(xml.getInputStream()), zipOut);
                zipOut.closeEntry();
                xml.freeSources();
            }
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
            Optional<Integer> version, HttpServletResponse response) throws StorageException, StillProcessingException,
            RollbackedException, IOException {
        checkUUID(sipId);
        XmlRef xml = archivalService.getXml(sipId, version);
        response.setContentType("application/xml");
        response.setStatus(200);
        response.addHeader("Content-Disposition", "attachment; filename=" + sipId + "_xml_" + xml.getVersion());
        IOUtils.copyLarge(xml.getInputStream(), response.getOutputStream());
        xml.freeSources();
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
     * @param sip            SIP part of AIP
     * @param aipXml         ARCLib XML part of AIP
     * @param sipChecksum    SIP checksum
     * @param aipXmlChecksum XML checksum
     * @param id             optional parameter, if not specified id is generated
     * @return SIP ID of created AIP
     * @throws IOException
     */
    @RequestMapping(value = "/store", method = RequestMethod.POST)
    public String save(@RequestParam("sip") MultipartFile sip, @RequestParam("aipXml") MultipartFile aipXml,
                       @RequestParam("sipChecksum") Checksum sipChecksum, @RequestParam("aipXmlChecksum") Checksum aipXmlChecksum,
                       @RequestParam(value = "UUID") Optional<String> id) throws IOException {
        String sipId = id.isPresent() ? id.get() : UUID.randomUUID().toString();

        checkChecksum(sipChecksum);
        checkChecksum(aipXmlChecksum);

        AipRef aipRef = new AipRef(sipId, sip.getInputStream(), sipChecksum, aipXml.getInputStream(), aipXmlChecksum);
        archivalService.store(aipRef);
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
     * @param sipId  Id of SIP to which XML belongs
     * @param xml    ARCLib XML
     * @param checksum XML checksum
     */
    @RequestMapping(value = "/{sipId}/update", method = RequestMethod.POST)
    public void updateXml(@PathVariable("sipId") String sipId, @RequestParam("xml") MultipartFile xml,
                          @RequestParam("xmlChecksum") Checksum checksum) throws IOException {
        checkUUID(sipId);
        checkChecksum(checksum);

        archivalService.updateXml(sipId, xml.getInputStream(), checksum);
    }

    /**
     * Logically removes AIP package by setting its state to {@link cz.cas.lib.arcstorage.domain.AipState#REMOVED}
     * <p>Removed package can is still retrieved when {@link AipApi#get} method is called.</p>
     *
     * @param sipId
     * @throws IOException
     * @throws DeletedException
     * @throws RollbackedException
     * @throws StillProcessingException
     */
    @RequestMapping(value = "/{sipId}", method = RequestMethod.DELETE)
    public void remove(@PathVariable("sipId") String sipId) throws DeletedException, StillProcessingException,
            RollbackedException, StorageException {
        checkUUID(sipId);
        archivalService.remove(sipId);
    }

    /**
     * Physically removes SIP part of AIP package and sets its state to
     * {@link cz.cas.lib.arcstorage.domain.AipState#DELETED}. XMLs and data in transaction database are not removed.
     * <p>Deleted package is no longer retrieved when {@link AipApi#get} method is called.</p>
     *
     * @param sipId
     * @throws IOException
     * @throws RollbackedException
     * @throws StillProcessingException
     */
    @RequestMapping(value = "/{sipId}/hard", method = RequestMethod.DELETE)
    public void delete(@PathVariable("sipId") String sipId) throws StillProcessingException, RollbackedException,
            StorageException {
        checkUUID(sipId);
        archivalService.delete(sipId);
    }

    /**
     * Retrieves information about AIP containing state, id, XMLs ...
     *
     * @param sipId
     * @throws StillProcessingException
     * @throws StorageException
     */
    @RequestMapping(value = "/{uuid}/state", method = RequestMethod.GET)
    public List<AipStateInfo> getAipState(@PathVariable("uuid") String sipId) throws StillProcessingException,
            StorageException {
        checkUUID(sipId);
        return archivalService.getAipState(sipId);
    }

    /**
     * Retrieves state of Archival Storage.
     *
     * @return
     */
    @RequestMapping(value = "/state", method = RequestMethod.GET)
    public List<StorageState> getStorageState() {
        return archivalService.getStorageState();
    }

    @Inject
    public void setArchivalService(ArchivalService archivalService) {
        this.archivalService = archivalService;
    }
}
