package cz.cas.lib.arcstorage.gateway.service;

import cz.cas.lib.arcstorage.domain.AipSip;
import cz.cas.lib.arcstorage.domain.AipState;
import cz.cas.lib.arcstorage.domain.AipXml;
import cz.cas.lib.arcstorage.domain.XmlState;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.gateway.exception.CantWriteException;
import cz.cas.lib.arcstorage.gateway.exception.DeletedException;
import cz.cas.lib.arcstorage.gateway.exception.RollbackedException;
import cz.cas.lib.arcstorage.gateway.exception.StillProcessingException;
import cz.cas.lib.arcstorage.gateway.storage.StorageService;
import cz.cas.lib.arcstorage.gateway.storage.exception.StorageException;
import cz.cas.lib.arcstorage.gateway.storage.shared.StorageUtils;
import cz.cas.lib.arcstorage.store.StorageConfigStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.util.Utils.asList;

@Service
@Slf4j
public class ArchivalService {

    private ArchivalAsyncService async;
    private ArchivalDbService archivalDbService;
    private StorageConfigStore storageConfigStore;

    /**
     * Retrieves reference to AIP.
     *
     * @param sipId
     * @param all   if true reference to SIP and all XMLs is returned otherwise reference to SIP and latest XML is retrieved
     * @return reference of AIP which contains id and stream of SIP and XML/XMLs, if there are more XML to return those
     * which are rollbacked are skipped
     * @throws DeletedException         if SIP is deleted
     * @throws RollbackedException      if SIP is rollbacked or only one XML is requested and that one is rollbacked
     * @throws StillProcessingException if SIP or some of requested XML is still processing
     */
    public AipRef get(String sipId, Optional<Boolean> all) throws RollbackedException, StillProcessingException, DeletedException, StorageException {
        AipSip sipEntity = archivalDbService.getAip(sipId);

        if (sipEntity.getState() == AipState.DELETED)
            throw new DeletedException(sipEntity);
        if (sipEntity.getState() == AipState.ROLLBACKED)
            throw new RollbackedException(sipEntity);
        if (sipEntity.getState() == AipState.PROCESSING)
            throw new StillProcessingException(sipEntity);

        List<AipXml> xmls = all.isPresent() && all.get() ? sipEntity.getXmls() : asList(sipEntity.getLatestXml());
        Optional<AipXml> unfinishedXml = xmls.stream().filter(xml -> xml.getState() == XmlState.PROCESSING).findFirst();
        if (unfinishedXml.isPresent())
            throw new StillProcessingException(unfinishedXml.get());
        if (xmls.size() == 1 && xmls.get(0).getState() == XmlState.ROLLBACKED)
            throw new RollbackedException(xmls.get(0));

        xmls = xmls.stream().filter(xml -> xml.getState() != XmlState.ROLLBACKED).collect(Collectors.toList());
        List<InputStream> refs;
        try {
            StorageService storageService = StorageUtils.createAdapter(storageConfigStore.getByPriority());
            refs = storageService.getAip(sipId, xmls.stream().map(AipXml::getVersion).collect(Collectors.toList()).toArray(new Integer[xmls.size()]));
        } catch (StorageException e) {
            log.error("Storage error has occurred during retrieval process of AIP: " + sipId);
            throw e;
        }
        AipRef aip = new AipRef();
        aip.setSip(new FileRef(sipEntity.getId(), refs.get(0), sipEntity.getChecksum()));
        AipXml xml;
        for (int i = 1; i < refs.size(); i++) {
            xml = xmls.get(i - 1);
            aip.addXml(new XmlFileRef(xml.getId(), refs.get(i), xml.getChecksum(), xml.getVersion()));
        }
        log.info("AIP: " + sipId + " has been successfully retrieved.");
        return aip;
    }

    /**
     * Retrieves AIP XML reference.
     *
     * @param sipId
     * @param version specifies version of XML to return, by default the latest XML is returned
     * @return reference to AIP XML
     * @throws DeletedException
     * @throws RollbackedException
     * @throws StillProcessingException
     */
    public XmlFileRef getXml(String sipId, Optional<Integer> version) throws RollbackedException, StillProcessingException, StorageException {
        AipSip sipEntity = archivalDbService.getAip(sipId);
        AipXml requestedXml;
        if (version.isPresent()) {
            Optional<AipXml> xmlOpt = sipEntity.getXmls().stream().filter(xml -> xml.getVersion() == version.get()).findFirst();
            if (!xmlOpt.isPresent()) {
                log.warn("Could not find XML version: " + version.get() + " of AIP: " + sipId);
                throw new MissingObject(AipXml.class, sipId + " version: " + version.get());
            }
            requestedXml = xmlOpt.get();
        } else
            requestedXml = sipEntity.getLatestXml();
        if (requestedXml.getState() == XmlState.ROLLBACKED)
            throw new RollbackedException(requestedXml);
        if (requestedXml.getState() == XmlState.PROCESSING)
            throw new StillProcessingException(requestedXml);
        InputStream xmlStream;
        try {
            StorageService storageService = StorageUtils.createAdapter(storageConfigStore.getByPriority());
            xmlStream = storageService.getXml(sipId, requestedXml.getVersion());
        } catch (StorageException e) {
            log.error("Storage error has occurred during retrieval process of XML version: " + requestedXml.getVersion() + " of AIP: " + sipId);
            throw e;
        }
        log.info("XML version: " + requestedXml.getVersion() + " of AIP: " + sipId + " has been successfully retrieved.");
        return new XmlFileRef(requestedXml.getId(), xmlStream, requestedXml.getChecksum(), requestedXml.getVersion());
    }


    /**
     * Stores AIP parts (SIP and ARCLib XML) into Archival Storage.
     * <p>
     * Verifies that data are consistent after transfer and if not storage and database are cleared.
     * </p>
     * <p>
     * Also handles AIP versioning when whole AIP is versioned.
     * </p>
     *
     * @return SIP ID of created AIP
     * @throws IOException
     */
    public void store(AipRef aip) throws CantWriteException {
        String xmlId = archivalDbService.registerAipCreation(aip.getSip().getId(), aip.getSip().getChecksum(), aip.getXml().getChecksum());
        aip.getXml().setId(xmlId);
        async.store(aip);
    }

    /**
     * Stores ARCLib AIP XML into Archival Storage.
     * <p>
     * If MD5 hash of file after upload does not match MD5 hash provided in request, the database is cleared and exception is thrown.
     *
     * @param sipId    Id of SIP to which XML belongs
     * @param xml      Stream of xml file
     * @param checksum
     */
    public void updateXml(String sipId, InputStream xml, Checksum checksum) {
        AipXml xmlEntity = archivalDbService.registerXmlUpdate(sipId, checksum);
        async.updateXml(sipId, new XmlFileRef(xmlEntity.getId(), xml, checksum, xmlEntity.getVersion()));
    }

    /**
     * Physically removes SIP from database. XMLs and data in transaction database are not removed.
     *
     * @param sipId
     * @throws IOException
     * @throws RollbackedException
     * @throws StillProcessingException
     */
    public void delete(String sipId) throws StillProcessingException, RollbackedException, StorageException {
        archivalDbService.registerSipDeletion(sipId);
        this.async.delete(sipId);
    }

    /**
     * Logically removes SIP from database.
     *
     * @param sipId
     * @throws IOException
     * @throws DeletedException
     * @throws RollbackedException
     * @throws StillProcessingException
     */
    public void remove(String sipId) throws StillProcessingException, DeletedException, RollbackedException, StorageException {
        archivalDbService.removeSip(sipId);
        async.remove(sipId);
    }

    /**
     * Retrieves information about AIP.
     *
     * @param sipId
     * @throws IOException
     */
    public List<AipStateInfo> getAipState(String sipId) throws StillProcessingException, StorageException {
        AipSip aip = archivalDbService.getAip(sipId);
        if (aip.getState() == AipState.PROCESSING)
            throw new StillProcessingException(aip);
        List<AipStateInfo> aipStateInfos = new ArrayList<>();
        for (StorageService storageService : storageConfigStore.findAll().stream().map(StorageUtils::createAdapter).collect(Collectors.toList())) {
            aipStateInfos.add(storageService.getAipInfo(sipId, aip.getChecksum(), aip.getState(), aip.getXmls().stream().collect(Collectors.toMap(xml -> xml.getVersion(), xml -> xml.getChecksum()))));
        }
        log.info(String.format("Info about AIP: %s has been successfully retrieved.", sipId));
        return aipStateInfos;
    }

    /**
     * Returns state of currently used storage.
     *
     * @return
     */
    public List<StorageState> getStorageState() {
        List<StorageState> storageStates = new ArrayList<>();
        storageConfigStore.findAll().stream().forEach(c -> {
            try {
                storageStates.add(StorageUtils.createAdapter(c).getStorageState());
            } catch (StorageException e) {
                throw new GeneralException(e);
            }
        });
        return storageStates;
    }

    /**
     * Deletes files which are in PROCESSING state from storage and database.
     *
     * @throws IOException
     */
    public void clearUnfinished() throws StorageException {
        int xmlCounter = 0;
        List<AipSip> unfinishedSips = new ArrayList<>();
        List<AipXml> unfinishedXmls = new ArrayList<>();
        archivalDbService.fillUnfinishedFilesLists(unfinishedSips, unfinishedXmls);
        for (StorageService storageService : storageConfigStore.findAll().stream().map(c -> StorageUtils.createAdapter(c)).collect(Collectors.toList())) {
            for (AipSip sip : unfinishedSips) {
                if (sip.getXmls().size() > 1)
                    log.warn("Found more than one XML of SIP package with id " + sip.getId() + " which was in PROCESSING state. SIP and its first XML will be rollbacked.");
                storageService.rollbackAip(sip.getId());
            }
            for (AipXml xml : unfinishedXmls)
                storageService.rollbackXml(xml.getSip().getId(), xml.getVersion());
        }
        archivalDbService.rollbackUnfinishedFilesRecords();
        log.info("Successfully rollbacked " + unfinishedSips.size() + " SIPs and " + xmlCounter + unfinishedXmls.size() + " XMLs");
    }


    @Inject
    public void setArchivalDbService(ArchivalDbService archivalDbService) {
        this.archivalDbService = archivalDbService;
    }

    @Inject
    public void setAsyncService(ArchivalAsyncService async) {
        this.async = async;
    }

    @Inject
    public void setStorageConfigStore(StorageConfigStore storageConfigStore) {
        this.storageConfigStore = storageConfigStore;
    }
}
