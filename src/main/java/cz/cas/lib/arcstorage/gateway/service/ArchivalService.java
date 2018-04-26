package cz.cas.lib.arcstorage.gateway.service;

import cz.cas.lib.arcstorage.domain.AipSip;
import cz.cas.lib.arcstorage.domain.AipXml;
import cz.cas.lib.arcstorage.domain.ObjectState;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.gateway.exception.CantWriteException;
import cz.cas.lib.arcstorage.gateway.exception.InvalidChecksumException;
import cz.cas.lib.arcstorage.gateway.exception.state.DeletedStateException;
import cz.cas.lib.arcstorage.gateway.exception.state.FailedStateException;
import cz.cas.lib.arcstorage.gateway.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.gateway.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.StorageUtils;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.store.StorageConfigStore;
import cz.cas.lib.arcstorage.store.Transactional;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.util.Utils.asList;
import static java.util.Collections.shuffle;

@Service
@Slf4j
public class ArchivalService {

    private ArchivalAsyncService async;
    private ArchivalDbService archivalDbService;
    private StorageConfigStore storageConfigStore;
    private StorageProvider storageProvider;

    /**
     * Retrieves reference to AIP.
     *
     * @param sipId
     * @param all   if true reference to SIP and all XMLs is returned otherwise reference to SIP and latest XML is retrieved
     * @return reference of AIP which contains id and inputStream of SIP and XML/XMLs, if there are more XML to return those
     * which are rollbacked are skipped
     * @throws DeletedStateException         if SIP is deleted
     * @throws RollbackStateException        if SIP is rollbacked or only one XML is requested and that one is rollbacked
     * @throws StillProcessingStateException if SIP or some of requested XML is still processing
     * @throws StorageException              if error has occurred during retrieval process of AIP
     * @throws FailedStateException          if SIP is failed
     */
    public AipDto get(String sipId, Optional<Boolean> all) throws RollbackStateException, StillProcessingStateException,
            DeletedStateException, FailedStateException, InvalidChecksumException {
        AipSip sipEntity = archivalDbService.getAip(sipId);

        switch (sipEntity.getState()) {
            case PROCESSING:
                throw new StillProcessingStateException(sipEntity);
            case FAILED:
                throw new FailedStateException(sipEntity);
            case DELETED:
                throw new DeletedStateException(sipEntity);
            case ROLLBACKED:
                throw new RollbackStateException(sipEntity);
        }

        List<AipXml> xmls = all.isPresent() && all.get() ? sipEntity.getXmls() : asList(sipEntity.getLatestXml());

        Optional<AipXml> unfinishedXml = xmls.stream()
                .filter(xml -> xml.getState() == ObjectState.PROCESSING)
                .findFirst();

        if (unfinishedXml.isPresent())
            throw new StillProcessingStateException(unfinishedXml.get());
        if (xmls.size() == 1 && xmls.get(0).getState() == ObjectState.ROLLBACKED)
            throw new RollbackStateException(xmls.get(0));

        xmls = xmls.stream()
                .filter(xml -> xml.getState() != ObjectState.ROLLBACKED)
                .collect(Collectors.toList());

        List<FileContentDto> refs = retrieveAip(sipEntity, xmls);

        AipDto aip = new AipDto();
        aip.setSip(new ArchivalObjectDto(sipEntity.getId(), refs.get(0), sipEntity.getChecksum()));
        AipXml xml;
        for (int i = 1; i < refs.size(); i++) {
            xml = xmls.get(i - 1);
            aip.addXml(new XmlDto(xml.getId(), refs.get(i), xml.getChecksum(), xml.getVersion()));
        }
        return aip;
    }

    /**
     * Retrieves AIP XML reference.
     *
     * @param sipId
     * @param version specifies version of XML to return, by default the latest XML is returned
     * @return reference to AIP XML
     * @throws InvalidChecksumException
     * @throws FailedStateException
     * @throws RollbackStateException
     * @throws StillProcessingStateException {
     */
    public XmlDto getXml(String sipId, Optional<Integer> version) throws
            InvalidChecksumException, FailedStateException, RollbackStateException, StillProcessingStateException {
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
        switch (requestedXml.getState()) {
            case ROLLBACKED:
                throw new RollbackStateException(requestedXml);
            case FAILED:
                throw new FailedStateException(requestedXml);
            case PROCESSING:
                throw new StillProcessingStateException(requestedXml);
        }

        FileContentDto xmlRef;
        try {
            xmlRef = retrieveXml(requestedXml);
        } catch (InvalidChecksumException e) {
            log.error("Storage error has occurred during retrieval process of XML version: " +
                    requestedXml.getVersion() + " of AIP: " + sipId);
            throw e;
        }
        log.info("XML version: " + requestedXml.getVersion() + " of AIP: " + sipId + " has been successfully retrieved.");
        return new XmlDto(requestedXml.getId(), xmlRef, requestedXml.getChecksum(), requestedXml.getVersion());
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
     * @throws CantWriteException
     */
    @Transactional
    public void store(AipDto aip) throws CantWriteException {
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
        async.updateXml(sipId, new XmlDto(xmlEntity.getId(), new FileContentDto(xml), checksum, xmlEntity.getVersion()));
    }

    /**
     * Physically removes SIP from storage. XMLs and data in transaction database are not removed.
     *
     * @param sipId
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     * @throws StorageException
     * @throws FailedStateException
     */
    public void delete(String sipId) throws StillProcessingStateException, RollbackStateException, StorageException, FailedStateException {
        archivalDbService.registerSipDeletion(sipId);
        this.async.delete(sipId);
    }

    /**
     * Logically removes SIP from database.
     *
     * @param sipId
     * @throws StorageException
     * @throws DeletedStateException
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     * @throws FailedStateException
     */
    public void remove(String sipId) throws StillProcessingStateException, DeletedStateException, RollbackStateException, StorageException, FailedStateException {
        archivalDbService.removeSip(sipId);
        async.remove(sipId);
    }

    /**
     * Retrieves information about AIP.
     *
     * @param sipId
     * @throws StillProcessingStateException
     * @throws StorageService
     */
    public List<AipStateInfoDto> getAipState(String sipId) throws StillProcessingStateException, StorageException {
        AipSip aip = archivalDbService.getAip(sipId);
        if (aip.getState() == ObjectState.PROCESSING)
            throw new StillProcessingStateException(aip);
        List<AipStateInfoDto> aipStateInfoDtos = new ArrayList<>();
        for (StorageService storageService : storageConfigStore.findAll().stream().map(storageProvider::createAdapter).collect(Collectors.toList())) {
            aipStateInfoDtos.add(storageService.getAipInfo(sipId, aip.getChecksum(), aip.getState(), aip.getXmls().stream().collect(Collectors.toMap(xml -> xml.getVersion(), xml -> xml.getChecksum()))));
        }
        log.info(String.format("Info about AIP: %s has been successfully retrieved.", sipId));
        return aipStateInfoDtos;
    }

    /**
     * Returns state of currently used storage.
     *
     * @return
     */
    public List<StorageStateDto> getStorageState() {
        List<StorageStateDto> storageStateDtos = new ArrayList<>();
        storageConfigStore.findAll().stream().forEach(c -> {
            try {
                storageStateDtos.add(storageProvider.createAdapter(c).getStorageState());
            } catch (StorageException e) {
                throw new GeneralException(e);
            }
        });
        return storageStateDtos;
    }

    /**
     * Rollback files which are in PROCESSING state.
     * This will be used only after system crash.
     *
     * @throws StorageException
     */
    public void clearUnfinished() throws StorageException {
        int xmlCounter = 0;
        List<AipSip> unfinishedSips = new ArrayList<>();
        List<AipXml> unfinishedXmls = new ArrayList<>();
        archivalDbService.fillUnfinishedFilesLists(unfinishedSips, unfinishedXmls);
        for (StorageService storageService : storageConfigStore.findAll().stream().map(c -> storageProvider.createAdapter(c)).collect(Collectors.toList())) {
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

    private List<FileContentDto> retrieveAip(AipSip sipEntity, List<AipXml> xmls) throws InvalidChecksumException {
        TreeMap<Integer, List<StorageService>> storageServicesByPriorities = getStorageServicesByPriorities();
        List<StorageService> highestPriorityStorageConfigs = storageServicesByPriorities.pollFirstEntry().getValue();

        //shuffle to ensure randomness in selection of the storage config
        shuffle(highestPriorityStorageConfigs);
        StorageService storageServiceChosen = highestPriorityStorageConfigs.get(0);

        List<FileContentDto> fileRefs;
        try {
            Result result = retrieveAipFromStorage(sipEntity, xmls, storageServiceChosen);
            fileRefs = !result.invalidChecksumFound ? result.getFileContentDtos() :
                    recoverAipFromOtherStorages(sipEntity, xmls, storageServicesByPriorities, result);
        } catch (InvalidChecksumException e) {
            log.error("Cannot retrieve AIP " + sipEntity.getId() + " from neither of the storages because the checksums do not match.");
            throw e;
        } catch (StorageException e) {
            log.error("Storage error has occurred during retrieval process of AIP: " + sipEntity.getId());
            fileRefs = recoverAipFromOtherStorages(sipEntity, xmls, storageServicesByPriorities, null);
        }
        log.info("AIP: " + sipEntity.getId() + " has been successfully retrieved.");
        return fileRefs;
    }

    private FileContentDto retrieveXml(AipXml aipXml) throws InvalidChecksumException {
        TreeMap<Integer, List<StorageService>> storageServicesByPriorities = getStorageServicesByPriorities();
        List<StorageService> highestPriorityStorageConfigs = storageServicesByPriorities.pollFirstEntry().getValue();

        //shuffle to ensure randomness in selection of the storage config
        shuffle(highestPriorityStorageConfigs);
        StorageService storageServiceChosen = highestPriorityStorageConfigs.get(0);

        FileContentDto xmlRef;
        try {
            xmlRef = retrieveXmlFromStorage(aipXml, storageServiceChosen);
            if (xmlRef == null) {
                xmlRef = recoverXmlFromOtherStorages(aipXml, storageServicesByPriorities, storageServiceChosen);
            }
        } catch (InvalidChecksumException e) {
            log.error("Cannot retrieve XML " + aipXml.getId() + " form neither of the storages because the checksums do not match.");
            throw e;
        } catch (StorageException e) {
            log.error("Storage error has occurred during retrieval process of XML: " + aipXml.getId());
            xmlRef = recoverXmlFromOtherStorages(aipXml, storageServicesByPriorities, null);
        }
        log.info("XML: " + aipXml.getId() + " has been successfully retrieved.");
        return xmlRef;
    }

    private Result retrieveAipFromStorage(AipSip sipEntity, List<AipXml> xmls, StorageService storageService)
            throws StorageException {
        String storageName = storageService.getStorageConfig().getName();
        log.info("Storage: " + storageName + " chosen to retrieve AIP: " + sipEntity.getId());

        List<FileContentDto> refs = storageService.getAip(sipEntity.getId(), xmls.stream()
                .map(AipXml::getVersion)
                .collect(Collectors.toList())
                .toArray(new Integer[xmls.size()]));

        Result result = new Result(refs, storageService);

        //verification of the sip checksum
        FileContentDto sipFileContentDto = refs.get(0);
        Checksum sipComputedChecksum = StorageUtils.computeChecksum(sipFileContentDto.getInputStream(),
                sipEntity.getChecksum().getType());
        if (!sipEntity.getChecksum().equals(sipComputedChecksum)) {
            log.error("Checksum for AIP " + sipEntity.getId() + " is invalid at storage " + storageName);
            result.setInvalidChecksumSip(sipEntity);
            result.setInvalidChecksumFound(true);
        }

        //verification of the xmls checksums
        for (int i = 1; i < refs.size(); i++) {
            FileContentDto xmlFileContentDto = refs.get(i);
            AipXml xmlEntity = xmls.get(i - 1);

            Checksum xmlComputedChecksum = StorageUtils.computeChecksum(xmlFileContentDto.getInputStream(),
                    xmlEntity.getChecksum().getType());
            if (!xmlEntity.getChecksum().equals(xmlComputedChecksum)) {
                log.error("Checksum for XML " + xmlEntity.getId() + " is invalid at storage " + storageName);
                result.addInvalidChecksumXml(xmlEntity);
                result.setInvalidChecksumFound(true);
            }
        }
        return result;
    }

    private FileContentDto retrieveXmlFromStorage(AipXml xmlEntity, StorageService storageService)
            throws StorageException {
        String storageName = storageService.getStorageConfig().getName();
        log.info("Storage: " + storageName + " chosen to retrieve AIP: " + xmlEntity.getId());

        FileContentDto xmlFileContentDto = storageService.getXml(xmlEntity.getSip().getId(), xmlEntity.getVersion());

        //verification of the XML checksum
        Checksum xmlComputedChecksum = StorageUtils.computeChecksum(xmlFileContentDto.getInputStream(),
                xmlEntity.getChecksum().getType());
        if (!xmlEntity.getChecksum().equals(xmlComputedChecksum)) {
            log.error("Checksum for XML " + xmlEntity.getId() + " is invalid at storage " + storageName);
            return null;
        }
        return xmlFileContentDto;
    }

    public List<FileContentDto> recoverAipFromOtherStorages(AipSip sipEntity, List<AipXml> xmls,
                                                            TreeMap<Integer, List<StorageService>> sortedStorageServices,
                                                            Result latestInvalidChecksumResult) throws InvalidChecksumException {
        List<StorageService> storageServices = sortedStorageServices.values().stream()
                .map(storageServicesByPriority -> {
                    shuffle(storageServicesByPriority);
                    return storageServicesByPriority;
                })
                .flatMap(List::stream)
                .collect(Collectors.toList());

        List<Result> invalidChecksumResults = new ArrayList<>();

        if (latestInvalidChecksumResult != null) {
            storageServices.remove(latestInvalidChecksumResult.storageService);
            invalidChecksumResults.add(latestInvalidChecksumResult);
        }

        Result result = null;
        for (StorageService storageService : storageServices) {
            try {
                result = retrieveAipFromStorage(sipEntity, xmls, storageService);
                if (!result.invalidChecksumFound) break;
                invalidChecksumResults.add(result);
            } catch (StorageException e) {
                //try other storages when the current storage has failed
                log.error("Storage error has occurred during retrieval process of AIP: " + sipEntity.getId());
            }
        }
        if (result.invalidChecksumFound) throw new InvalidChecksumException(sipEntity);

        log.info("AIP " + sipEntity.getId() + " has been successfully retrieved.");

        for (Result invalidChecksumResult : invalidChecksumResults) {
            StorageService usedStorageService = invalidChecksumResult.storageService;

            //repair sip at the storage
            if (invalidChecksumResult.invalidChecksumSip != null) {
                FileContentDto aipFileContentDto = result.getFileContentDtos().get(0);
                ArchivalObjectDto sipRef = new ArchivalObjectDto(sipEntity.getId(), aipFileContentDto,
                        invalidChecksumResult.invalidChecksumSip.getChecksum());
                try {
                    usedStorageService.storeSip(sipRef, new AtomicBoolean(false));
                    log.info("SIP " + sipEntity.getId() + " has been successfully recovered at storage" +
                            usedStorageService.getStorageConfig().getName());
                } catch (StorageException e) {
                    log.error("SIP " + sipEntity.getId() + " has failed to be recovered at storage" +
                            usedStorageService.getStorageConfig().getName());
                }
            }

            //repair xmls at the storage
            for (AipXml invalidChecksumXml : invalidChecksumResult.invalidChecksumXmls) {
                FileContentDto xmlFileContentDto = result.getFileContentDtos().get(invalidChecksumXml.getVersion());
                XmlDto xmlRef = new XmlDto(xmlFileContentDto, invalidChecksumXml.getChecksum(), invalidChecksumXml.getVersion());
                try {
                    usedStorageService.storeXml(sipEntity.getId(), xmlRef, new AtomicBoolean(false));
                    log.info("XML: " + invalidChecksumXml.getId() + " has been successfully recovered at storage" +
                            usedStorageService.getStorageConfig().getName());
                } catch (StorageException e) {
                    log.info("XML: " + invalidChecksumXml.getId() + " has failed to be recovered at storage" +
                            usedStorageService.getStorageConfig().getName());
                }
            }
        }
        return result.getFileContentDtos();
    }

    public FileContentDto recoverXmlFromOtherStorages(AipXml xmlEntity, TreeMap<Integer, List<StorageService>> sortedStorageServices,
                                                      StorageService invalidChecksumStorage) throws InvalidChecksumException {
        List<StorageService> storageServices = sortedStorageServices.values().stream()
                .map(storageServicesByPriority -> {
                    shuffle(storageServicesByPriority);
                    return storageServicesByPriority;
                })
                .flatMap(List::stream)
                .collect(Collectors.toList());

        List<StorageService> invalidChecksumStorages = new ArrayList<>();

        if (invalidChecksumStorage != null) {
            storageServices.remove(invalidChecksumStorage);
            invalidChecksumStorages.add(invalidChecksumStorage);
        }

        FileContentDto xmlFileContentDto = null;
        for (StorageService storageService : storageServices) {
            try {
                xmlFileContentDto = retrieveXmlFromStorage(xmlEntity, storageService);
                if (xmlFileContentDto != null) break;
                invalidChecksumStorages.add(storageService);
            } catch (StorageException e) {
                //try other storages when the current storage has failed
                log.error("Storage error has occurred during retrieval process of AIP: " + xmlEntity.getId());
            }
        }
        if (xmlFileContentDto == null) throw new InvalidChecksumException(xmlEntity);

        log.info("XML " + xmlEntity.getId() + " has been successfully retrieved");

        for (StorageService storageService : invalidChecksumStorages) {
            //repair xml at the given storage
            XmlDto xmlRef = new XmlDto(xmlFileContentDto, xmlEntity.getChecksum(), xmlEntity.getVersion());
            try {
                storageService.storeXml(xmlEntity.getSip().getId(), xmlRef, new AtomicBoolean(false));
                log.info("XML: " + xmlEntity.getId() + " has been successfully recovered at storage" +
                        storageService.getStorageConfig().getName());
            } catch (StorageException e) {
                log.error("XML: " + xmlEntity.getId() + " has been failed to be recovered at storage" +
                        storageService.getStorageConfig().getName());
            }
        }
        return xmlFileContentDto;
    }

    private TreeMap<Integer, List<StorageService>> getStorageServicesByPriorities() {
        //sorted map of storage configs where the keys are the priorities and the values are the lists of configs
        TreeMap<Integer, List<StorageService>> storageServicesByPriorities = new TreeMap<>(Collections.reverseOrder());

        storageConfigStore.findAll().forEach(storageConfig -> {
            StorageService storageService = storageProvider.createAdapter(storageConfig);
            if (storageService.testConnection()) {
                List<StorageService> storageServices = storageServicesByPriorities.get(storageConfig.getPriority());
                if (storageServices == null) storageServices = new ArrayList<>();
                storageServices.add(storageService);
                storageServicesByPriorities.put(storageConfig.getPriority(), storageServices);
            }
        });
        return storageServicesByPriorities;
    }

    @Getter
    @Setter
    private class Result {
        private List<FileContentDto> fileContentDtos;
        private StorageService storageService;
        private boolean invalidChecksumFound = false;

        private AipSip invalidChecksumSip;
        private List<AipXml> invalidChecksumXmls = new ArrayList<>();


        public Result(List<FileContentDto> fileContentDtos, StorageService storageService) {
            this.fileContentDtos = fileContentDtos;
            this.storageService = storageService;
        }

        public void addInvalidChecksumXml(AipXml xml) {
            invalidChecksumXmls.add(xml);
        }
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

    @Inject
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }
}
