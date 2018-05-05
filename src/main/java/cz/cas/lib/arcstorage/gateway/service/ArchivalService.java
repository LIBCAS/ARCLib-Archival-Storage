package cz.cas.lib.arcstorage.gateway.service;

import cz.cas.lib.arcstorage.domain.AipSip;
import cz.cas.lib.arcstorage.domain.AipXml;
import cz.cas.lib.arcstorage.domain.ObjectState;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.gateway.exception.CantWriteException;
import cz.cas.lib.arcstorage.gateway.exception.FileCorruptedAtAllStoragesException;
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
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.storage.StorageUtils.copyStreamAndComputeChecksum;
import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static cz.cas.lib.arcstorage.util.Utils.asList;
import static java.util.Collections.shuffle;

@Service
@Slf4j
public class ArchivalService {

    private ArchivalAsyncService async;
    private ArchivalDbService archivalDbService;
    private StorageConfigStore storageConfigStore;
    private StorageProvider storageProvider;
    private Path tmpFolder;

    /**
     * Retrieves reference to AIP.
     *
     * @param sipId
     * @param all   if true reference to SIP and all XMLs is returned otherwise reference to SIP and latest XML is retrieved
     * @return reference of AIP which contains id and inputStream of SIP and XML/XMLs, if there are more XML to return those
     * which are rolled back are skipped
     * @throws DeletedStateException         if SIP is deleted
     * @throws RollbackStateException        if SIP is rolled back or only one XML is requested and that one is rolled back
     * @throws StillProcessingStateException if SIP or some of requested XML is still processing
     * @throws StorageException              if error has occurred during retrieval process of AIP
     * @throws FailedStateException          if SIP is failed
     */
    public AipRetrievalResource get(String sipId, Optional<Boolean> all) throws RollbackStateException, StillProcessingStateException,
            DeletedStateException, FailedStateException, FileCorruptedAtAllStoragesException {
        AipSip sipEntity = archivalDbService.getAip(sipId);

        switch (sipEntity.getState()) {
            case PROCESSING:
                throw new StillProcessingStateException(sipEntity);
            case FAILED:
                throw new FailedStateException(sipEntity);
            case DELETED:
                throw new DeletedStateException(sipEntity);
            case ROLLED_BACK:
                throw new RollbackStateException(sipEntity);
        }

        List<AipXml> xmls = all.isPresent() && all.get() ? sipEntity.getXmls() : asList(sipEntity.getLatestXml());

        Optional<AipXml> unfinishedXml = xmls.stream()
                .filter(xml -> xml.getState() == ObjectState.PROCESSING)
                .findFirst();

        if (unfinishedXml.isPresent())
            throw new StillProcessingStateException(unfinishedXml.get());
        if (xmls.size() == 1 && xmls.get(0).getState() == ObjectState.ROLLED_BACK)
            throw new RollbackStateException(xmls.get(0));

        xmls = xmls.stream()
                .filter(xml -> xml.getState() != ObjectState.ROLLED_BACK)
                .collect(Collectors.toList());

        return retrieveAip(sipEntity, xmls);
    }

    /**
     * Retrieves AIP XML reference.
     *
     * @param sipId
     * @param version specifies version of XML to return, by default the latest XML is returned
     * @return reference to AIP XML
     * @throws FailedStateException
     * @throws RollbackStateException
     * @throws FileCorruptedAtAllStoragesException
     * @throws StillProcessingStateException       {
     */
    public ArchivalObjectDto getXml(String sipId, Optional<Integer> version) throws
            FailedStateException, RollbackStateException, StillProcessingStateException, FileCorruptedAtAllStoragesException {
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
            case ROLLED_BACK:
                throw new RollbackStateException(requestedXml);
            case FAILED:
                throw new FailedStateException(requestedXml);
            case PROCESSING:
                throw new StillProcessingStateException(requestedXml);
        }

        InputStream xmlRef;
        try {
            xmlRef = retrieveXml(requestedXml);
        } catch (FileCorruptedAtAllStoragesException e) {
            log.error("Storage error has occurred during retrieval process of XML version: " +
                    requestedXml.getVersion() + " of AIP: " + sipId);
            throw e;
        }
        log.info("XML version: " + requestedXml.getVersion() + " of AIP: " + sipId + " has been successfully retrieved.");
        return new ArchivalObjectDto(requestedXml.getId(), xmlRef, requestedXml.getChecksum());
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
     * @throws InvalidChecksumException
     */
    @Transactional
    public void store(AipDto aip) throws CantWriteException, InvalidChecksumException {
        archivalDbService.registerAipCreation(aip.getSip().getId(), aip.getSip().getChecksum(), aip.getXml().getId(),
                aip.getXml().getChecksum());
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
        async.updateObject(new ArchivalObjectDto(toXmlId(sipId, xmlEntity.getVersion()), xml, checksum));
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
    public void delete(String sipId) throws StillProcessingStateException, RollbackStateException,
            StorageException, FailedStateException {
        archivalDbService.registerSipDeletion(sipId);
        async.delete(sipId);
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
    public void remove(String sipId) throws StillProcessingStateException, DeletedStateException,
            RollbackStateException, StorageException, FailedStateException {
        archivalDbService.removeSip(sipId);
        async.remove(sipId);
    }

    /**
     * Retrieves information about AIP.
     *
     * @param sipId
     * @throws StillProcessingStateException
     * @throws StorageException
     */
    public List<AipStateInfoDto> getAipState(String sipId) throws StillProcessingStateException, StorageException {
        AipSip aip = archivalDbService.getAip(sipId);
        if (aip.getState() == ObjectState.PROCESSING)
            throw new StillProcessingStateException(aip);
        List<AipStateInfoDto> aipStateInfoDtos = new ArrayList<>();
        for (StorageService storageService : storageConfigStore.findAll().stream()
                .map(storageProvider::createAdapter)
                .collect(Collectors.toList())) {
            aipStateInfoDtos.add(storageService.getAipInfo(sipId, aip.getChecksum(), aip.getState(),
                    aip.getXmls().stream()
                            .collect(Collectors.toMap(xml -> xml.getVersion(), xml -> xml.getChecksum()))));
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
     * Rollback files which are in {@link ObjectState#PROCESSING} or {@link ObjectState#FAILED} state.
     * This will be used after system failure to clean up storage and free some space.
     *
     * @throws StorageException
     */
    public void cleanUp() throws StorageException {
        int xmlCounter = 0;
        List<AipSip> unfinishedSips = new ArrayList<>();
        List<AipXml> unfinishedXmls = new ArrayList<>();
        archivalDbService.fillUnfinishedFilesLists(unfinishedSips, unfinishedXmls);
        for (StorageService storageService : storageConfigStore.findAll().stream().map(c -> storageProvider.createAdapter(c)).collect(Collectors.toList())) {
            for (AipSip sip : unfinishedSips) {
                if (sip.getXmls().size() > 1)
                    log.warn("Found more than one XML of SIP package with id " + sip.getId() + " which was in PROCESSING state. SIP and its first XML will be rolled back.");
                storageService.rollbackAip(sip.getId());
            }
            for (AipXml xml : unfinishedXmls)
                storageService.rollbackObject(toXmlId(xml.getSip().getId(), xml.getVersion()));
        }
        archivalDbService.rollbackUnfinishedFilesRecords();
        log.info("Successfully rolled back " + unfinishedSips.size() + " SIPs and " + xmlCounter + unfinishedXmls.size() + " XMLs");
    }

    /**
     * Tries to retrieve AIP. Storage is chosen randomly from those with highest priority. If the chose storage throws
     * {@link StorageException}, or checksum does not match, {@link #recoverAipFromOtherStorages(AipSip, List, TreeMap, AipRetrievalResult)}
     * is called to scan through all storages until it finds the right one or throws {@link FileCorruptedAtAllStoragesException} which
     * is propagated.
     *
     * @param sipEntity sip from main request
     * @param xmls      xmls from main request
     * @return valid AIP
     * @throws FileCorruptedAtAllStoragesException
     */
    private AipRetrievalResource retrieveAip(AipSip sipEntity, List<AipXml> xmls) throws FileCorruptedAtAllStoragesException {
        TreeMap<Integer, List<StorageService>> storageServicesByPriorities = getStorageServicesByPriorities();
        List<StorageService> highestPriorityStorageConfigs = storageServicesByPriorities.pollFirstEntry().getValue();

        //shuffle to ensure randomness in selection of the storage config
        shuffle(highestPriorityStorageConfigs);
        StorageService storageServiceChosen = highestPriorityStorageConfigs.get(0);

        AipRetrievalResource aip;
        try {
            AipRetrievalResult result = retrieveAipFromStorage(sipEntity, xmls, storageServiceChosen);
            aip = !result.invalidChecksumFound ? result.getAipFromStorage() :
                    recoverAipFromOtherStorages(sipEntity, xmls, storageServicesByPriorities, result);
        } catch (FileCorruptedAtAllStoragesException e) {
            log.error("Cannot retrieve AIP " + sipEntity.getId() + " from neither of the storages because the checksums do not match.");
            throw e;
        } catch (StorageException e) {
            log.error("Storage error has occurred during retrieval process of AIP: " + sipEntity.getId());
            aip = recoverAipFromOtherStorages(sipEntity, xmls, storageServicesByPriorities, null);
        }
        log.info("AIP: " + sipEntity.getId() + " has been successfully retrieved.");
        return aip;
    }

    /**
     * Tries to retrieve XML. Storage is chosen randomly from those with highest priority. If the chose storage throws
     * {@link StorageException}, or checksum does not match, {@link #recoverXmlFromOtherStorages(AipXml, TreeMap, StorageService)}
     * is called to scan through all storages until it finds the right one or throws {@link FileCorruptedAtAllStoragesException} which
     * is propagated.
     *
     * @param aipXml xml from main request
     * @return valid XML's inputstream
     * @throws FileCorruptedAtAllStoragesException
     */
    private InputStream retrieveXml(AipXml aipXml) throws FileCorruptedAtAllStoragesException {
        TreeMap<Integer, List<StorageService>> storageServicesByPriorities = getStorageServicesByPriorities();
        List<StorageService> highestPriorityStorageConfigs = storageServicesByPriorities.pollFirstEntry().getValue();

        //shuffle to ensure randomness in selection of the storage config
        shuffle(highestPriorityStorageConfigs);
        StorageService storageServiceChosen = highestPriorityStorageConfigs.get(0);

        InputStream xmlRef;
        try {
            xmlRef = retrieveXmlFromStorage(aipXml, storageServiceChosen);
            if (xmlRef == null) {
                xmlRef = recoverXmlFromOtherStorages(aipXml, storageServicesByPriorities, storageServiceChosen);
            }
        } catch (FileCorruptedAtAllStoragesException e) {
            log.error("Cannot retrieve XML " + aipXml.getId() + " form neither of the storages because the checksums do not match.");
            throw e;
        } catch (StorageException e) {
            log.error("Storage error has occurred during retrieval process of XML: " + aipXml.getId());
            xmlRef = recoverXmlFromOtherStorages(aipXml, storageServicesByPriorities, null);
        }
        log.info("XML: " + aipXml.getId() + " has been successfully retrieved.");
        return xmlRef;
    }

    /**
     * Retrieves references to AIP files from storage together with information whether or not are SIP and XMLs valid
     * i.e. their checksum match expected values. Currently SIP is stored to local temp folder and XMLs into main memory.
     * Connection used for retrieval is closed.
     *
     * @param sipEntity      sip from main request
     * @param xmls           xmls from main request
     * @param storageService service used fo retrieval
     * @return AIP with additional information describing wheter the AIP is OK or has to be recovered
     * @throws StorageException
     */
    private AipRetrievalResult retrieveAipFromStorage(AipSip sipEntity, List<AipXml> xmls, StorageService storageService)
            throws StorageException {
        String storageName = storageService.getStorageConfig().getName();
        log.info("Storage: " + storageName + " chosen to retrieve AIP: " + sipEntity.getId());

        AipRetrievalResource aipFromStorage = storageService.getAip(sipEntity.getId(), xmls.stream()
                .map(AipXml::getVersion)
                .collect(Collectors.toList())
                .toArray(new Integer[xmls.size()]));
        AipRetrievalResult result = new AipRetrievalResult(aipFromStorage, storageService);

        File tmpSipFile = new File(tmpFolder.resolve(UUID.randomUUID().toString()).toString());
        try (FileOutputStream fos = new FileOutputStream(tmpSipFile)) {
            //copy sip to tmp folder and verify checksum
            Checksum sipStorageChecksum = copyStreamAndComputeChecksum(aipFromStorage.getSip(), fos, sipEntity.getChecksum().getType());
            if (!sipEntity.getChecksum().equals(sipStorageChecksum)) {
                log.error("Checksum for AIP " + sipEntity.getId() + " is invalid at storage " + storageName);
                result.setInvalidChecksumSip(sipEntity);
                result.setInvalidChecksumFound(true);
            }
            //reassigning the dto with the input stream
            aipFromStorage.setSip(new FileInputStream(tmpSipFile));

            //copy xmls to memory and verify checksums
            for (AipXml xmlEntity : xmls) {
                InputStream xmlFromStorage = aipFromStorage.getXmls().get(xmlEntity.getVersion());
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                IOUtils.copy(xmlFromStorage, baos);
                byte[] bytes = baos.toByteArray();

                //verification of the XML checksum
                Checksum xmlComputedChecksum = StorageUtils.computeChecksum(
                        new ByteArrayInputStream(bytes), xmlEntity.getChecksum().getType());
                if (!xmlEntity.getChecksum().equals(xmlComputedChecksum)) {
                    log.error("Checksum for XML " + xmlEntity.getId() + " is invalid at storage " + storageName);
                    result.addInvalidChecksumXml(xmlEntity);
                    result.setInvalidChecksumFound(true);
                }
                //reassigning the dto with the input stream
                aipFromStorage.getXmls().put(xmlEntity.getVersion(), new ByteArrayInputStream(bytes));
            }
        } catch (IOException e) {
            log.error("Stream with the file content is closed or other unspecified IOException occurred.", e);
            throw new GeneralException(
                    "Stream with the file content is closed or other unspecified IOException occurred.", e);
        } finally {
            aipFromStorage.close();
        }
        return result;
    }

    /**
     * Retrieves single XML from storage. Returns null if the XML has been corrupted, i.e. its checksum does not match expected value.
     * The retrieved XML is stored in main memory. Connection used for retrieval is closed.
     *
     * @param xmlEntity
     * @param storageService
     * @return DTO with XML stream if XML was found and is valid, null if XML checksum does not match expected value
     * @throws StorageException
     */
    private InputStream retrieveXmlFromStorage(AipXml xmlEntity, StorageService storageService)
            throws StorageException {
        String storageName = storageService.getStorageConfig().getName();
        log.info("Storage: " + storageName + " chosen to retrieve AIP: " + xmlEntity.getId());

        ObjectRetrievalResource xmlFromStorage = storageService.getObject(toXmlId(xmlEntity.getSip().getId(), xmlEntity.getVersion()));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            IOUtils.copy(xmlFromStorage.getInputStream(), baos);
        } catch (IOException e) {
            log.error("Stream with the file content is closed or other unspecified IOException occurred.", e);
            throw new GeneralException(
                    "Stream with the file content is closed or other unspecified IOException occurred.", e);
        } finally {
            xmlFromStorage.close();
        }
        byte[] bytes = baos.toByteArray();

        //verification of the XML checksum
        Checksum xmlComputedChecksum = StorageUtils.computeChecksum(
                new ByteArrayInputStream(bytes), xmlEntity.getChecksum().getType());
        if (!xmlEntity.getChecksum().equals(xmlComputedChecksum)) {
            log.error("Checksum for XML " + xmlEntity.getId() + " is invalid at storage " + storageName);
            return null;
        }
        return new ByteArrayInputStream(bytes);
    }

    /**
     * This method is called when the very first attempt to return AIP fails. It scans through all storages until it finds
     * valid AIP. Then it tries to recover all AIPs on storages where the AIP was corrupted. If the recovery fails it is logged
     * and the method continues.
     *
     * @param sipEntity                   sip from the main request
     * @param xmls                        xmls from the main request
     * @param sortedStorageServices       storage services which are shuffled and used for retrieval
     * @param latestInvalidChecksumResult result of the first attempt which has failed because of corrupted AIP,
     *                                    or null if the first attempt failed because of other error
     * @return valid AIP
     * @throws FileCorruptedAtAllStoragesException if no valid AIP was found
     */
    //todo: possible optimization: if SIP is ok there is no need to pull whole AIP from the storage, only particular XML/XMLs
    public AipRetrievalResource recoverAipFromOtherStorages(AipSip sipEntity, List<AipXml> xmls,
                                                            TreeMap<Integer, List<StorageService>> sortedStorageServices,
                                                            AipRetrievalResult latestInvalidChecksumResult) throws FileCorruptedAtAllStoragesException {
        List<StorageService> storageServices = sortedStorageServices.values().stream()
                .map(storageServicesByPriority -> {
                    shuffle(storageServicesByPriority);
                    return storageServicesByPriority;
                })
                .flatMap(List::stream)
                .collect(Collectors.toList());

        List<AipRetrievalResult> invalidChecksumResults = new ArrayList<>();

        if (latestInvalidChecksumResult != null) {
            storageServices.remove(latestInvalidChecksumResult.storageService);
            invalidChecksumResults.add(latestInvalidChecksumResult);
        }

        AipRetrievalResult result = null;
        //iterate over all the storages to find an uncorrupted version of the AIP
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
        if (result.invalidChecksumFound) throw new FileCorruptedAtAllStoragesException(sipEntity);

        log.info("AIP " + sipEntity.getId() + " has been successfully retrieved.");

        for (AipRetrievalResult invalidChecksumResult : invalidChecksumResults) {
            StorageService usedStorageService = invalidChecksumResult.storageService;

            //repair sip at the storage
            if (invalidChecksumResult.invalidChecksumSip != null) {
                InputStream sipInputStream = result.getAipFromStorage().getSip();
                ArchivalObjectDto sipRef = new ArchivalObjectDto(sipEntity.getId(), sipInputStream, sipEntity.getChecksum());
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
                InputStream xmlInputStream = result.getAipFromStorage().getXmls().get(invalidChecksumXml.getVersion());
                String xmlId = toXmlId(sipEntity.getId(), invalidChecksumXml.getVersion());
                ArchivalObjectDto xmlDto = new ArchivalObjectDto(xmlId, xmlInputStream, invalidChecksumXml.getChecksum());
                try {
                    usedStorageService.storeObject(xmlDto, new AtomicBoolean(false));
                    log.info("XML: " + invalidChecksumXml.getId() + " has been successfully recovered at storage" +
                            usedStorageService.getStorageConfig().getName());
                } catch (StorageException e) {
                    log.error("XML: " + invalidChecksumXml.getId() + " has failed to be recovered at storage" +
                            usedStorageService.getStorageConfig().getName());
                }
            }
        }
        return result.getAipFromStorage();
    }

    public InputStream recoverXmlFromOtherStorages(AipXml xmlEntity, TreeMap<Integer, List<StorageService>> sortedStorageServices,
                                                   StorageService invalidChecksumStorage) throws FileCorruptedAtAllStoragesException {
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

        InputStream xmlInputStream = null;
        for (StorageService storageService : storageServices) {
            try {
                xmlInputStream = retrieveXmlFromStorage(xmlEntity, storageService);
                if (xmlInputStream != null) break;
                invalidChecksumStorages.add(storageService);
            } catch (StorageException e) {
                //try other storages when the current storage has failed
                log.error("Storage error has occurred during retrieval process of AIP: " + xmlEntity.getId());
            }
        }
        if (xmlInputStream == null) throw new FileCorruptedAtAllStoragesException(xmlEntity);

        log.info("XML " + xmlEntity.getId() + " has been successfully retrieved");

        for (StorageService storageService : invalidChecksumStorages) {
            //repair xml at the given storage
            String xmlId = toXmlId(xmlEntity.getSip().getId(), xmlEntity.getVersion());
            ArchivalObjectDto xmlDto = new ArchivalObjectDto(xmlId, xmlInputStream, xmlEntity.getChecksum());
            try {
                storageService.storeObject(xmlDto, new AtomicBoolean(false));
                log.info("XML: " + xmlEntity.getId() + " has been successfully recovered at storage" +
                        storageService.getStorageConfig().getName());
            } catch (StorageException e) {
                log.error("XML: " + xmlEntity.getId() + " has been failed to be recovered at storage" +
                        storageService.getStorageConfig().getName());
            }
        }
        return xmlInputStream;
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

    /**
     * private DTO for files returned from storage services together with information whether they are OK or corrupted
     */
    @Getter
    @Setter
    private class AipRetrievalResult {
        private AipRetrievalResource aipFromStorage;
        private StorageService storageService;
        private boolean invalidChecksumFound = false;

        private AipSip invalidChecksumSip;
        private List<AipXml> invalidChecksumXmls = new ArrayList<>();


        public AipRetrievalResult(AipRetrievalResource aipFromStorage, StorageService storageService) {
            this.aipFromStorage = aipFromStorage;
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

    @Inject
    public void setTmpFolder(@Value("${arcstorage.tmp-folder}") String path) {
        this.tmpFolder = Paths.get(path);
    }
}
