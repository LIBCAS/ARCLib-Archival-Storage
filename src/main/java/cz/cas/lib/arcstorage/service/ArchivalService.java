package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.service.exception.FileCorruptedAtAllStoragesException;
import cz.cas.lib.arcstorage.service.exception.InvalidChecksumException;
import cz.cas.lib.arcstorage.service.exception.StorageNotReachableException;
import cz.cas.lib.arcstorage.service.exception.state.DeletedStateException;
import cz.cas.lib.arcstorage.service.exception.state.FailedStateException;
import cz.cas.lib.arcstorage.service.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.service.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.StorageUtils;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.storage.StorageUtils.*;
import static cz.cas.lib.arcstorage.util.Utils.asList;
import static cz.cas.lib.arcstorage.util.Utils.inputStreamToBytes;
import static java.util.Collections.shuffle;

@Service
@Slf4j
public class ArchivalService {

    private ArchivalAsyncService async;
    private ArchivalDbService archivalDbService;
    private StorageProvider storageProvider;
    private Path tmpFolder;

    /**
     * Retrieves reference to AIP.
     *
     * @param sipId
     * @param all   if true reference to SIP and all XMLs is returned otherwise reference to SIP and latest XML is retrieved
     * @return reference of AIP which contains id and inputStream of SIP and XML/XMLs, if there are more XML to return those
     * which are rolled back are skipped
     * @throws DeletedStateException               if SIP is deleted
     * @throws RollbackStateException              if SIP is rolled back or only one XML is requested and that one is rolled back
     * @throws StillProcessingStateException       if SIP or some of requested XML is still processing
     * @throws FileCorruptedAtAllStoragesException if SIP is corrupted at all reachable storages
     * @throws FailedStateException                if SIP is failed
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
        return new ArchivalObjectDto(requestedXml.getId(), toXmlId(sipId, requestedXml.getVersion()), xmlRef, requestedXml.getChecksum());
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
     * @throws StorageNotReachableException
     * @throws InvalidChecksumException
     * @throws IOException
     */
    @Transactional
    public void save(AipDto aip) throws InvalidChecksumException, StorageNotReachableException, IOException {
        List<StorageService> reachableAdapters = storageProvider.createReachableAdapters();
        //validate checksum of XML
        byte[] xmlContent;
        try (BufferedInputStream ios = new BufferedInputStream(aip.getXml().getInputStream())) {
            xmlContent = inputStreamToBytes(ios);
            validateChecksum(aip.getXml().getChecksum(), new ByteArrayInputStream(xmlContent));
            aip.getXml().setInputStream(new ByteArrayInputStream(xmlContent));
        }
        //copy SIP to tmp file and validate its checksum
        Path tmpSipPath = tmpFolder.resolve(aip.getSip().getId());
        try (BufferedInputStream ios = new BufferedInputStream(aip.getSip().getInputStream())) {
            Files.copy(ios, tmpSipPath, StandardCopyOption.REPLACE_EXISTING);
            validateChecksum(aip.getSip().getChecksum(), tmpSipPath);
        }
        archivalDbService.registerAipCreation(aip.getSip().getId(), aip.getSip().getChecksum(), aip.getXml().getDatabaseId(), aip.getXml().getChecksum());
        async.saveAip(aip, tmpSipPath, xmlContent, reachableAdapters);
    }

    /**
     * Stores ARCLib AIP XML into Archival Storage.
     * <p>
     * If MD5 value of file after upload does not match MD5 value provided in request, the database is cleared and exception is thrown.
     *
     * @param sipId    Id of SIP to which XML belongs
     * @param xml      Stream of xml file
     * @param checksum
     */
    @Transactional
    public void saveXml(String sipId, InputStream xml, Checksum checksum, Optional<Integer> version) throws StorageNotReachableException, IOException, InvalidChecksumException {
        List<StorageService> reachableAdapters = storageProvider.createReachableAdapters();
        byte[] bytes;
        try (BufferedInputStream ios = new BufferedInputStream(xml)) {
            bytes = inputStreamToBytes(ios);
            validateChecksum(checksum, new ByteArrayInputStream(bytes));
            xml = new ByteArrayInputStream(bytes);
        }
        AipXml xmlEntity = archivalDbService.registerXmlUpdate(sipId, checksum, version);
        async.saveObject(new ArchivalObjectDto(xmlEntity.getId(), toXmlId(sipId, xmlEntity.getVersion()), xml, checksum), ObjectType.XML, new ByteArrayHolder(bytes), reachableAdapters);
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
            StorageException, FailedStateException, StorageNotReachableException {
        List<StorageService> reachableAdapters = storageProvider.createReachableAdapters();
        archivalDbService.registerSipDeletion(sipId);
        async.deleteAip(sipId, reachableAdapters);
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
            RollbackStateException, StorageException, FailedStateException, StorageNotReachableException {
        List<StorageService> reachableAdapters = storageProvider.createReachableAdapters();
        archivalDbService.removeSip(sipId);
        async.removeAip(sipId, reachableAdapters);
    }

    /**
     * Retrieves information about AIP.
     *
     * @param sipId
     * @throws StillProcessingStateException
     * @throws StorageException
     */
    //todo: implement self-healing
    //todo: recreate current flat structure (objecstate, databasechecksum etc. are replicated in all AipStateInfos..
    //todo: it would be better to have parent object with those and then child objects specific for every storage
    public List<AipStateInfoDto> getAipState(String sipId) throws StillProcessingStateException, StorageException {
        AipSip aip = archivalDbService.getAip(sipId);
        if (aip.getState() == ObjectState.PROCESSING)
            throw new StillProcessingStateException(aip);
        List<AipStateInfoDto> result = storageProvider.createAllAdapters().stream().map(
                adapter -> {
                    if (!adapter.getStorage().isReachable()) {
                        return new AipStateInfoDto(adapter.getStorage().getName(), adapter.getStorage().getStorageType(), aip.getState());
                    } else {
                        try {
                            return adapter.getAipInfo(sipId, aip.getChecksum(), aip.getState(),
                                    aip.getXmls().stream()
                                            .collect(Collectors.toMap(xml -> xml.getVersion(), xml -> xml.getChecksum())));
                        } catch (StorageException e) {
                            return new AipStateInfoDto(adapter.getStorage().getName(), adapter.getStorage().getStorageType(),aip.getState());
                        }
                    }
                }
        ).collect(Collectors.toList());
        log.info(String.format("Info about AIP: %s has been successfully retrieved.", sipId));
        return result;
    }

    /**
     * Returns state of all connected storages.
     *
     * @return
     */
    public List<StorageStateDto> getStorageState() {
        List<StorageStateDto> storageStateDtos = new ArrayList<>();
        storageProvider.createAllAdapters().stream().forEach(
                adapter -> {
                    try {
                        if (!adapter.getStorage().isReachable()) {
                            adapter.getStorage().setReachable(false);
                            storageStateDtos.add(new StorageStateDto(adapter.getStorage(), null));
                        } else
                            storageStateDtos.add(adapter.getStorageState());
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
     * @throws StorageNotReachableException if any storage is unreachable before the process starts
     * @throws StorageException             if any storage fails to rollback
     */
    public void cleanup() throws StorageNotReachableException {
        int xmlCounter = 0;
        List<AipSip> unfinishedSips = new ArrayList<>();
        List<AipXml> unfinishedXmls = new ArrayList<>();
        List<StorageService> storageServices = storageProvider.createReachableAdapters();
        archivalDbService.fillUnfinishedFilesLists(unfinishedSips, unfinishedXmls);
        for (StorageService storageService : storageServices) {
            for (AipSip sip : unfinishedSips) {
                if (sip.getXmls().size() > 1)
                    log.warn("Found more than one XML of SIP package with id " + sip.getId() + " which was in PROCESSING state. SIP and its first XML will be rolled back.");
                try {
                    storageService.rollbackAip(sip.getId());
                } catch (StorageException e) {
                    log.error("cleanup of sip " + sip.getId() + " failed: " + e.getMessage());
                    archivalDbService.setObjectState(sip.getId(), ObjectType.SIP, ObjectState.FAILED);
                }
            }
            for (AipXml xml : unfinishedXmls) {
                try {
                    storageService.rollbackObject(toXmlId(xml.getSip().getId(), xml.getVersion()));
                } catch (StorageException e) {
                    log.error("cleanup of xml " + xml.getId() + " failed: " + e.getMessage());
                    archivalDbService.setObjectState(xml.getId(), ObjectType.XML, ObjectState.FAILED);
                }
            }
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
        List<StorageService> highestPriorityStorages = storageServicesByPriorities.pollFirstEntry().getValue();

        //shuffle to ensure randomness in selection of the storage
        shuffle(highestPriorityStorages);
        StorageService storageServiceChosen = highestPriorityStorages.get(0);

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
        List<StorageService> highestPriorityStorages = storageServicesByPriorities.pollFirstEntry().getValue();

        //shuffle to ensure randomness in selection of the storage
        shuffle(highestPriorityStorages);
        StorageService storageServiceChosen = highestPriorityStorages.get(0);

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
        String storageName = storageService.getStorage().getName();
        log.info("Storage: " + storageName + " chosen to retrieve AIP: " + sipEntity.getId());

        AipRetrievalResource aipFromStorage = storageService.getAip(sipEntity.getId(), xmls.stream()
                .map(AipXml::getVersion)
                .collect(Collectors.toList())
                .toArray(new Integer[xmls.size()]));
        File tmpSipFile = tmpFolder.resolve(aipFromStorage.getId()).toFile();

        AipRetrievalResult result = new AipRetrievalResult(aipFromStorage, storageService);

        try (FileOutputStream fos = new FileOutputStream(tmpSipFile)) {
            //copy sip to tmp folder and verify checksum
            Checksum sipStorageChecksum = copyStreamAndComputeChecksum(aipFromStorage.getSip(), fos, sipEntity.getChecksum().getType());
            if (!sipEntity.getChecksum().equals(sipStorageChecksum)) {
                log.error("Checksum for AIP " + sipEntity.getId() + " is invalid at storage " + storageName);
                result.setInvalidChecksumSip(sipEntity);
                result.setInvalidChecksumFound(true);
            }
            //reassigning the dto with the input stream
            aipFromStorage.setSip(new BufferedInputStream(new FileInputStream(tmpSipFile)));
        } catch (IOException e) {
            log.error("Error while creating or writing to file at " + tmpSipFile, e);
            throw new GeneralException(
                    "Error while creating or writing to file at " + tmpSipFile, e);
        }
        //copy xmls to memory and verify checksums
        for (AipXml xmlEntity : xmls) {
            try (InputStream xmlFromStorage = aipFromStorage.getXmls().get(xmlEntity.getVersion())) {
                byte[] bytes = inputStreamToBytes(xmlFromStorage);

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
            } catch (IOException e) {
                log.error("Error when reading content of XML " + xmlEntity.getId(), e);
                throw new GeneralException(
                        "Error when reading content of XML " + xmlEntity.getId(), e);
            }
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
        String storageName = storageService.getStorage().getName();
        log.info("Storage: " + storageName + " chosen to retrieve AIP: " + xmlEntity.getId());

        ObjectRetrievalResource xmlFromStorage = storageService.getObject(toXmlId(xmlEntity.getSip().getId(), xmlEntity.getVersion()));

        //verification of the XML checksum
        byte[] bytes;
        try (InputStream ios = xmlFromStorage.getInputStream()) {
            bytes = inputStreamToBytes(ios);
            Checksum xmlComputedChecksum = StorageUtils.computeChecksum(
                    new ByteArrayInputStream(bytes), xmlEntity.getChecksum().getType());
            if (!xmlEntity.getChecksum().equals(xmlComputedChecksum)) {
                log.error("Checksum for XML " + xmlEntity.getId() + " is invalid at storage " + storageName);
                return null;
            }
        } catch (IOException e) {
            log.error("Error when reading content of XML " + xmlEntity.getId(), e);
            throw new GeneralException(
                    "Error when reading content of XML " + xmlEntity.getId(), e);
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
            tmpFolder.resolve(latestInvalidChecksumResult.getAipFromStorage().getId()).toFile().delete();
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
                tmpFolder.resolve(result.getAipFromStorage().getId()).toFile().delete();
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
                SipDto sipRef = new SipDto(sipEntity.getId(), sipInputStream, sipEntity.getChecksum());
                try {
                    usedStorageService.storeSip(sipRef, new AtomicBoolean(false));
                    log.info("SIP " + sipEntity.getId() + " has been successfully recovered at storage" +
                            usedStorageService.getStorage().getName());
                } catch (StorageException e) {
                    log.error("SIP " + sipEntity.getId() + " has failed to be recovered at storage" +
                            usedStorageService.getStorage().getName());
                }
            }

            //repair xmls at the storage
            for (AipXml invalidChecksumXml : invalidChecksumResult.invalidChecksumXmls) {
                InputStream xmlInputStream = result.getAipFromStorage().getXmls().get(invalidChecksumXml.getVersion());
                String xmlStorageId = toXmlId(sipEntity.getId(), invalidChecksumXml.getVersion());
                ArchivalObjectDto xmlDto = new ArchivalObjectDto(invalidChecksumXml.getId(), xmlStorageId, xmlInputStream, invalidChecksumXml.getChecksum());
                try {
                    usedStorageService.storeObject(xmlDto, new AtomicBoolean(false));
                    log.info("XML: " + invalidChecksumXml.getId() + " has been successfully recovered at storage" +
                            usedStorageService.getStorage().getName());
                } catch (StorageException e) {
                    log.error("XML: " + invalidChecksumXml.getId() + " has failed to be recovered at storage" +
                            usedStorageService.getStorage().getName());
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
            String xmlStorageId = toXmlId(xmlEntity.getSip().getId(), xmlEntity.getVersion());
            ArchivalObjectDto xmlDto = new ArchivalObjectDto(xmlEntity.getId(), xmlStorageId, xmlInputStream, xmlEntity.getChecksum());
            try {
                storageService.storeObject(xmlDto, new AtomicBoolean(false));
                log.info("XML: " + xmlEntity.getId() + " has been successfully recovered at storage" +
                        storageService.getStorage().getName());
            } catch (StorageException e) {
                log.error("XML: " + xmlEntity.getId() + " has failed to be recovered at storage" +
                        storageService.getStorage().getName());
            }
        }
        return xmlInputStream;
    }

    /**
     * called only by retrieval, GET methods.. methods which writes writes to all storages
     *
     * @return
     */
    private TreeMap<Integer, List<StorageService>> getStorageServicesByPriorities() {
        //sorted map where the keys are the priorities and the values are the lists of storage services
        TreeMap<Integer, List<StorageService>> storageServicesByPriorities = new TreeMap<>(Collections.reverseOrder());
        storageProvider.createAllAdapters().forEach(adapter -> {
            if (adapter.getStorage().isReachable()) {
                List<StorageService> storageServices = storageServicesByPriorities.get(adapter.getStorage().getPriority());
                if (storageServices == null) storageServices = new ArrayList<>();
                storageServices.add(adapter);
                storageServicesByPriorities.put(adapter.getStorage().getPriority(), storageServices);
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
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    @Inject
    public void setTmpFolder(@Value("${arcstorage.tmp-folder}") String path) {
        this.tmpFolder = Paths.get(path);
    }
}
