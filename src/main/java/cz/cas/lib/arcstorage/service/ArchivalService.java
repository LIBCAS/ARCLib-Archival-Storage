package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.service.exception.BadXmlVersionProvidedException;
import cz.cas.lib.arcstorage.service.exception.InvalidChecksumException;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.state.*;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.ObjectCouldNotBeRetrievedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.util.Utils;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.inject.Inject;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.storage.StorageUtils.*;
import static cz.cas.lib.arcstorage.util.Utils.*;

@Service
@Slf4j
public class ArchivalService {

    private ArchivalAsyncService async;
    private ArchivalDbService archivalDbService;
    private StorageProvider storageProvider;
    private Path tmpFolder;
    private ExecutorService executor;
    private ArcstorageMailCenter arcstorageMailCenter;

    /**
     * Retrieves reference to AIP.
     *
     * @param sipId id of the AIP to retrieve
     * @param all   if <code>true</code> reference to SIP and all XMLs is returned otherwise reference to SIP and latest XML is retrieved
     * @return reference of AIP which contains id and inputStream of SIP and XML/XMLs, if there are more XML to return those
     * which are rolled back are skipped
     * @throws DeletedStateException              if SIP is deleted
     * @throws RollbackStateException             if SIP is rolled back or only one XML is requested and that one is rolled back
     * @throws StillProcessingStateException      if SIP or some of requested XML is still processing
     * @throws ObjectCouldNotBeRetrievedException if SIP is corrupted at all reachable storages
     * @throws FailedStateException               if SIP is failed
     * @throws RemovedStateException
     * @throws NoLogicalStorageReachableException
     * @throws NoLogicalStorageAttachedException
     */
    public AipRetrievalResource getAip(String sipId, boolean all) throws RollbackStateException,
            StillProcessingStateException, DeletedStateException, FailedStateException,
            ObjectCouldNotBeRetrievedException, RemovedStateException, NoLogicalStorageReachableException,
            NoLogicalStorageAttachedException {
        log.info("Retrieving AIP with id " + sipId + ".");

        AipSip sipEntity = archivalDbService.getAip(sipId);

        switch (sipEntity.getState()) {
            case PROCESSING:
            case PRE_PROCESSING:
                throw new StillProcessingStateException(sipEntity);
            case ARCHIVAL_FAILURE:
                throw new FailedStateException(sipEntity);
            case DELETED:
            case DELETION_FAILURE:
                throw new DeletedStateException(sipEntity);
            case ROLLED_BACK:
                throw new RollbackStateException(sipEntity);
            case REMOVED:
                throw new RemovedStateException(sipEntity);
        }

        List<AipXml> xmls = all ? sipEntity.getArchivedXmls() : asList(sipEntity.getLatestArchivedXml());
        if (xmls.isEmpty())
            throw new IllegalStateException("found ARCHIVED SIP " + sipId + " with no ARCHIVED XML");
        return retrieveAip(sipEntity, xmls);
    }

    /**
     * Retrieves AIP XML reference.
     *
     * @param sipId   id of the AIP that the XML belongs
     * @param version specifies version of XML to return, by default the latest XML is returned
     * @return reference to AIP XML
     * @throws FailedStateException
     * @throws RollbackStateException
     * @throws ObjectCouldNotBeRetrievedException
     * @throws StillProcessingStateException
     * @throws ObjectCouldNotBeRetrievedException
     * @throws NoLogicalStorageReachableException
     * @throws NoLogicalStorageAttachedException
     */
    public Pair<Integer, ObjectRetrievalResource> getXml(String sipId, Integer version) throws
            FailedStateException, RollbackStateException, StillProcessingStateException,
            ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        log.info("Retrieving XML of AIP with id " + sipId + ".");

        AipSip sipEntity = archivalDbService.getAip(sipId);
        AipXml requestedXml;
        if (version != null) {
            Optional<AipXml> xmlOpt = sipEntity.getXmls().stream().filter(xml -> xml.getVersion() == version).findFirst();
            if (!xmlOpt.isPresent()) {
                log.warn("Could not find XML version: " + version + " of AIP: " + sipId);
                throw new MissingObject(AipXml.class, sipId + " version: " + version);
            }
            requestedXml = xmlOpt.get();
        } else
            requestedXml = sipEntity.getLatestArchivedXml();
        return new Pair<>(requestedXml.getVersion(), getObject(requestedXml.toDto()));
    }

    /**
     * Retrieves object reference.
     *
     * @param id id of the object to retrieve
     * @return reference to object
     * @throws FailedStateException
     * @throws RollbackStateException
     * @throws ObjectCouldNotBeRetrievedException
     * @throws StillProcessingStateException
     * @throws ObjectCouldNotBeRetrievedException
     * @throws NoLogicalStorageReachableException
     * @throws NoLogicalStorageAttachedException
     */
    public ObjectRetrievalResource getGeneralObject(String id) throws
            FailedStateException, RollbackStateException, StillProcessingStateException,
            ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        return getObject(archivalDbService.getObject(id).toDto());
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
     * @param aip AIP to store
     * @return SIP ID of created AIP
     * @throws SomeLogicalStoragesNotReachableException
     * @throws InvalidChecksumException
     * @throws IOException
     * @throws NoLogicalStorageAttachedException
     * @throws ReadOnlyStateException
     */
    public void saveAip(AipDto aip) throws InvalidChecksumException, SomeLogicalStoragesNotReachableException, IOException,
            NoLogicalStorageAttachedException, ReadOnlyStateException {
        log.info("Saving AIP with id " + aip.getSip().getStorageId());

        AipSip aipSip = archivalDbService.registerAipCreation(aip.getSip().getDatabaseId(), aip.getSip().getChecksum(),
                aip.getXml().getDatabaseId(), aip.getXml().getChecksum());
        List<StorageService> reachableAdapters;
        Path tmpSipPath;
        byte[] xmlContent;
        try {
            reachableAdapters = storageProvider.createAdaptersForWriteOperation();
            //validate checksum of XML
            try (BufferedInputStream ios = new BufferedInputStream(aip.getXml().getInputStream())) {
                xmlContent = inputStreamToBytes(ios);
                validateChecksum(aip.getXml().getChecksum(), new ByteArrayInputStream(xmlContent));
                log.info("Checksum of XML of AIP with id " + aip.getSip().getStorageId() + " has been validated.");
                aip.getXml().setInputStream(new ByteArrayInputStream(xmlContent));
            }
            //copy SIP to tmp file and validate its checksum
            tmpSipPath = tmpFolder.resolve(aip.getSip().getDatabaseId());
            try (BufferedInputStream ios = new BufferedInputStream(aip.getSip().getInputStream())) {
                Files.copy(ios, tmpSipPath, StandardCopyOption.REPLACE_EXISTING);
                log.info("SIP content of AIP with id " + aip.getSip().getStorageId() + " has been stored to temporary storage.");
                validateChecksum(aip.getSip().getChecksum(), tmpSipPath);
                log.info("Checksum of SIP of AIP with id " + aip.getSip().getStorageId() + " has been validated.");
            }
        } catch (Exception e) {
            archivalDbService.setObjectState(aip.getSip().getDatabaseId(), ObjectState.ARCHIVAL_FAILURE);
            throw e;
        }
        aip.getSip().setState(ObjectState.PROCESSING);
        aip.getXml().setState(ObjectState.PROCESSING);
        archivalDbService.setObjectsState(ObjectState.PROCESSING, asList(aip.getSip().getDatabaseId(), aip.getXml().getDatabaseId()));

        async.saveAip(aip, tmpSipPath, xmlContent, reachableAdapters, aipSip.getOwner().getDataSpace());
    }

    /**
     * Stores ARCLib AIP XML into Archival Storage.
     * <p>
     * If MD5 value of file after upload does not match MD5 value provided in request, the database is cleared and exception is thrown.
     *
     * @param sipId    Id of SIP to which XML belongs
     * @param xml      Stream of xml file
     * @param checksum checksum of the XML
     * @param version  version of the XML
     * @throws SomeLogicalStoragesNotReachableException
     * @throws IOException
     * @throws NoLogicalStorageAttachedException
     */
    public void saveXmlAsynchronously(String sipId, InputStream xml, Checksum checksum, Integer version)
            throws SomeLogicalStoragesNotReachableException, IOException, NoLogicalStorageAttachedException,
            DeletedStateException, FailedStateException, RollbackStateException, StillProcessingStateException,
            BadXmlVersionProvidedException, ReadOnlyStateException {
        log.info("Asynchronously saving XML in version " + version + " of AIP with id " + sipId + ".");
        AipXml xmlEntity = archivalDbService.registerXmlUpdate(sipId, checksum, version);
        byte[] bytes;
        List<StorageService> reachableAdapters;
        try {
            reachableAdapters = storageProvider.createAdaptersForWriteOperation();
            try (BufferedInputStream ios = new BufferedInputStream(xml)) {
                bytes = inputStreamToBytes(ios);
                validateChecksum(checksum, new ByteArrayInputStream(bytes));
                log.info("Checksum of XML in version " + version + " of AIP with id " + sipId + " has been validated.");
                xml = new ByteArrayInputStream(bytes);
            }
        } catch (Exception e) {
            archivalDbService.setObjectState(xmlEntity.getId(), ObjectState.ARCHIVAL_FAILURE);
            throw e;
        }
        xmlEntity.setState(ObjectState.PROCESSING);
        archivalDbService.saveObject(xmlEntity);
        log.info("State of object with id " + xmlEntity.getId() + " changed to " + ObjectState.PROCESSING);
        ArchivalObjectDto objectDto = xmlEntity.toDto();
        objectDto.setInputStream(xml);
        async.saveObject(objectDto, new ByteArrayHolder(bytes), reachableAdapters);
    }

    /**
     * Stores ARCLib AIP XML into Archival Storage.
     * <p>
     * If MD5 value of file after upload does not match MD5 value provided in request, the database is cleared and exception is thrown.
     *
     * @param sipId    Id of SIP to which XML belongs
     * @param xml      Stream of xml file
     * @param checksum checksum of the Xml
     * @throws SomeLogicalStoragesNotReachableException
     * @throws IOException
     * @throws NoLogicalStorageAttachedException
     */
    @Transactional
    public void saveXmlSynchronously(String sipId, InputStream xml, Checksum checksum, Integer version)
            throws SomeLogicalStoragesNotReachableException, IOException, NoLogicalStorageAttachedException, DeletedStateException,
            FailedStateException, RollbackStateException, StillProcessingStateException, BadXmlVersionProvidedException, ReadOnlyStateException {
        log.info("Synchronously saving XML in version " + version + " of AIP with id " + sipId + ".");

        AipXml xmlEntity = archivalDbService.registerXmlUpdate(sipId, checksum, version);
        byte[] bytes;
        List<StorageService> reachableAdapters;
        try {
            reachableAdapters = storageProvider.createAdaptersForWriteOperation();
            try (BufferedInputStream ios = new BufferedInputStream(xml)) {
                bytes = inputStreamToBytes(ios);
                validateChecksum(checksum, new ByteArrayInputStream(bytes));
                log.info("Checksum of XML in version " + version + " of AIP with id " + sipId + " has been validated.");
                xml = new ByteArrayInputStream(bytes);
            }
        } catch (Exception e) {
            archivalDbService.setObjectState(xmlEntity.getId(), ObjectState.ARCHIVAL_FAILURE);
            throw e;
        }
        xmlEntity.setState(ObjectState.PROCESSING);
        archivalDbService.saveObject(xmlEntity);
        log.info("State of object with id " + xmlEntity.getId() + " changed to " + ObjectState.PROCESSING);
        ArchivalObjectDto objectDto = xmlEntity.toDto();
        objectDto.setInputStream(xml);
        saveObjectSynchronously(objectDto, new ByteArrayHolder(bytes), reachableAdapters);
    }

    /**
     * Physically removes object from storage. Data in transaction database are not removed.
     *
     * @param id id of the object to delete
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     * @throws FailedStateException
     * @throws SomeLogicalStoragesNotReachableException
     * @throws NoLogicalStorageAttachedException
     * @throws ReadOnlyStateException
     */
    public void delete(String id) throws StillProcessingStateException, RollbackStateException,
            FailedStateException, SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException {
        log.info("Deleting object with id " + id + ".");
        List<StorageService> reachableAdapters = storageProvider.createAdaptersForWriteOperation();
        ArchivalObject archivalObject = archivalDbService.registerObjectDeletion(id);
        async.deleteObject(archivalObject.toDto(), reachableAdapters);
    }

    /**
     * Logically removes object.
     *
     * @param id id of the object to remove
     * @throws DeletedStateException
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     * @throws StorageException
     * @throws FailedStateException
     * @throws SomeLogicalStoragesNotReachableException
     * @throws ReadOnlyStateException
     * @throws NoLogicalStorageAttachedException
     */
    public void removeObject(String id) throws StillProcessingStateException, DeletedStateException,
            RollbackStateException, FailedStateException, SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException, StorageException {
        log.info("Removing object with id " + id + ".");
        List<StorageService> reachableAdapters = storageProvider.createAdaptersForWriteOperation();
        ArchivalObject obj = archivalDbService.removeObject(id);
        async.removeObject(id, reachableAdapters, obj.getOwner().getDataSpace());
    }

    /**
     * Renews logically removed object.
     *
     * @param id id of the object to renew
     * @throws StorageException
     * @throws DeletedStateException
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     * @throws FailedStateException
     * @throws SomeLogicalStoragesNotReachableException
     * @throws NoLogicalStorageAttachedException
     * @throws ReadOnlyStateException
     */
    public void renew(String id) throws StillProcessingStateException, DeletedStateException,
            RollbackStateException, StorageException, FailedStateException, SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException {
        log.info("Renewing object with id " + id + ".");
        List<StorageService> reachableAdapters = storageProvider.createAdaptersForWriteOperation();
        ArchivalObject archivalObject = archivalDbService.renewObject(id);
        async.renewObject(id, reachableAdapters, archivalObject.getOwner().getDataSpace());
    }

//    /**
//     * Retrieves information about AIP from every storage.
//     *
//     * @param sipId
//     * @throws StillProcessingStateException
//     * @throws NoLogicalStorageAttachedException
//     */
//    //todo: implement self-healing
//    //todo: recreate current flat structure (objecstate, databasechecksum etc. are replicated in all AipStateInfos..
//    //todo: it would be better to have parent object with those and then child objects specific for every storage
//    public List<AipStateInfoDto> getAipStates(String sipId) throws StillProcessingStateException, NoLogicalStorageAttachedException {
//        AipSip aip = archivalDbService.getAip(sipId);
//        if (aip.getState() == ObjectState.PROCESSING)
//            throw new StillProcessingStateException(aip);
//        List<AipStateInfoDto> result = storageProvider.createAllAdapters().stream().map(
//                adapter -> {
//                    if (!adapter.getStorage().isReachable()) {
//                        return new AipStateInfoDto(adapter.getStorage().getName(), adapter.getStorage().getStorageType(),
//                                aip.getState());
//                    } else {
//                        try {
//                            return adapter.getAipInfo(sipId, aip.getChecksum(), aip.getState(),
//                                    aip.getXmls().stream()
//                                            .collect(Collectors.toMap(xml -> xml.getVersion(), xml -> xml.getChecksum())));
//                        } catch (StorageException e) {
//                            return new AipStateInfoDto(adapter.getStorage().getName(), adapter.getStorage().getStorageType(),
//                                    aip.getState());
//                        }
//                    }
//                }
//        ).collect(Collectors.toList());
//        log.info(String.format("Info about AIP: %s has been successfully retrieved.", sipId));
//        return result;
//    }

    /**
     * Checks whether AIP is in the ARCHIVED state in database.
     *
     * @param aipId id of the AIP
     * @return state of the AIP
     */
    public ObjectState getAipState(String aipId) {
        log.info("Getting AIP state of AIP with id " + aipId + ".");
        AipSip aip = archivalDbService.getAip(aipId);
        Utils.notNull(aip, () -> new MissingObject(AipSip.class, aipId));
        return aip.getState();
    }

    /**
     * Retrieves information about AIP from a single storage.
     *
     * @param sipId id of the AIP
     * @throws StorageException
     */
    //todo: implement self-healing
    //todo: recreate current flat structure (objecstate, databasechecksum etc. are replicated in all AipStateInfos..
    //todo: it would be better to have parent object with those and then child objects specific for every storage
    public AipStateInfoDto getAipInfo(String sipId, String storageId) throws StorageException {
        log.info("Getting AIP info of AIP with id " + sipId + " at storage with id " + storageId + ".");
        AipSip aip = archivalDbService.getAip(sipId);

        StorageService storageService = storageProvider.createAdapter(storageId);
        if ((aip.getState() != ObjectState.ARCHIVED && aip.getState() != ObjectState.REMOVED)
                || !storageService.getStorage().isReachable()) {
            AipStateInfoDto incompleteStateInfo = new AipStateInfoDto(storageService.getStorage().getName(),
                    storageService.getStorage().getStorageType(), aip.getState(), aip.getChecksum(),
                    storageService.getStorage().isReachable());
            aip.getXmls().forEach(x -> incompleteStateInfo.addXmlInfo(new XmlStateInfoDto(x.getVersion(),
                    false, null, x.getChecksum())));
            return incompleteStateInfo;
        }
        AipStateInfoDto result = storageService.getAipInfo(sipId, aip.getChecksum(), aip.getState(),
                aip.getXmls().stream().collect(Collectors.toMap(xml -> xml.getVersion(), xml -> xml.getChecksum())), aip.getOwner().getDataSpace());

        log.info(String.format("Info about AIP: %s has been successfully retrieved from storage %s.", sipId,
                storageService.getStorage().getName()));
        return result;
    }

    /**
     * Returns state of all connected storages.
     *
     * @return state for every storage
     */
    public List<StorageStateDto> getStorageState() throws NoLogicalStorageAttachedException {
        log.info("Getting storage states of all storages.");
        List<StorageStateDto> storageStateDtos = new ArrayList<>();
        storageProvider.createAllAdapters().stream().forEach(
                adapter -> {
                    try {
                        if (!adapter.getStorage().isReachable()) {
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
     * Retrieves object if the object is in the allowed state or throws corresponding exception.
     *
     * @param objectDto dto with the object to retrieve
     * @return retrieved object
     * @throws FailedStateException
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     * @throws ObjectCouldNotBeRetrievedException
     * @throws NoLogicalStorageReachableException
     * @throws NoLogicalStorageAttachedException
     */
    public ObjectRetrievalResource getObject(ArchivalObjectDto objectDto) throws
            FailedStateException, RollbackStateException, StillProcessingStateException,
            ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        log.info("Retrieving object with storage id " + objectDto.getStorageId() + ".");

        switch (objectDto.getState()) {
            case ROLLED_BACK:
                throw new RollbackStateException(objectDto);
            case ARCHIVAL_FAILURE:
                throw new FailedStateException(objectDto);
            case PROCESSING:
            case PRE_PROCESSING:
                throw new StillProcessingStateException(objectDto);
        }
        ObjectRetrievalResource objectRef;
        try {
            objectRef = retrieveObject(objectDto);
        } catch (ObjectCouldNotBeRetrievedException e) {
            log.error("Storage error has occurred during retrieval process of object " + objectDto.getStorageId());
            throw e;
        }
        log.info("object " + objectDto.getStorageId() + " has been successfully retrieved.");
        return objectRef;
    }

    /**
     * Retrieves AIP.
     * <p>
     * Storage is chosen randomly from those with highest priority. If the chosen storage throws
     * {@link StorageException}, or checksum does not match, {@link #recoverAipFromOtherStorages(AipSip, List, List, AipRetrievalResult)}
     * is called to scan through all storages until it finds the right one or throws {@link ObjectCouldNotBeRetrievedException} which
     * is propagated.
     *
     * @param sipEntity sip from main request
     * @param xmls      xmls from main request
     * @return valid AIP
     * @throws ObjectCouldNotBeRetrievedException if AIP is corrupted at the given storages
     */
    private AipRetrievalResource retrieveAip(AipSip sipEntity, List<AipXml> xmls)
            throws ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        log.info("Retrieving AIP with id " + sipEntity.getId() + ".");

        List<StorageService> storageServicesByPriorities = storageProvider.getReachableStorageServicesByPriorities();

        AipRetrievalResource aip;
        try {
            AipRetrievalResult result = retrieveAipFromStorage(sipEntity, xmls, storageServicesByPriorities.get(0));
            aip = !result.invalidChecksumFound ? result.getAipFromStorage() :
                    recoverAipFromOtherStorages(sipEntity, xmls, storageServicesByPriorities, result);
        } catch (ObjectCouldNotBeRetrievedException e) {
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
     * Retrieves object.
     * <p>
     * Storage is chosen randomly from those with highest priority. If the chose storage throws
     * {@link StorageException}, or checksum does not match, {@link #recoverObjectFromOtherStorages(ArchivalObjectDto, List, boolean)}
     * is called to scan through all storages until it finds the right one or throws {@link ObjectCouldNotBeRetrievedException} which
     * is propagated.
     *
     * @param archivalObject object from main request
     * @return {@link ObjectRetrievalResource} with valid object's input stream
     * @throws ObjectCouldNotBeRetrievedException if object is corrupted at the given storages
     */
    private ObjectRetrievalResource retrieveObject(ArchivalObjectDto archivalObject)
            throws ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, NoLogicalStorageAttachedException {
        log.info("Retrieving archival object with storage id " + archivalObject.getStorageId() + ".");

        List<StorageService> storageServicesByPriorities = storageProvider.getReachableStorageServicesByPriorities();
        ObjectRetrievalResource objectRef;
        try {
            objectRef = retrieveObjectFromStorage(archivalObject, storageServicesByPriorities.get(0));
            if (objectRef == null) {
                objectRef = recoverObjectFromOtherStorages(archivalObject, storageServicesByPriorities, true);
            }
        } catch (ObjectCouldNotBeRetrievedException e) {
            log.error("Cannot retrieve object " + archivalObject.getStorageId() + " form neither of the storages.");
            throw e;
        } catch (StorageException e) {
            log.error("Storage error has occurred during retrieval process of object: " + archivalObject.getStorageId());
            objectRef = recoverObjectFromOtherStorages(archivalObject, storageServicesByPriorities, false);
        }
        log.info("object: " + archivalObject.getStorageId() + " has been successfully retrieved.");
        return objectRef;
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
     * @throws StorageException if an error occurred during AIP retrieval
     */
    private AipRetrievalResult retrieveAipFromStorage(AipSip sipEntity, List<AipXml> xmls, StorageService storageService)
            throws StorageException {
        String storageName = storageService.getStorage().getName();
        log.info("Storage: " + storageName + " chosen to retrieve AIP: " + sipEntity.getId());

        AipRetrievalResource aipFromStorage = storageService.getAip(sipEntity.getId(), sipEntity.getOwner().getDataSpace(), xmls.stream()
                .map(AipXml::getVersion)
                .collect(Collectors.toList())
                .toArray(new Integer[xmls.size()]));
        String tmpSipFileId = aipFromStorage.getId();
        File tmpSipFile = tmpFolder.resolve(tmpSipFileId).toFile();

        AipRetrievalResult result = new AipRetrievalResult(aipFromStorage, storageService);

        boolean sipValid = copyObjectToTmpFolderAndVerifyChecksum(aipFromStorage.getSip(), sipEntity.getChecksum(),
                tmpSipFile, tmpSipFileId, storageName);
        if (!sipValid) {
            log.info("Invalid checksum of SIP with id " + sipEntity.getId() + " at storage " + storageService.getStorage().getName() + ".");
            result.setInvalidChecksumSip(sipEntity);
            result.setInvalidChecksumFound(true);
        }
        //reassigning the dto with the input stream
        else {
            log.info("Validated checksum of SIP with id " + sipEntity.getId() + " retrieved from storage " +
                    storageService.getStorage().getName() + ".");
            try {
                aipFromStorage.setSip(new FileInputStream(tmpSipFile));
            } catch (FileNotFoundException e) {
                throw new UncheckedIOException("could not find tmp file " + aipFromStorage.getId(), e);
            }
        }

        //copy xmls to tmp folders and verify checksum
        for (AipXml xmlEntity : xmls) {
            String tmpXmlFileId = toXmlId(aipFromStorage.getId(), xmlEntity.getVersion());
            File tmpXmlFile = tmpFolder.resolve(tmpXmlFileId).toFile();
            boolean xmlValid = copyObjectToTmpFolderAndVerifyChecksum(aipFromStorage.getXmls().get(xmlEntity.getVersion()),
                    xmlEntity.getChecksum(), tmpXmlFile, tmpXmlFileId, storageName);

            if (!xmlValid) {
                result.addInvalidChecksumXml(xmlEntity);
                result.setInvalidChecksumFound(true);
            }
            //reassigning the dto with the input stream
            else {
                log.info("Validated checksum of XML with id " + xmlEntity.getId() + " of AIP with id " + sipEntity.getId() +
                        " retrieved from storage " + storageService.getStorage().getName() + ".");
                try {
                    aipFromStorage.getXmls().put(xmlEntity.getVersion(), new FileInputStream(tmpXmlFile));
                } catch (FileNotFoundException e) {
                    throw new UncheckedIOException("could not find tmp file " + tmpXmlFileId, e);
                }
            }
        }
        return result;
    }

    /**
     * Retrieves single object from storage. Returns <code>null</code> if the object has been corrupted, i.e. its checksum does not match expected value.
     * The retrieved object is stored in temporary file. Connection used for retrieval is closed.
     *
     * @param objectDto      DTO with the object to retrieve
     * @param storageService storage services to retrieve he object from
     * @return DTO with object stream if object was found and is valid, null if object checksum does not match expected value
     * @throws StorageException if an error occurred during object retrieval
     */
    private ObjectRetrievalResource retrieveObjectFromStorage(ArchivalObjectDto objectDto, StorageService storageService)
            throws StorageException {
        String storageName = storageService.getStorage().getName();
        log.info("Storage: " + storageName + " chosen to retrieve object: " + objectDto.getStorageId());

        ObjectRetrievalResource objectFromStorage = storageService.getObject(objectDto.getStorageId(), objectDto.getOwner().getDataSpace());
        String tmpFileId = objectFromStorage.getId();
        File tmpFile = tmpFolder.resolve(tmpFileId).toFile();
        boolean valid = copyObjectToTmpFolderAndVerifyChecksum(objectFromStorage.getInputStream(), objectDto.getChecksum(), tmpFile, tmpFileId, storageName);
        if (!valid)
            return null;
        try {
            log.info("Validated checksum of object with storage id " + objectDto.getStorageId() +
                    " retrieved from storage " + storageService.getStorage().getName() + ".");
            objectFromStorage.setInputStream(new FileInputStream(tmpFile));
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException("could not find tmp file " + objectFromStorage.getId(), e);
        }
        return objectFromStorage;
    }

    /**
     * This method is called when the very first attempt to return AIP fails. It scans through all storages until it finds
     * valid AIP. Then it tries to recover all AIPs on storages where the AIP was corrupted. If the recovery fails it is logged
     * and the method continues.
     *
     * @param sipEntity                   sip from the main request
     * @param xmls                        xmls from the main request
     * @param storageServices             storage services which are shuffled and used for retrieval
     * @param latestInvalidChecksumResult result of the first attempt which has failed because of corrupted AIP,
     *                                    or null if the first attempt failed because of other error
     * @return valid AIP
     * @throws ObjectCouldNotBeRetrievedException if no valid AIP was found
     */
    //todo: possible optimization: if SIP is ok there is no need to pull whole AIP from the storage, only particular XML/XMLs
    private AipRetrievalResource recoverAipFromOtherStorages(AipSip sipEntity, List<AipXml> xmls,
                                                             List<StorageService> storageServices,
                                                             AipRetrievalResult latestInvalidChecksumResult)
            throws ObjectCouldNotBeRetrievedException {
        log.info("Recovering AIP: " + sipEntity.getId() + " from other storages.");
        List<AipRetrievalResult> invalidChecksumResults = new ArrayList<>();

        if (latestInvalidChecksumResult != null) {
            tmpFolder.resolve(latestInvalidChecksumResult.getAipFromStorage().getId()).toFile().delete();
            invalidChecksumResults.add(latestInvalidChecksumResult);
        }

        AipRetrievalResult result = null;
        StorageService successfulService = null;
        //iterate over all the storages to find an uncorrupted version of the AIP
        for (int i = 1; i < storageServices.size(); i++) {
            try {
                result = retrieveAipFromStorage(sipEntity, xmls, storageServices.get(i));
                if (!result.invalidChecksumFound) {
                    successfulService = storageServices.get(i);
                    break;
                }
                invalidChecksumResults.add(result);
                tmpFolder.resolve(result.getAipFromStorage().getId()).toFile().delete();
            } catch (StorageException e) {
                //try other storages when the current storage has failed
                log.error("Storage error has occurred during retrieval process of AIP " + sipEntity.getId() + " from storage " +
                        storageServices.get(i).getStorage().getName() + ".");
            }
        }
        List<StorageService> invalidChecksumStorages = invalidChecksumResults.stream()
                .map(AipRetrievalResult::getStorageService)
                .collect(Collectors.toList());

        if (successfulService == null) {
            log.error("AIP: " + sipEntity.getId() + " has failed to be recovered from any storage service.");
            storageServices.removeAll(invalidChecksumStorages);
            arcstorageMailCenter.sendObjectRetrievalError(sipEntity.toDto(), null, servicesToEntities(storageServices), servicesToEntities(invalidChecksumStorages), null);
            throw new ObjectCouldNotBeRetrievedException(sipEntity);
        }

        log.info("AIP " + sipEntity.getId() + " has been successfully retrieved.");
        List<StorageService> recoveredStorages = new ArrayList<>();
        for (AipRetrievalResult invalidChecksumResult : invalidChecksumResults) {
            StorageService usedStorageService = invalidChecksumResult.storageService;
            boolean success = true;
            //repair sip at the storage
            if (invalidChecksumResult.invalidChecksumSip != null) {
                success = recoverSingleObject(usedStorageService, sipEntity.toDto(), result.getAipFromStorage().getId());
            }
            //repair XMLs at the storage
            for (AipXml xml : invalidChecksumResult.invalidChecksumXmls) {
                ArchivalObjectDto xmlDto = xml.toDto();
                success = success && recoverSingleObject(usedStorageService, xmlDto, xmlDto.getStorageId());
            }
            if (success)
                log.info("AIP has been successfully recovered at storage " + usedStorageService.getStorage().getName() + ".");
            recoveredStorages.add(invalidChecksumResult.getStorageService());
        }
        storageServices.removeAll(invalidChecksumStorages);
        arcstorageMailCenter.sendObjectRetrievalError(sipEntity.toDto(), successfulService.getStorage(), servicesToEntities(storageServices),
                servicesToEntities(invalidChecksumStorages), servicesToEntities(recoveredStorages));
        return result.getAipFromStorage();
    }

    /**
     * This method is called when the very first attempt to return object fails. It scans through all storages until it finds
     * valid object. Then it tries to recover all objects on storages where the object was corrupted. If the recovery fails it is logged
     * and the method continues.
     *
     * @param objectDto                 object from the main request
     * @param storageServices           storage services which are used for retrieval
     * @param problemWasInvalidChecksum true if the first storage has failed because of corrupted object,
     *                                  or false if the first attempt failed because of other error
     * @return {@link ObjectRetrievalResource} with valid object
     * @throws ObjectCouldNotBeRetrievedException if no valid object was found
     */
    private ObjectRetrievalResource recoverObjectFromOtherStorages(ArchivalObjectDto objectDto, List<StorageService> storageServices,
                                                                   boolean problemWasInvalidChecksum) throws ObjectCouldNotBeRetrievedException {
        log.info("Recovering object " + objectDto.getStorageId() + " from other storages.");
        List<StorageService> invalidChecksumStorages = new ArrayList<>();

        //if the first storage retrieved object but it was corrupted, add the storage to those which will be recovered
        if (problemWasInvalidChecksum) {
            invalidChecksumStorages.add(storageServices.get(0));
        }

        ObjectRetrievalResource objectRetrievalResource = null;
        StorageService successfulService = null;
        //iterate over all the storages to find an uncorrupted version of the object
        for (int i = 1; i < storageServices.size(); i++) {
            try {
                objectRetrievalResource = retrieveObjectFromStorage(objectDto, storageServices.get(i));
                if (objectRetrievalResource != null) {
                    successfulService = storageServices.get(i);
                    break;
                }
                invalidChecksumStorages.add(storageServices.get(i));
            } catch (StorageException e) {
                //try other storages when the current storage has failed
                log.error("Storage error has occurred during retrieval process of object: " + objectDto.getStorageId());
            }
        }
        if (objectRetrievalResource == null) {
            log.error("Object: " + objectDto.getStorageId() + " has failed to be recovered from any storage service.");
            storageServices.removeAll(invalidChecksumStorages);
            arcstorageMailCenter.sendObjectRetrievalError(objectDto, null, servicesToEntities(storageServices),
                    servicesToEntities(invalidChecksumStorages), null);
            throw new ObjectCouldNotBeRetrievedException(objectDto);
        }

        log.info("object " + objectDto.getStorageId() + " has been successfully retrieved");
        List<StorageService> recoveredStorages = new ArrayList<>();
        for (StorageService storageService : invalidChecksumStorages) {
            boolean success = recoverSingleObject(storageService, objectDto, objectRetrievalResource.getId());
            if (success)
                recoveredStorages.add(storageService);
        }
        storageServices.removeAll(invalidChecksumStorages);
        arcstorageMailCenter.sendObjectRetrievalError(objectDto, successfulService.getStorage(), servicesToEntities(storageServices), servicesToEntities(invalidChecksumStorages), servicesToEntities(recoveredStorages));
        return objectRetrievalResource;
    }

    /**
     * Synchronously saves archival object to the provided storage services.
     * <p>
     * If the storage process succeeds at every storage service, the object in DB changes state to ARCHIVED.
     * In case of an archival storage error at any of the storages, the storage process is rolled back.
     * If the rollback succeeds, object in DB is set to ROLLED_BACK.
     * If the rollback fails at any of the storages, object in DB is set to ARCHIVAL FAILURE.
     *
     * @param archivalObject  archival object to store
     * @param tmpSourceHolder source holder storing the object content
     * @param storageServices storage services to store to
     */
    private void saveObjectSynchronously(ArchivalObjectDto archivalObject, TmpSourceHolder tmpSourceHolder, List<StorageService> storageServices) {
        String op = "synchronously storing object: ";
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        AtomicBoolean rollback = new AtomicBoolean(false);
        for (StorageService a : storageServices) {
            CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                        try (InputStream objectStream = tmpSourceHolder.createInputStream()) {
                            ArchivalObjectDto archivalObjectCpy = new ArchivalObjectDto(archivalObject, objectStream);
                            a.storeObject(archivalObjectCpy, rollback, archivalObject.getOwner().getDataSpace());
                            log.info(strSX(a.getStorage().getName(), archivalObject.getStorageId()) + op + "success");
                        } catch (StorageException e) {
                            log.warn(strSX(a.getStorage().getName(), archivalObject.getStorageId()) + op + "error");
                            throw new GeneralException(e);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }, executor
            );
            futures.add(c);
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
        } catch (InterruptedException e) {
            String s = op + "main thread has been interrupted";
            log.error(s);
            throw new GeneralException(s, e);
        } catch (ExecutionException e) {
            rollback.set(true);
            log.error(op + "some storage has encountered problem");
        } finally {
            tmpSourceHolder.freeSpace();
        }

        if (!rollback.get()) {
            archivalDbService.setObjectState(archivalObject.getDatabaseId(), ObjectState.ARCHIVED);
            log.info(strX(archivalObject.getStorageId()) + op + "success on all storages");
            return;
        }
        log.info(op + "Archival storage error. Starting rollback.");
        futures = new ArrayList<>();
        for (StorageService a : storageServices) {
            CompletableFuture<Void> c = CompletableFuture.runAsync(() -> {
                try {
                    a.rollbackObject(archivalObject.getStorageId(), archivalObject.getOwner().getDataSpace());
                    log.warn(strSX(a.getStorage().getName(), archivalObject.getStorageId()) + "rolled back");
                } catch (StorageException e) {
                    log.error(strSX(a.getStorage().getName(), archivalObject.getStorageId()) + "rollback process error");
                    throw new GeneralException(e);
                }
            }, executor);
            futures.add(c);
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
            archivalDbService.setObjectState(archivalObject.getDatabaseId(), ObjectState.ROLLED_BACK);
            log.info(strX(archivalObject.getStorageId()) + "rollback successful on all storages.");
            throw new GeneralException("XML update failure, current state: " + ObjectState.ROLLED_BACK);
        } catch (InterruptedException e) {
            archivalDbService.setObjectState(archivalObject.getDatabaseId(), ObjectState.ARCHIVAL_FAILURE);
            log.error("Main thread has been interrupted during rollback.");
            throw new GeneralException("XML update failure, current state: " + ObjectState.ARCHIVAL_FAILURE);
        } catch (ExecutionException e) {
            archivalDbService.setObjectState(archivalObject.getDatabaseId(), ObjectState.ARCHIVAL_FAILURE);
            log.error(strX(archivalObject.getStorageId()) + "rollback failed on some storages.");
            throw new GeneralException("XML update failure, current state: " + ObjectState.ARCHIVAL_FAILURE);
        }
    }

    /**
     * Recovers object at the provided archival storage.
     *
     * @param storageService storage service to recover at
     * @param objectDto      object to be recovered
     * @param tmpFileId      id of the file storing the object content at the temporary storage
     * @return <code>true</code> if the recovery was successful, <code>false</code> otherwise
     */
    private boolean recoverSingleObject(StorageService storageService, ArchivalObjectDto objectDto, String tmpFileId) {
        log.info("Recovering object " + objectDto.getStorageId() + " at storage " + storageService.getStorage().getName() + ".");
        try (FileInputStream objectInputStream = new FileInputStream(tmpFolder.resolve(tmpFileId).toFile())) {
            objectDto.setInputStream(objectInputStream);
            storageService.storeObject(objectDto, new AtomicBoolean(false), objectDto.getOwner().getDataSpace());
            log.info("Object " + objectDto.getStorageId() + " has been successfully recovered at storage " +
                    storageService.getStorage().getName() + ".");
        } catch (StorageException e) {
            log.error("Object " + objectDto.getStorageId() + " has failed to be recovered at storage " +
                    storageService.getStorage().getName() + ".");
            return false;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read tmp file " + tmpFileId, e);
        }
        return true;
    }

    /**
     * Copies object to temporary folder and verifies checksum.
     *
     * @param objectIs       input stream with the object
     * @param checksum       checksum of the object
     * @param tmpFile        file to copy to
     * @param objectLogId    object id (used in the log message)
     * @param storageLogName storage name (used in the log message)
     * @return <code>true</code> in case of success, <code>false</code> otherwise
     */
    private boolean copyObjectToTmpFolderAndVerifyChecksum(InputStream objectIs, Checksum checksum, File tmpFile,
                                                           String objectLogId, String storageLogName) {
        try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
            Checksum objectStorageChecksum = copyStreamAndComputeChecksum(objectIs, fos, checksum.getType());
            if (!checksum.equals(objectStorageChecksum)) {
                log.error("Checksum for object " + objectLogId + " is invalid at storage " + storageLogName +
                        ". Expected checksum: " + checksum + " computed checksum: " + objectStorageChecksum);
                tmpFile.delete();
                return false;
            }
            return true;
        } catch (IOException e) {
            if (tmpFile.exists())
                tmpFile.delete();
            throw new UncheckedIOException(
                    "Error while creating or writing to file at " + tmpFile, e);
        }
    }

    /**
     * Cleans up the storage.
     * <li>Rollbacks files which are in {@link ObjectState#ARCHIVAL_FAILURE} state.</li>
     * <li>if {@param cleanAlsoProcessing} is set to true, rollbacks also files which are in
     * {@link ObjectState#PROCESSING}/{@link ObjectState#PRE_PROCESSING} state.</li>
     * <li>Deletes files which are in {@link ObjectState#DELETION_FAILURE} state.</li>
     *
     * @param cleanAlsoProcessing whether objects with state {@link ObjectState#PROCESSING}/{@link ObjectState#PRE_PROCESSING}
     *                            should be rolled back too..
     * @return list of objects for clean up
     * @throws SomeLogicalStoragesNotReachableException if any storage is unreachable before the process starts
     * @throws NoLogicalStorageAttachedException        if no logical storage is attached
     */
    public List<ArchivalObject> cleanup(boolean cleanAlsoProcessing) throws SomeLogicalStoragesNotReachableException,
            NoLogicalStorageAttachedException, ReadOnlyStateException {
        List<StorageService> storageServices = storageProvider.createAdaptersForWriteOperation();
        List<ArchivalObject> objectsForCleanup = archivalDbService.findObjectsForCleanup(cleanAlsoProcessing);
        async.cleanUp(objectsForCleanup, storageServices);
        return objectsForCleanup;
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
    public void setArcstorageMailCenter(ArcstorageMailCenter arcstorageMailCenter) {
        this.arcstorageMailCenter = arcstorageMailCenter;
    }

    @Inject
    public void setTmpFolder(@Value("${arcstorage.tmp-folder}") String path) {
        this.tmpFolder = Paths.get(path);
    }

    @Resource(name = "ReservedExecutorService")
    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }
}
