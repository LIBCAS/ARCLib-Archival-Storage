package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.*;
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
import cz.cas.lib.arcstorage.storagesync.exception.SynchronizationInProgressException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static cz.cas.lib.arcstorage.storage.StorageUtils.validateChecksum;
import static cz.cas.lib.arcstorage.util.Utils.*;

/**
 * Service which provides specific methods {@link AipSip} and {@link AipXml}
 */
@Service
@Slf4j
public class AipService {

    private ArchivalAsyncService async;
    private ArchivalDbService archivalDbService;
    private StorageProvider storageProvider;
    private Path tmpFolder;
    private ExecutorService executorService;
    private ArcstorageMailCenter arcstorageMailCenter;
    private ArchivalService archivalService;

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
        log.debug("Retrieving AIP with id " + sipId + ".");

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
            case ROLLBACK_FAILURE:
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
     * @param sipId        id of the AIP to retrieve
     * @param filePaths    paths of specified files we want to extract from ZIP
     * @param outputStream output stream into which result zip is stored
     * @throws DeletedStateException              if SIP is deleted {@link ObjectState}
     * @throws RollbackStateException             if SIP is rolled back or only one XML is requested and that one is rolled back {@link ObjectState}
     * @throws StillProcessingStateException      if SIP or some of requested XML is still processing {@link ObjectState}
     * @throws ObjectCouldNotBeRetrievedException if SIP is corrupted at all reachable storages {@link ObjectState}
     * @throws FailedStateException               if SIP is failed {@link ObjectState}
     * @throws RemovedStateException              if object has been logically removed: it exists in storage but should not be accessible to all users, used only for SIP (XMLs cant be removed), stored also at storage layer {@link ObjectState}
     * @throws NoLogicalStorageAttachedException  if storageId is null and there is not even one logical storage attached
     * @throws NoLogicalStorageReachableException if storageId is null and there is not even one logical storage reachable
     * @throws IOException                        if there were an IO exception during processing
     */
    public void getAipSpecifiedFiles(String sipId, Set<String> filePaths, OutputStream outputStream) throws ObjectCouldNotBeRetrievedException, RemovedStateException, FailedStateException, DeletedStateException, RollbackStateException, NoLogicalStorageAttachedException, NoLogicalStorageReachableException, StillProcessingStateException, IOException {
        AipRetrievalResource aip = getAip(sipId, false);

        ZipOutputStream zipOut = new ZipOutputStream(outputStream);

        // we need to work with input stream as with ZipInputStream to be able process it correctly
        try (ZipInputStream zipInputStream = new ZipInputStream(aip.getSip())) {
            ZipEntry zipEntry = zipInputStream.getNextEntry();

            while (zipEntry != null) {
                // check if zip entry is file we want
                if (filePaths.contains(zipEntry.getName())) {
                    zipOut.putNextEntry(new ZipEntry(sipId + "/" + zipEntry.getName()));
                    IOUtils.copyLarge(zipInputStream, zipOut);
                    zipOut.closeEntry();
                }

                zipEntry = getNextZipEntry(zipInputStream);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            zipOut.finish();
        }
    }

    /**
     * Small private method to handle
     *
     * @param aipDataZipIn
     * @return
     * @throws IOException
     */
    private ZipEntry getNextZipEntry(ZipInputStream aipDataZipIn) throws IOException {
        aipDataZipIn.closeEntry();
        return aipDataZipIn.getNextEntry();
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
        log.debug("Retrieving XML of AIP with id " + sipId + ".");

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
        return Pair.of(requestedXml.getVersion(), archivalService.getObject(requestedXml.toDto()));
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
        log.debug("Saving AIP with id " + aip.getSip().getStorageId());
        Path tmpSipPath = null;
        Path tmpXmlPath = null;
        AipSip aipSip;
        List<StorageService> reachableAdapters;

        try (BufferedInputStream sipIs = new BufferedInputStream(aip.getSip().getInputStream());
             BufferedInputStream xmlIs = new BufferedInputStream(aip.getXml().getInputStream())) {
            Pair<AipSip, Boolean> registrationResult = archivalDbService.registerAipCreation(aip.getSip().getDatabaseId(), aip.getSip().getChecksum(), aip.getXml().getChecksum(), aip.getSip().getCreated());
            aipSip = registrationResult.getLeft();
            aip.getXml().setDatabaseId(aipSip.getLatestXml().getId());
            try {
                if (registrationResult.getRight())
                    reachableAdapters = storageProvider.createAdaptersForModifyOperation();
                else
                    reachableAdapters = storageProvider.createAdaptersForWriteOperation();
                //validate checksum of XML
                tmpXmlPath = tmpFolder.resolve(aip.getXml().getDatabaseId());
                Files.copy(xmlIs, tmpXmlPath, StandardCopyOption.REPLACE_EXISTING);
                log.debug("XML content of AIP with id " + aip.getSip().getStorageId() + " has been stored to temporary storage.");
                validateChecksum(aip.getXml().getChecksum(), tmpXmlPath);
                log.debug("Checksum of XML of AIP with id " + aip.getSip().getStorageId() + " has been validated.");
                //copy SIP to tmp file and validate its checksum
                tmpSipPath = tmpFolder.resolve(aip.getSip().getDatabaseId());
                Files.copy(sipIs, tmpSipPath, StandardCopyOption.REPLACE_EXISTING);
                log.debug("SIP content of AIP with id " + aip.getSip().getStorageId() + " has been stored to temporary storage.");
                validateChecksum(aip.getSip().getChecksum(), tmpSipPath);
                log.debug("Checksum of SIP of AIP with id " + aip.getSip().getStorageId() + " has been validated.");
            } catch (Exception e) {
                archivalDbService.setObjectsState(ObjectState.ARCHIVAL_FAILURE, aip.getSip().getDatabaseId(), aip.getXml().getDatabaseId());
                if (tmpXmlPath != null)
                    tmpXmlPath.toFile().delete();
                if (tmpSipPath != null)
                    tmpSipPath.toFile().delete();
                throw e;
            }
        }

        aip.getSip().setState(ObjectState.PROCESSING);
        aip.getXml().setState(ObjectState.PROCESSING);

        archivalDbService.setObjectsState(ObjectState.PROCESSING, aip.getSip().getDatabaseId(), aip.getXml().getDatabaseId());
        async.saveAip(aip, new TmpFileHolder(tmpSipPath.toFile()), new TmpFileHolder(tmpXmlPath.toFile()), reachableAdapters, aipSip.getOwner().getDataSpace());
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
    public void saveXml(String sipId, InputStream xml, Checksum checksum, Integer version, boolean sync)
            throws SomeLogicalStoragesNotReachableException, IOException, NoLogicalStorageAttachedException,
            DeletedStateException, FailedStateException, RollbackStateException, StillProcessingStateException,
            BadXmlVersionProvidedException, ReadOnlyStateException {
        String logPrefix = sync ? "Synchronously" : "Asynchronously";
        log.debug(logPrefix + " saving XML in version " + version + " of AIP with id " + sipId + ".");
        List<StorageService> reachableAdapters;
        AipXml xmlEntity;
        Path tmpXmlPath = null;

        try (BufferedInputStream xmlIs = new BufferedInputStream(xml)) {
            Pair<AipXml, Boolean> registrationResult = archivalDbService.registerXmlUpdate(sipId, checksum, version);
            xmlEntity = registrationResult.getLeft();
            try {
                if (registrationResult.getRight())
                    reachableAdapters = storageProvider.createAdaptersForModifyOperation();
                else
                    reachableAdapters = storageProvider.createAdaptersForWriteOperation();
                tmpXmlPath = tmpFolder.resolve(xmlEntity.getId());
                Files.copy(xmlIs, tmpXmlPath, StandardCopyOption.REPLACE_EXISTING);
                log.debug("XML content of AIP with id " + sipId + " has been stored to temporary storage.");
                validateChecksum(xmlEntity.getChecksum(), tmpXmlPath);
                log.debug("Checksum of XML in version " + version + " of AIP with id " + sipId + " has been validated.");
            } catch (Exception e) {
                archivalDbService.setObjectsState(ObjectState.ARCHIVAL_FAILURE, xmlEntity.getId());
                if (tmpXmlPath != null)
                    tmpXmlPath.toFile().delete();
                throw e;
            }
        }

        xmlEntity.setState(ObjectState.PROCESSING);
        archivalDbService.saveObject(xmlEntity);
        log.debug("State of object with id " + xmlEntity.getId() + " changed to " + ObjectState.PROCESSING);
        ArchivalObjectDto objectDto = xmlEntity.toDto();
        objectDto.setInputStream(xml);
        async.saveObject(objectDto, new TmpFileHolder(tmpXmlPath.toFile()), reachableAdapters, sync);
    }

    /**
     * rolls back latest XML version of AIP XML
     *
     * @param aipId
     */
    public void rollbackXml(String aipId, int xmlVersion) throws NoLogicalStorageAttachedException, SomeLogicalStoragesNotReachableException {
        AipSip sip = archivalDbService.findSip(aipId);
        if (sip == null) {
            log.debug("Skipped rollback of AIP XML of AIP: " + aipId + " because no such AIP is stored in DB");
            return;
        }
        AipXml latestXml = sip.getLatestXml();
        if (latestXml.getVersion() < xmlVersion) {
            log.debug("Skipped rollback of AIP XML of AIP: " + aipId + " because the specified AIP XML version: " + xmlVersion + " is not found in DB");
            return;
        }
        if (latestXml.getVersion() > xmlVersion) {
            throw new UnsupportedOperationException(
                    "Can't rollback XML of AIP:" + aipId + " because the specified version: " + xmlVersion + " is not the latest version registered in Archival Storage DB," +
                            "the latest version in Archival Storage DB: " + latestXml.getVersion());
        }
        archivalService.rollbackObject(latestXml);
    }

    /**
     * Retrieves state of the AIP in Database
     *
     * @param aipId id of the AIP
     * @return state of the AIP
     */
    public ObjectState getAipState(String aipId) {
        log.debug("Getting AIP state of AIP with id " + aipId + ".");
        AipSip aip = archivalDbService.getAip(aipId);
        return aip.getState();
    }

    /**
     * Retrieves state of the XML in Database
     *
     * @param aipId id of the AIP
     * @return state of the XML
     */
    public ObjectState getXmlState(String aipId, int xmlVersion) {
        log.debug("Getting AIP state of AIP with id " + aipId + ".");
        AipXml xml = archivalDbService.getXml(aipId, xmlVersion);
        return xml.getState();
    }

    /**
     * Verifies state of specified AIPs at specified storage (or all storages if storageId is null). The method has synchronous and asynchronous part.
     * <ol>
     * <li>First list of storages at which the AIPs should be verified is created.
     * If the <b>storageId parameter is specified</b> the storage is checked for reachability and if not reachable
     * (or is just synchronizing) exception is thrown. Otherwise the storage is added to the list.
     * If the <b>storageId parameter is null</b>, then the the list of storages which are reachable and not synchronizing
     * is created and verification is done at every such storage. If there is no such storage, exception is thrown.
     * </li>
     * <li>Then at every storage from the list all AIPs which are in state with property {@link ObjectState#metadataMustBeStoredAtLogicalStorage()} = true,
     * are verified. If there is either metadata or data inconsistency between the AIP at storage and in DB, AIP recovery process starts in other thread (asynchronous)
     * and the DTO with inconsistency information is added to the result list. Objects which has property {@link ObjectState#metadataMustBeStoredAtLogicalStorage()} = false
     * are neither verified at storage nor recovered from other storage and the DTO with inconsistency information is added to the result list immediately, also mail with
     * list of such objects (which are recommended for cleanup) is sent to all users with admin role.
     * </li>
     * <li>Synchronous part of the method ends and result list with consistency/inconsistency information is returned.
     * </li>
     * <li>Asynchronous threads continues with AIP recoveries and at the end the mail with information about inconsistencies is
     * sent to all users with admin role.
     * </li>
     * </ol>
     *
     * @param aipSips   list of AIPs which state should be verified, ordered by creation time (ascending)
     * @param storageId id of storage at which aip state should be verified, if null is set then verification will be done at all reachable stores
     * @return states of all objects of reachable storage service/services
     * @throws NoLogicalStorageAttachedException        if storageId is null and there is not even one logical storage attached
     * @throws NoLogicalStorageReachableException       if storageId is null and there is not even one logical storage reachable
     * @throws SomeLogicalStoragesNotReachableException if storageId is specified and the storage is not reachable
     * @throws SynchronizationInProgressException       if storageId is specified and the storage is just synchronizing
     * @throws IllegalArgumentException                 if aipSips list is null or empty
     */
    public List<AipConsistencyVerificationResultDto> verifyAipsAtStorage(List<AipSip> aipSips, String storageId) throws NoLogicalStorageAttachedException, NoLogicalStorageReachableException, SomeLogicalStoragesNotReachableException, SynchronizationInProgressException {
        List<StorageService> reachableStorages;
        notNull(aipSips, () -> new IllegalArgumentException("List of AIPs to verify can't be null"));
        if (aipSips.isEmpty())
            throw new IllegalArgumentException("List of AIPs to verify can't be empty");
        if (storageId == null) {
            reachableStorages = storageProvider.createAdaptersForRead();
        } else {
            StorageService adapter = storageProvider.createAdapter(storageId);
            Storage storage = adapter.getStorage();
            if (!storage.isReachable())
                throw new SomeLogicalStoragesNotReachableException(storage);
            if (storage.isSynchronizing())
                throw new SynchronizationInProgressException();
            reachableStorages = asList(adapter);
        }
        List<AipConsistencyVerificationResultDto> allImmediateResults = new ArrayList<>();
        List<CompletableFuture<RecoveryResultDto>> allRecoveryResults = new ArrayList<>();
        for (StorageService reachableStorage : reachableStorages) {
            for (AipSip aipSip : aipSips) {
                Pair<AipConsistencyVerificationResultDto, CompletableFuture<RecoveryResultDto>> result = verifyAipAtStorage(aipSip.getId(), reachableStorage);
                allImmediateResults.add(result.getLeft());
                if (result.getRight() != null)
                    allRecoveryResults.add(result.getRight());
            }
        }

        Map<String, ObjectConsistencyVerificationResultDto> candidatesForCleanup = new HashMap<>();
        for (AipConsistencyVerificationResultDto singleAipVerificationResult : allImmediateResults) {
            ObjectConsistencyVerificationResultDto aipVerificationResult = singleAipVerificationResult.getAipState();
            if (aipVerificationResult.considerCleanup())
                candidatesForCleanup.put(aipVerificationResult.getStorageId(), aipVerificationResult);
            for (XmlConsistencyVerificationResultDto xmlVerificationResult : singleAipVerificationResult.getXmlStates()) {
                if (xmlVerificationResult.considerCleanup())
                    candidatesForCleanup.put(xmlVerificationResult.getStorageId(), xmlVerificationResult);
            }
        }
        if (!candidatesForCleanup.isEmpty())
            arcstorageMailCenter.sendCleanupRecommendation(candidatesForCleanup);

        Map<String, RecoveryResultDto> recoveryResultsGroupedByStorages = new HashMap<>();
        CompletableFuture.allOf(allRecoveryResults.toArray(new CompletableFuture[0]))
                .whenComplete((a, b) -> {
                    for (CompletableFuture<RecoveryResultDto> recRes : allRecoveryResults) {
                        if (!recRes.isCompletedExceptionally()) {
                            RecoveryResultDto result = recRes.join();
                            RecoveryResultDto fromMap = recoveryResultsGroupedByStorages.get(result.getStorageId());
                            if (fromMap != null)
                                fromMap.merge(result);
                            else
                                recoveryResultsGroupedByStorages.put(result.getStorageId(), result);
                        }
                    }
                    arcstorageMailCenter.sendAipsVerificationError(recoveryResultsGroupedByStorages);
                });
        return allImmediateResults;
    }

    /**
     * Verifies AIP at single storage and if data or metadata are inconsistent between storage and DB
     * starts recovery process - tries to obtain object from other storage and recover the copy at failing storage.
     * <p>
     * If the AIP is in state with {@link ObjectState#metadataMustBeStoredAtLogicalStorage()} false, then no verification
     * at storage is done and no recovery is started. These objects should be cleaned up using {@link ArchivalAsyncService#cleanUp(List, List)}.
     * The same applies to all AIP XMLs of the AIP.
     * </p>
     *
     * @param sipId          id of AIP
     * @param storageService storage service to use
     * @return Pair holding information about current state at of AIP at storage and Future holding result of recovery.. if no
     * no recovery has started, null is returned instead of the Future
     */
    private Pair<AipConsistencyVerificationResultDto, CompletableFuture<RecoveryResultDto>> verifyAipAtStorage(String sipId, StorageService storageService) {
        String storageId = storageService.getStorage().getId();
        log.debug("Verifying AIP with id " + sipId + " at storage with id " + storageId + ".");
        AipSip aip = archivalDbService.getAip(sipId);

        if (!aip.getState().metadataMustBeStoredAtLogicalStorage()) {
            AipConsistencyVerificationResultDto incompleteStateInfo = new AipConsistencyVerificationResultDto(storageService.getStorage().getName(),
                    storageService.getStorage().getStorageType(),
                    storageService.getStorage().isReachable());
            ObjectConsistencyVerificationResultDto aipRes = new ObjectConsistencyVerificationResultDto(aip.getId(), aip.getId(), aip.getState(), false, false, null, aip.getChecksum(), aip.getCreated());
            if (aipRes.considerCleanup())
                log.warn("AIP " + sipId + " is in error state or processing for too long. AIP was created at: " + aip.getCreated() + ", current state is: " + aip.getState() + ". Consider cleanup.");
            else
                log.debug("AIP " + sipId + " is still processing.");
            incompleteStateInfo.setAipState(aipRes);
            aip.getXmls().forEach(x -> {
                XmlConsistencyVerificationResultDto xmlInfo = new XmlConsistencyVerificationResultDto(x.getId(), toXmlId(aip.getId(), x.getVersion()), x.getState(), false, false, null, x.getChecksum(), x.getCreated(), x.getVersion());
                incompleteStateInfo.addXmlInfo(xmlInfo);
            });
            return Pair.of(incompleteStateInfo, null);
        }

        Map<Integer, ArchivalObjectDto> xmlsToCheck = new HashMap<>();
        List<AipXml> xmlsWhichCantBeChecked = new ArrayList<>();
        for (AipXml xml : aip.getXmls()) {
            if (xml.getState().metadataMustBeStoredAtLogicalStorage())
                xmlsToCheck.put(xml.getVersion(), xml.toDto());
            else
                xmlsWhichCantBeChecked.add(xml);
        }
        AipConsistencyVerificationResultDto result;
        try {
            result = storageService.getAipInfo(aip.toDto(), xmlsToCheck, aip.getOwner().getDataSpace());
        } catch (StorageException e) {
            AipConsistencyVerificationResultDto incompleteStateInfo = new AipConsistencyVerificationResultDto(storageService.getStorage().getName(),
                    storageService.getStorage().getStorageType(),
                    storageService.getStorage().isReachable());
            incompleteStateInfo.setAipState(new ObjectConsistencyVerificationResultDto(aip.getId(), aip.getId(), aip.getState(), false, false, null, aip.getChecksum(), aip.getCreated()));
            aip.getXmls().forEach(x -> {
                XmlConsistencyVerificationResultDto xmlInfo = new XmlConsistencyVerificationResultDto(x.getId(), toXmlId(aip.getId(), x.getVersion()), x.getState(), false, false, null, x.getChecksum(), x.getCreated(), x.getVersion());
                incompleteStateInfo.addXmlInfo(xmlInfo);
            });
            result = incompleteStateInfo;
        }
        List<ObjectConsistencyVerificationResultDto> checkedObjects = new ArrayList<>();
        checkedObjects.add(result.getAipState());
        checkedObjects.addAll(result.getXmlStates());
        boolean allCheckedAreOk = checkedObjects.stream().allMatch(
                o -> o.isMetadataConsistent() && (!o.getState().contentMustBeStoredAtLogicalStorage() || o.isContentConsistent()));

        if (xmlsWhichCantBeChecked.size() > 1)
            log.error("FATAL ERROR: found more than one XMLs of AIP " + aip.getId() + " in error or processing state.. XMLs: " + Arrays.toString(xmlsWhichCantBeChecked.toArray(new AipXml[0])));
        for (AipXml x : xmlsWhichCantBeChecked) {
            XmlConsistencyVerificationResultDto uncheckedXml = new XmlConsistencyVerificationResultDto(x.getId(), toXmlId(aip.getId(), x.getVersion()), x.getState(), false, false, null, x.getChecksum(), x.getCreated(), x.getVersion());
            if (uncheckedXml.considerCleanup())
                log.warn("XML " + x.getId() + " of AIP " + sipId + " is in error state or processing for too long. XML was created at: " + x.getCreated() + ", current state is: " + x.getState() + ". Consider cleanup.");
            else
                log.debug("XML " + x.getId() + " of AIP " + sipId + " is still processing.");
            result.addXmlInfo(uncheckedXml);
        }

        if (allCheckedAreOk) {
            if (xmlsWhichCantBeChecked.isEmpty())
                log.debug("Successfully verified AIP with id " + sipId + " at storage with id " + storageId + ". All objects were consistent.");
            else
                log.debug("All other objects of AIP: " + sipId + " were successfully verified at storage: " + storageId + " and are consistent.");
            return Pair.of(result, null);
        }

        CompletableFuture<RecoveryResultDto> recResFuture = CompletableFuture.supplyAsync(() -> {
            log.error("Some inconsistent objects found during verification of AIP with id " + sipId + " at storage with id " + storageId + ". Starting recovery process.");
            RecoveryResultDto recResDto = new RecoveryResultDto(storageId);

            for (ObjectConsistencyVerificationResultDto checkedObject : checkedObjects) {
                if (checkedObject.getState().contentMustBeStoredAtLogicalStorage() && !checkedObject.isContentConsistent()) {
                    recResDto.getContentInconsistencyObjectsIds().add(checkedObject.getStorageId());
                    String recoveryMsg = "Recovery of content of object: " + checkedObject.getStorageId() + " has ";
                    ArchivalObject object = archivalDbService.getObject(checkedObject.getDatabaseId());
                    ObjectRetrievalResource objectRetrievalResource;
                    try {
                        objectRetrievalResource = archivalService.retrieveObject(object.toDto(), asList(storageService));
                        ArchivalObjectDto archivalObjectDto = null;
                        try {
                            archivalObjectDto = new ArchivalObjectDto(object.toDto(), objectRetrievalResource.getInputStream());
                            storageService.storeObject(archivalObjectDto, new AtomicBoolean(false), archivalObjectDto.getOwner().getDataSpace());
                            log.debug(recoveryMsg + "succeeded");
                            recResDto.getContentRecoveredObjectsIds().add(checkedObject.getStorageId());
                        } finally {
                            if (archivalObjectDto != null)
                                IOUtils.closeQuietly(archivalObjectDto.getInputStream());
                        }
                    } catch (Exception e) {
                        log.debug(e.toString());
                        log.warn(recoveryMsg + "failed");
                    }
                } else if (!checkedObject.isMetadataConsistent()) {
                    recResDto.getMetadataInconsistencyObjectsIds().add(checkedObject.getStorageId());
                    String recoveryMsg = "Recovery of metadata of object: " + checkedObject.getStorageId() + " has ";
                    ArchivalObject object = archivalDbService.getObject(checkedObject.getDatabaseId());
                    try {
                        storageService.storeObjectMetadata(object.toDto(), object.getOwner().getDataSpace());
                        log.debug(recoveryMsg + "succeeded");
                        recResDto.getMetadataRecoveredObjectsIds().add(checkedObject.getStorageId());
                    } catch (StorageException e) {
                        log.debug(e.toString());
                        log.warn(recoveryMsg + "failed");
                    }
                }
            }
            return recResDto;
        }, executorService);
        return Pair.of(result, recResFuture);
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
        log.debug("Retrieving AIP with id " + sipEntity.getId() + ".");

        List<StorageService> storageServicesByPriorities = storageProvider.createAdaptersForRead();

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
        log.debug("Storage: " + storageName + " chosen to retrieve AIP: " + sipEntity.getId());

        AipRetrievalResource aipFromStorage = storageService.getAip(sipEntity.getId(), sipEntity.getOwner().getDataSpace(), xmls.stream()
                .map(AipXml::getVersion)
                .collect(Collectors.toList())
                .toArray(new Integer[xmls.size()]));
        String tmpSipFileId = aipFromStorage.getId();
        File tmpSipFile = tmpFolder.resolve(tmpSipFileId).toFile();

        AipRetrievalResult result = new AipRetrievalResult(aipFromStorage, storageService);

        boolean sipValid = archivalService.copyObjectToTmpFolderAndVerifyChecksum(sipEntity.getId(), aipFromStorage.getSip(), sipEntity.getChecksum(),
                tmpSipFile, storageName);
        if (!sipValid) {
            log.debug("Invalid checksum of SIP with id " + sipEntity.getId() + " at storage " + storageService.getStorage().getName() + ".");
            result.setInvalidChecksumSip(sipEntity);
            result.setInvalidChecksumFound(true);
        }
        //reassigning the dto with the input stream
        else {
            log.debug("Validated checksum of SIP with id " + sipEntity.getId() + " retrieved from storage " +
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
            boolean xmlValid = archivalService.copyObjectToTmpFolderAndVerifyChecksum(xmlEntity.getId(), aipFromStorage.getXmls().get(xmlEntity.getVersion()),
                    xmlEntity.getChecksum(), tmpXmlFile, storageName);

            if (!xmlValid) {
                result.addInvalidChecksumXml(xmlEntity);
                result.setInvalidChecksumFound(true);
            }
            //reassigning the dto with the input stream
            else {
                log.debug("Validated checksum of XML with id " + xmlEntity.getId() + " of AIP with id " + sipEntity.getId() +
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
        log.debug("Recovering AIP: " + sipEntity.getId() + " from other storages.");
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
        if (result == null)
            result = latestInvalidChecksumResult;
        List<StorageService> invalidChecksumStorages = invalidChecksumResults.stream()
                .map(AipRetrievalResult::getStorageService)
                .collect(Collectors.toList());

        if (successfulService == null) {
            log.error("AIP: " + sipEntity.getId() + " has failed to be recovered from any storage service.");
            storageServices.removeAll(invalidChecksumStorages);
            arcstorageMailCenter.sendObjectRetrievalError(sipEntity.toDto(), null, servicesToEntities(storageServices), servicesToEntities(invalidChecksumStorages), null);
            throw new ObjectCouldNotBeRetrievedException(result.getInvalidChecksumSip(), result.getInvalidChecksumXmls());
        }

        log.debug("AIP " + sipEntity.getId() + " has been successfully retrieved.");
        List<StorageService> recoveredStorages = new ArrayList<>();
        for (AipRetrievalResult invalidChecksumResult : invalidChecksumResults) {
            StorageService usedStorageService = invalidChecksumResult.storageService;
            boolean success = true;
            //repair sip at the storage
            if (invalidChecksumResult.invalidChecksumSip != null) {
                success = archivalService.recoverSingleObject(usedStorageService, sipEntity.toDto(), result.getAipFromStorage().getId());
            }
            //repair XMLs at the storage
            for (AipXml xml : invalidChecksumResult.invalidChecksumXmls) {
                ArchivalObjectDto xmlDto = xml.toDto();
                success = success && archivalService.recoverSingleObject(usedStorageService, xmlDto, xmlDto.getStorageId());
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
     * private DTO for files returned from storage services together with information whether they are OK or corrupted
     */
    @Getter
    @Setter
    private class AipRetrievalResult {
        private AipRetrievalResource aipFromStorage;
        private StorageService storageService;
        private boolean invalidChecksumFound = false;

        private AipSip invalidChecksumSip = null;
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
    public void setTmpFolder(@Value("${arcstorage.tmpFolder}") String path) {
        this.tmpFolder = Paths.get(path);
    }

    @Inject
    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Inject
    public void setArchivalService(ArchivalService archivalService) {
        this.archivalService = archivalService;
    }
}
