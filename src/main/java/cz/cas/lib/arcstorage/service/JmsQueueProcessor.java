package cz.cas.lib.arcstorage.service;


import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.jms.JmsSender;
import cz.cas.lib.arcstorage.jms.context.ProcessingObjectContext;
import cz.cas.lib.arcstorage.jms.context.StoragesContextRegistry;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.FileDoesNotExistException;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Slf4j
public class JmsQueueProcessor {

    private StorageProvider storageProvider;
    private FileLocationResolver fileLocationResolver;
    private ArchivalDbService archivalDbService;
    private JmsSender jmsSender;
    private StoragesContextRegistry storagesContextRegistry;

    public void saveAip(StorageService targetStorage, AipDto aip, String userId) throws StorageException, SomeLogicalStoragesNotReachableException {
        StorageService primaryStorage = storageProvider.createPrimaryStorageAdapter();
        if (targetStorage.getStorage().equals(primaryStorage.getStorage())) {
            saveAipAtPrimary(targetStorage, aip, userId);
        } else {
            saveAipAtSecondary(primaryStorage, targetStorage, aip);
        }
    }

    public void saveObject(StorageService targetStorage, ArchivalObjectDto dto, @NonNull Instant operationTimestamp, String userId) throws StorageException, SomeLogicalStoragesNotReachableException {
        StorageService primaryStorage = storageProvider.createPrimaryStorageAdapter();
        if (targetStorage.getStorage().equals(primaryStorage.getStorage())) {
            saveObjectAtPrimary(targetStorage, dto, operationTimestamp, userId, false);
        } else {
            saveObjectAtSecondary(primaryStorage, targetStorage, dto, operationTimestamp);
        }
    }

    public void delete(StorageService targetStorage, ArchivalObjectDto dto, @NonNull Instant operationTimestamp) throws StorageException {
        try {
            targetStorage.delete(dto, dto.getDataSpace(), operationTimestamp);
            log.debug("{}, {}, successfully deleted", targetStorage.getStorage(), dto);
        } catch (StorageException e) {
            log.error("{}, {}, deletion failed", targetStorage.getStorage(), dto, e);
            throw e;
        }
    }

    public void rollback(StorageService targetStorage, ArchivalObjectDto dto, @NonNull Instant operationTimestamp) throws StorageException {
        try {
            targetStorage.rollbackObject(dto, dto.getDataSpace(), operationTimestamp);
            log.debug("{}, {}, successfully rolled back", targetStorage.getStorage(), dto);
        } catch (StorageException e) {
            log.error("{}, {}, rollback failed", targetStorage.getStorage(), dto, e);
            throw e;
        }
    }

    public void remove(StorageService targetStorage, ArchivalObjectDto dto, @NonNull Instant operationTimestamp) throws StorageException {
        try {
            targetStorage.remove(dto, dto.getDataSpace(), operationTimestamp);
            log.debug("{}, {}, successfully removed", targetStorage.getStorage(), dto);
        } catch (StorageException e) {
            log.error("{}, {}, remove failed", targetStorage.getStorage(), dto, e);
            throw e;
        }
    }

    public void renew(StorageService targetStorage, ArchivalObjectDto dto, @NonNull Instant operationTimestamp) throws StorageException {
        try {
            targetStorage.renew(dto, dto.getDataSpace(), operationTimestamp);
            log.debug("{}, {}, successfully renewed", targetStorage.getStorage(), dto);
        } catch (StorageException e) {
            log.error("{}, {}, renew failed", targetStorage.getStorage(), dto, e);
            throw e;
        }
    }

    public void forget(StorageService targetStorage, ArchivalObjectDto dto, @NonNull Instant operationTimestamp) throws StorageException {
        try {
            targetStorage.forgetObject(dto.getStorageId(), dto.getDataSpace(), operationTimestamp);
            log.debug("{}, {}, successfully forgot", targetStorage.getStorage(), dto);
        } catch (StorageException e) {
            log.error("{}, {}, forget failed", targetStorage.getStorage(), dto, e);
            throw e;
        }
    }

    public void copy(StorageService targetStorage, ArchivalObjectDto object, @NonNull Instant operationTimestamp) throws StorageException, SomeLogicalStoragesNotReachableException {
        switch (object.getState()) {
            case DELETED:
            case DELETION_FAILURE:
                targetStorage.delete(object, object.getDataSpace(), operationTimestamp);
                break;
            case ROLLED_BACK:
            case ROLLBACK_FAILURE:
            case ARCHIVAL_FAILURE:
                targetStorage.rollbackObject(object, object.getDataSpace(), operationTimestamp);
            case ARCHIVED:
                StorageService primaryStorage = storageProvider.createPrimaryStorageAdapter();
                saveObjectAtSecondary(primaryStorage, targetStorage, object, operationTimestamp);
                break;
            case REMOVED:
                primaryStorage = storageProvider.createPrimaryStorageAdapter();
                saveObjectAtSecondary(primaryStorage, targetStorage, object, operationTimestamp);
                targetStorage.remove(object, object.getDataSpace(), operationTimestamp);
                break;
            case PRE_PROCESSING:
            case PROCESSING:
            case FORGOT: //forgotten objects are not event present in DB, this should not occur
            default:
                throw new IllegalArgumentException("can't copy object " + object.getStorageId() + " because it is in " + object.getState() + " state");
        }
    }

    public void activateDataspace(StorageService targetStorage, String dataSpace) throws IOStorageException {
        log.debug("Creating new data space: " + dataSpace + " at storage: " + targetStorage.getStorage().getName() + ".");
        targetStorage.createNewDataSpace(dataSpace);
        log.info("Date space: " + dataSpace + " successfully created at storage: " + targetStorage.getStorage().getName() + ".");
    }

    private void saveAipAtPrimary(StorageService primaryStorage, AipDto aip, String userId) {
        String xmlDbId = aip.getXml().getDatabaseId();
        String sipDbId = aip.getSip().getDatabaseId();

        TmpFileHolder tmpXml = new TmpFileHolder(fileLocationResolver.getFileTmpPath(xmlDbId).toFile());
        TmpFileHolder tmpSip = new TmpFileHolder(fileLocationResolver.getFileTmpPath(sipDbId).toFile());
        try (InputStream sipStream = tmpSip.createInputStream();
             InputStream xmlStream = tmpXml.createInputStream()) {
            ProcessingObjectContext objProcessingContext = storagesContextRegistry.getProcessingObjectContext(primaryStorage.getStorage().getId(), aip.getSip().getDatabaseId());
            AtomicBoolean rollbackSignal = objProcessingContext.getStopSignal();
            primaryStorage.storeAip(new AipDto(aip, sipStream, xmlStream), rollbackSignal, aip.getSip().getDataSpace());
            Set<Storage> secondaryStorages = storageProvider.getSecondaryStorages();
            if (rollbackSignal.get()) {
                log.debug("{} (main), {}, store process interrupted by rollback signal", primaryStorage.getStorage(), aip);
            } else {
                jmsSender.healthCheck();
                for (Storage s : secondaryStorages) {
                    jmsSender.saveAip(s.getId(), aip, userId);
                }
                log.debug("{} (main), {}, successfully stored", primaryStorage.getStorage(), aip);
                archivalDbService.setArchived(userId, aip.getSip(), aip.getXml());
            }
        } catch (IOException e) {
            log.error("{} (main), {}, storing failed ; Can't read SIP tmp file at path {} or XML tmp file at path {}", primaryStorage.getStorage(), aip, tmpSip, tmpXml, e);
            archivalDbService.setObjectsState(ObjectState.ARCHIVAL_FAILURE, sipDbId, xmlDbId);
        } catch (Exception e) {
            log.error("{} (main), {}, storing failed", primaryStorage.getStorage(), aip, e);
            archivalDbService.setObjectsState(ObjectState.ARCHIVAL_FAILURE, sipDbId, xmlDbId);
        } finally {
            tmpSip.freeSpace();
            tmpXml.freeSpace();
        }
    }

    private void saveAipAtSecondary(StorageService primaryStorage, StorageService targetStorage, AipDto aip) throws StorageException {
        try (AipRetrievalResource aipFromPrimary = primaryStorage.getAip(aip.getSip().getStorageId(), aip.getSip().getDataSpace(), 1)) {
            ProcessingObjectContext objProcessingContext = storagesContextRegistry.getProcessingObjectContext(targetStorage.getStorage().getId(), aip.getSip().getDatabaseId());
            AtomicBoolean rollbackSignal = objProcessingContext.getStopSignal();
            targetStorage.storeAip(new AipDto(aip, aipFromPrimary.getSip(), aipFromPrimary.getXmls().get(1)), rollbackSignal, aip.getXml().getDataSpace());
            if (rollbackSignal.get()) {
                log.debug("{} (secondary), {}, store process interrupted by rollback signal", targetStorage.getStorage(), aip);
            } else {
                log.debug("{} (secondary), {}, successfully stored", targetStorage.getStorage(), aip);
            }
        } catch (FileDoesNotExistException e) {
            log.info("{} (secondary), {}, storing skipped: The aip is no longer present in primary storage {}", targetStorage.getStorage(), aip, primaryStorage.getStorage());
        } catch (IOException e) {
            log.error("{} (secondary), {}, storing failed: Can't read SIP or XML from primary storage {}", targetStorage.getStorage(), aip, primaryStorage.getStorage());
            throw new UncheckedIOException(e);
        } catch (StorageException e) {
            log.error("{} (secondary), {}, storing failed", targetStorage.getStorage(), aip, e);
            throw e;
        }
    }

    /**
     * @param primaryStorage
     * @param dto
     * @param operationTimestamp
     * @param userId
     * @param restApiDirect      if called directly from rest API instead of from JMS.. in that case rollback is not supported
     */
    public void saveObjectAtPrimary(StorageService primaryStorage, ArchivalObjectDto dto, Instant operationTimestamp, String userId, boolean restApiDirect) {
        String objDbId = dto.getDatabaseId();
        TmpFileHolder tmpObject = new TmpFileHolder(fileLocationResolver.getFileTmpPath(objDbId).toFile());

        try (InputStream objectStream = tmpObject.createInputStream()) {
            AtomicBoolean rollbackSignal;
            if (restApiDirect) {
                rollbackSignal = new AtomicBoolean(false);
            } else {
                ProcessingObjectContext objProcessingContext = storagesContextRegistry.getProcessingObjectContext(primaryStorage.getStorage().getId(), dto.getDatabaseId());
                rollbackSignal = objProcessingContext.getStopSignal();
            }
            primaryStorage.storeObject(new ArchivalObjectDto(dto, objectStream), rollbackSignal, dto.getDataSpace(), operationTimestamp);
            Set<Storage> secondaryStorages = storageProvider.getSecondaryStorages();
            if (rollbackSignal.get()) {
                log.debug("{} (main), {}, store process interrupted by rollback signal", primaryStorage.getStorage(), dto);
            } else {
                jmsSender.healthCheck();
                for (Storage s : secondaryStorages) {
                    jmsSender.saveObject(s.getId(), dto, operationTimestamp, userId);
                }
                log.debug("{} (main), {}, successfully stored", primaryStorage.getStorage(), dto);
                archivalDbService.setArchived(userId, dto);
            }
        } catch (IOException e) {
            log.error("{} (main), {}, storing failed ; Can't read object at path {}", primaryStorage.getStorage(), dto, tmpObject, e);
            archivalDbService.setObjectsState(ObjectState.ARCHIVAL_FAILURE, objDbId);
            if (restApiDirect) {
                throw new UncheckedIOException(e);
            }
        } catch (Exception e) {
            log.error("{} (main), {}, storing failed", primaryStorage.getStorage(), dto, e);
            archivalDbService.setObjectsState(ObjectState.ARCHIVAL_FAILURE, objDbId);
            if (restApiDirect) {
                throw new RuntimeException(e);
            }
        } finally {
            tmpObject.freeSpace();
        }
    }

    private void saveObjectAtSecondary(StorageService primaryStorage, StorageService targetStorage, ArchivalObjectDto dto, @NonNull Instant operationTimestamp) throws StorageException {
        try (ObjectRetrievalResource objectFromPrimary = primaryStorage.getObject(dto.getStorageId(), dto.getDataSpace())) {
            ProcessingObjectContext objProcessingContext = storagesContextRegistry.getProcessingObjectContext(targetStorage.getStorage().getId(), dto.getDatabaseId());
            AtomicBoolean rollbackSignal = objProcessingContext.getStopSignal();
            targetStorage.storeObject(new ArchivalObjectDto(dto, objectFromPrimary.getInputStream()), rollbackSignal, dto.getDataSpace(), operationTimestamp);
            if (rollbackSignal.get()) {
                log.debug("{} (secondary), {}, store process interrupted by rollback signal", targetStorage.getStorage(), dto);
            } else {
                log.debug("{} (secondary), {}, successfully stored", targetStorage.getStorage(), dto);
            }
        } catch (FileDoesNotExistException e) {
            log.info("{} (secondary), {}, storing skipped: The object is no longer present in primary storage {}", targetStorage.getStorage(), dto, primaryStorage.getStorage());
        } catch (IOException e) {
            log.error("{} (secondary), {}, storing failed: Can't read object from primary storage {}", targetStorage.getStorage(), dto, primaryStorage.getStorage());
            throw new UncheckedIOException(e);
        } catch (StorageException e) {
            log.error("{} (secondary), {}, storing failed", targetStorage.getStorage(), dto, e);
            throw e;
        }
    }

    @Autowired
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    @Autowired
    public void setFileLocationResolver(FileLocationResolver fileLocationResolver) {
        this.fileLocationResolver = fileLocationResolver;
    }

    @Autowired
    public void setArchivalDbService(ArchivalDbService archivalDbService) {
        this.archivalDbService = archivalDbService;
    }

    @Autowired
    public void setJmsSender(JmsSender jmsSender) {
        this.jmsSender = jmsSender;
    }

    @Autowired
    public void setStoragesContextRegistry(StoragesContextRegistry storagesContextRegistry) {
        this.storagesContextRegistry = storagesContextRegistry;
    }
}
