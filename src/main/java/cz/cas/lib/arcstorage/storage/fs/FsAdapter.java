package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import org.springframework.lang.NonNull;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Adapter for {@link ZfsStorageService} and {@link FsStorageService} for more elegant sharing of code of the {@link LocalFsProcessor} and {@link RemoteFsProcessor}.
 */
public interface FsAdapter extends StorageService {

    StorageService getFsProcessor();

    @Override
    StorageStateDto getStorageState() throws StorageException;

    @Override
    default boolean testConnection() {
        return getFsProcessor().testConnection();
    }

    @Override
    default void storeAip(AipDto aipDto, AtomicBoolean rollback, String dataSpace) throws StorageException {
        getFsProcessor().storeAip(aipDto, rollback, dataSpace);
    }

    @Override
    default AipRetrievalResource getAip(String aipId, String dataSpace, Integer... xmlVersions) throws StorageException {
        return getFsProcessor().getAip(aipId, dataSpace, xmlVersions);
    }

    @Override
    default void storeObject(ArchivalObjectDto archivalObjectDto, AtomicBoolean rollback, String dataSpace, @NonNull Instant operationTimestamp) throws StorageException {
        getFsProcessor().storeObject(archivalObjectDto, rollback, dataSpace, operationTimestamp);
    }

    @Override
    default void storeObjectMetadata(ArchivalObjectDto objectDto, String dataSpace) throws StorageException {
        getFsProcessor().storeObjectMetadata(objectDto, dataSpace);
    }

    @Override
    default ObjectRetrievalResource getObject(String id, String dataSpace) throws StorageException {
        return getFsProcessor().getObject(id, dataSpace);
    }

    @Override
    default void delete(ArchivalObjectDto objectDto, String dataSpace, @NonNull Instant operationTimestamp) throws StorageException {
        getFsProcessor().delete(objectDto, dataSpace, operationTimestamp);
    }

    @Override
    default void remove(ArchivalObjectDto objectDto, String dataSpace, @NonNull Instant operationTimestamp) throws StorageException {
        getFsProcessor().remove(objectDto, dataSpace, operationTimestamp);
    }

    @Override
    default void renew(ArchivalObjectDto objectDto, String dataSpace, @NonNull Instant operationTimestamp) throws StorageException {
        getFsProcessor().renew(objectDto, dataSpace, operationTimestamp);
    }

    @Override
    default void rollbackAip(AipDto aipDto, String dataSpace, @NonNull Instant operationTimestamp) throws StorageException {
        getFsProcessor().rollbackAip(aipDto, dataSpace, operationTimestamp);
    }

    @Override
    default void rollbackObject(ArchivalObjectDto dto, String dataSpace, @NonNull Instant operationTimestamp) throws StorageException {
        getFsProcessor().rollbackObject(dto, dataSpace, operationTimestamp);
    }


    @Override
    default void forgetObject(String objectIdAtStorage, String dataSpace, @NonNull Instant operationTimestamp) throws StorageException {
        getFsProcessor().forgetObject(objectIdAtStorage, dataSpace, operationTimestamp);
    }

    @Override
    default AipConsistencyVerificationResultDto getAipInfo(ArchivalObjectDto aip, Map<Integer, ArchivalObjectDto> xmls, String dataSpace) throws StorageException {
        return getFsProcessor().getAipInfo(aip, xmls, dataSpace);
    }

    @Override
    default void createNewDataSpace(String dataSpace) throws IOStorageException {
        getFsProcessor().createNewDataSpace(dataSpace);
    }

    @Override
    default ArchivalObjectDto verifyStateOfObjects(List<ArchivalObjectDto> objects, AtomicLong counter) throws StorageException {
        return getFsProcessor().verifyStateOfObjects(objects, counter);
    }
}