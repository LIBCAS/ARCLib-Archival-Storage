package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;

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
        getFsProcessor().storeAip(aipDto, rollback,dataSpace);
    }

    @Override
    default AipRetrievalResource getAip(String aipId, String dataSpace, Integer... xmlVersions) throws StorageException {
        return getFsProcessor().getAip(aipId,dataSpace, xmlVersions);
    }

    @Override
    default void storeObject(ArchivalObjectDto archivalObjectDto, AtomicBoolean rollback, String dataSpace) throws StorageException {
        getFsProcessor().storeObject(archivalObjectDto, rollback,dataSpace);
    }

    @Override
    default void storeObjectMetadata(ArchivalObjectDto objectDto, String dataSpace) throws StorageException {
        getFsProcessor().storeObjectMetadata(objectDto, dataSpace);
    }

    @Override
    default ObjectRetrievalResource getObject(String id, String dataSpace) throws StorageException {
        return getFsProcessor().getObject(id,dataSpace);
    }

    @Override
    default void delete(String id, String dataSpace) throws StorageException {
        getFsProcessor().delete(id,dataSpace);
    }

    @Override
    default void remove(String id, String dataSpace) throws StorageException {
        getFsProcessor().remove(id,dataSpace);
    }

    @Override
    default void renew(String id, String dataSpace) throws StorageException {
        getFsProcessor().renew(id,dataSpace);
    }

    @Override
    default void rollbackAip(AipDto aipDto, String dataSpace) throws StorageException {
        getFsProcessor().rollbackAip(aipDto, dataSpace);
    }

    @Override
    default void rollbackObject(ArchivalObjectDto dto, String dataSpace) throws StorageException {
        getFsProcessor().rollbackObject(dto, dataSpace);
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