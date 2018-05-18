package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Adapter for more elegant sharing of code.
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
    default void storeAip(AipDto aipDto, AtomicBoolean rollback) throws StorageException {
        getFsProcessor().storeAip(aipDto, rollback);
    }

    @Override
    default AipRetrievalResource getAip(String sipId, Integer... xmlVersions) throws StorageException {
        return getFsProcessor().getAip(sipId, xmlVersions);
    }

    @Override
    default void storeObject(ArchivalObjectDto archivalObjectDto, AtomicBoolean rollback) throws StorageException {
        getFsProcessor().storeObject(archivalObjectDto, rollback);
    }

    @Override
    default ObjectRetrievalResource getObject(String id) throws StorageException {
        return getFsProcessor().getObject(id);
    }

    @Override
    default void deleteSip(String id) throws StorageException {
        getFsProcessor().deleteSip(id);
    }

    @Override
    default void remove(String id) throws StorageException {
        getFsProcessor().remove(id);
    }

    @Override
    default void renew(String id) throws StorageException {
        getFsProcessor().renew(id);
    }

    @Override
    default void rollbackAip(String sipId) throws StorageException {
        getFsProcessor().rollbackAip(sipId);
    }

    @Override
    default void rollbackObject(String sipId) throws StorageException {
        getFsProcessor().rollbackObject(sipId);
    }

    @Override
    default AipStateInfoDto getAipInfo(String sipId, Checksum sipChecksum, ObjectState objectState, Map<Integer, Checksum> xmlVersions) throws StorageException {
        return getFsProcessor().getAipInfo(sipId, sipChecksum, objectState, xmlVersions);
    }
}