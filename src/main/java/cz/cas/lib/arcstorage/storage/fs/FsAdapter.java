package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.ObjectState;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;

import java.util.List;
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
    default List<FileContentDto> getAip(String sipId, Integer... xmlVersions) throws StorageException {
        return getFsProcessor().getAip(sipId, xmlVersions);
    }

    @Override
    default void storeXml(String sipId, XmlDto xmlRef, AtomicBoolean rollback) throws StorageException {
        getFsProcessor().storeXml(sipId, xmlRef, rollback);
    }

    @Override
    default FileContentDto getXml(String sipId, int version) throws StorageException {
        return getFsProcessor().getXml(sipId, version);
    }

    @Override
    default void storeSip(ArchivalObjectDto aipRef, AtomicBoolean rollback) throws StorageException {
        getFsProcessor().storeSip(aipRef, rollback);
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
    default void rollbackAip(String sipId) throws StorageException {
        getFsProcessor().rollbackAip(sipId);
    }

    @Override
    default void rollbackXml(String sipId, int version) throws StorageException {
        getFsProcessor().rollbackXml(sipId, version);
    }

    @Override
    default AipStateInfoDto getAipInfo(String sipId, Checksum sipChecksum, ObjectState objectState, Map<Integer, Checksum> xmlVersions) throws StorageException {
        return getFsProcessor().getAipInfo(sipId, sipChecksum, objectState, xmlVersions);
    }
}