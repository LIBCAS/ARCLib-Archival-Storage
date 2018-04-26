package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.AipState;
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
    StorageState getStorageState() throws StorageException;

    @Override
    default boolean testConnection() {
        return getFsProcessor().testConnection();
    }

    @Override
    default void storeAip(AipRef aipRef, AtomicBoolean rollback) throws StorageException {
        getFsProcessor().storeAip(aipRef, rollback);
    }

    @Override
    default List<FileRef> getAip(String sipId, Integer... xmlVersions) throws StorageException {
        return getFsProcessor().getAip(sipId, xmlVersions);
    }

    @Override
    default void storeXml(String sipId, XmlRef xmlRef, AtomicBoolean rollback) throws StorageException {
        getFsProcessor().storeXml(sipId, xmlRef, rollback);
    }

    @Override
    default FileRef getXml(String sipId, int version) throws StorageException {
        return getFsProcessor().getXml(sipId, version);
    }

    @Override
    default void storeSip(ArchiveFileRef aipRef, AtomicBoolean rollback) throws StorageException {
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
    default AipStateInfo getAipInfo(String sipId, Checksum sipChecksum, AipState aipState, Map<Integer, Checksum> xmlVersions) throws StorageException {
        return getFsProcessor().getAipInfo(sipId, sipChecksum, aipState, xmlVersions);
    }
}