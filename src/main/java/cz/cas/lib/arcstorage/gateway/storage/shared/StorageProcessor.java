package cz.cas.lib.arcstorage.gateway.storage.shared;

import cz.cas.lib.arcstorage.domain.AipState;
import cz.cas.lib.arcstorage.gateway.dto.AipRef;
import cz.cas.lib.arcstorage.gateway.dto.AipStateInfo;
import cz.cas.lib.arcstorage.gateway.dto.Checksum;
import cz.cas.lib.arcstorage.gateway.dto.XmlFileRef;
import cz.cas.lib.arcstorage.gateway.storage.exception.StorageException;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public interface StorageProcessor {
    void storeAip(AipRef aip, AtomicBoolean rollback) throws StorageException;

    void storeXml(String sipId, XmlFileRef xml, AtomicBoolean rollback) throws StorageException;

    void rollbackAip(String sipId) throws StorageException;

    void rollbackXml(String sipId, int version) throws StorageException;

    AipStateInfo getAipInfo(String sipId, Checksum sipChecksum, AipState aipState, Map<Integer, Checksum> xmlVersions) throws StorageException;
}
