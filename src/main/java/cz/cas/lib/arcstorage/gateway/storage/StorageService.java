package cz.cas.lib.arcstorage.gateway.storage;

import cz.cas.lib.arcstorage.domain.AipState;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.gateway.storage.exception.FileDoesNotExistException;
import cz.cas.lib.arcstorage.gateway.storage.exception.StorageException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation <b>must</b> store files in that way that later it is possible to retrieve:
 * <ul>
 * <li>initial MD5 checksum of file</li>
 * <li>creation time of file</li>
 * <li>info if file is being processed</li>
 * <li>info when file processing has failed</li>
 * <li>for SIP its ID and info if is {@link cz.cas.lib.arcstorage.domain.AipState#DELETED} or {@link cz.cas.lib.arcstorage.domain.AipState#REMOVED}</li>
 * <li>for XML its version and ID of SIP</li>
 * </ul>
 */

public interface StorageService {

    StorageConfig getStorageConfig();

    /**
     * Stores Aip files into storage.
     *
     * @return Object containing MD5 checksums computed from stored files.
     * @throws IOException
     */
    void storeAip(AipRef aipRef, AtomicBoolean rollback) throws StorageException;

    /**
     * Retrieves references to Aip files. Caller is responsible for closing retrieved streams.
     *
     * @param sipId
     * @param xmlVersions specifies which XML versions should be retrieved, typically all or the latest only
     * @return list with opened file streams where first item is SIP stream and others are XML streams in the same order as was passed in {@code xmlVersions} parameter
     * @throws IOException
     */
    List<InputStream> getAip(String sipId, Integer... xmlVersions) throws FileDoesNotExistException, StorageException;

    /**
     * Stores XML files into storage.
     *
     * @return checksum computed from stored file
     * @throws IOException
     */
    void storeXml(String sipId, XmlFileRef xmlFileRef, AtomicBoolean rollback) throws StorageException;

    /**
     * Retrieves reference to AipXml file. Caller is responsible for closing retrieved stream.
     *
     * @param sipId
     * @param version
     * @return
     * @throws IOException
     */
    InputStream getXml(String sipId, int version) throws StorageException;

    /**
     * Deletes SIP file from storage. Must not fail if SIP is already deleted.
     *
     * @param id
     * @throws IOException
     */
    void deleteSip(String id) throws StorageException;

    /**
     * Logically removes SIP. Must not fail if SIP is already removed.
     *
     * @param id
     * @throws IOException
     */
    void remove(String id) throws StorageException;

    void rollbackAip(String sipId) throws StorageException;

    /**
     * Deletes XML file from storage. Used only in case of storage/application failure.
     * <p>
     * In case that file has been already deleted (rollbacked) do nothing.
     * </p>
     *
     * @param sipId
     * @param version
     * @throws IOException
     */
    void rollbackXml(String sipId, int version) throws StorageException;

    /**
     * Computes and retrieves MD5 checksum of SIP file.
     *
     * @param sipId
     * @return MD5 checksum of SIP file or null if file does not exist or is locked (e.g. accessed by other process)
     * @throws IOException
     */
    AipStateInfo getAipInfo(String sipId, Checksum sipChecksum, AipState aipState, Map<Integer, Checksum> xmlVersions) throws StorageException;

    /**
     * Returns state of currently used storage.
     *
     * @return
     */
    StorageState getStorageState() throws StorageException;
}
