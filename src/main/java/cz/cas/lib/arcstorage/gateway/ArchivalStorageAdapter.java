package cz.cas.lib.arcstorage.gateway;

import cz.cas.lib.arcstorage.gateway.dto.AipCreationMd5InfoDto;
import cz.cas.lib.arcstorage.gateway.dto.StorageStateDto;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

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
public interface ArchivalStorageAdapter {

    /**
     * Stores Aip files into storage.
     *
     * @param sip
     * @param sipId
     * @param xml
     * @return Object containing MD5 checksums computed from stored files.
     * @throws IOException
     */
    AipCreationMd5InfoDto storeAip(InputStream sip, String sipId, InputStream xml) throws IOException;

    /**
     * Retrieves references to Aip files. Caller is responsible for closing retrieved streams.
     *
     * @param sipId
     * @param xmlVersions specifies which XML versions should be retrieved, typically all or the latest only
     * @return list with opened file streams where first item is SIP stream and others are XML streams in the same order as was passed in {@code xmlVersions} parameter
     * @throws IOException
     */
    List<InputStream> getAip(String sipId, Integer... xmlVersions) throws IOException;

    /**
     * Stores XML files into storage.
     *
     * @param sipId
     * @param version
     * @param xml
     * @return MD5 checksum computed from stored file
     * @throws IOException
     */
    String storeXml(InputStream xml, String sipId, int version) throws IOException;

    /**
     * Retrieves reference to AipXml file. Caller is responsible for closing retrieved stream.
     *
     * @param sipId
     * @param version
     * @return
     * @throws IOException
     */
    InputStream getXml(String sipId, int version) throws IOException;

    /**
     * Deletes SIP file from storage. Must not fail if SIP is already deleted.
     *
     * @param id
     * @param rollback false if the deletion is due to user request, true if it is due to storage/application failure
     * @throws IOException
     */
    void deleteSip(String id, boolean rollback) throws IOException;

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
    void deleteXml(String sipId, int version) throws IOException;

    /**
     * Logically removes SIP. Must not fail if SIP is already removed.
     *
     * @param id
     * @throws IOException
     */
    void remove(String id) throws IOException;

    /**
     * Computes and retrieves MD5 checksums of XML files.
     *
     * @param sipId
     * @param xmlVersions
     * @return Map with versions of XML as keys and MD5 strings with fixity information as values. If XML file does not exist or is locked (e.g. accessed by other process) value assigned to its ID should be null.
     * @throws IOException
     */
    Map<Integer, String> getXmlsMD5(String sipId, List<Integer> xmlVersions) throws IOException;

    /**
     * Computes and retrieves MD5 checksum of SIP file.
     *
     * @param sipId
     * @return MD5 checksum of SIP file or null if file does not exist or is locked (e.g. accessed by other process)
     * @throws IOException
     */
    String getSipMD5(String sipId) throws IOException;

    /**
     * Returns state of currently used storage.
     *
     * @return
     */
    StorageStateDto getStorageState();
}
