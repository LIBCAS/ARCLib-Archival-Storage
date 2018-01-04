package cz.cas.lib.arcstorage.gateway.storage;

import cz.cas.lib.arcstorage.domain.AipState;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.gateway.storage.exception.StorageException;
import cz.cas.lib.arcstorage.gateway.storage.shared.LocalStorageProcessor;
import cz.cas.lib.arcstorage.gateway.storage.shared.RemoteStorageProcessor;
import cz.cas.lib.arcstorage.gateway.storage.shared.StorageProcessor;
import cz.cas.lib.arcstorage.store.Transactional;
import lombok.extern.log4j.Log4j;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * File System implementation of {@link StorageService}. Files are stored to filesystem into <i>sip</i> and <i>xml</i> folders in <i>working directory</i>.
 * <p>Data are distributed into three level folder structure based on their uuid. E.g. sip file with id <i>38a4a26f-67fd-4e4c-8af3-1fd0f26465f6</i> will be stored into sip/38/a4/a2 folder</p>
 * <p>
 * Store files in that way that later it is possible to retrieve:
 * <ul>
 * <li>initial checksum of file and its type: checksums are stored during creation to the same directory as file into text file with file name and <i>.{@link cz.cas.lib.arcstorage.domain.ChecksumType}</i> suffix</li>
 * <li>creation time of file: provided by filesystem</li>
 * <li>info if file is being processed: new empty file with original file name and <i>.LOCK</i> suffix is created when processing starts and deleted when it ends</li>
 * <li>for SIP its ID and info if is {@link cz.cas.lib.arcstorage.domain.AipState#DELETED} or {@link cz.cas.lib.arcstorage.domain.AipState#REMOVED}:
 * SIP ID is its file name, when SIP is DELETED its files are no longer stored, when its REMOVED new empty file with SIP ID and <i>.REMOVED</i> sufffix is created</li>
 * <li>for XML its version and ID of SIP: XML file name follows <i>'SIPID'_xml_'XMLVERSION'</i> pattern</li>
 * </ul>
 * <b>For testing purposes, this prototype implementation uses {@link Thread#sleep(long)} in create/delete methods to simulate time-consuming operations.</b>
 */
@Transactional
@Log4j
public class FsStorageService implements StorageService {

    private StorageConfig storageConfig;
    private JsonParser jsonParser;
    private StorageProcessor storageProcessor;

    public FsStorageService(StorageConfig storageConfig) {
        this.storageConfig = storageConfig;
        jsonParser = JsonParserFactory.getJsonParser();
        String separator = storageConfig.getSipLocation().startsWith("/") ? "/" : "\\";
        if (isLocalhost())
            this.storageProcessor = new LocalStorageProcessor(storageConfig, separator);
        else
            this.storageProcessor = new RemoteStorageProcessor(storageConfig, separator);
    }

    public StorageConfig getStorageConfig() {
        return storageConfig;
    }

    @Override
    public void storeAip(AipRef aip, AtomicBoolean rollback) throws StorageException {
        storageProcessor.storeAip(aip, rollback);
    }

    @Override
    public void storeXml(String sipId, XmlFileRef xml, AtomicBoolean rollback) throws IOException {
        storageProcessor.storeXml(sipId, xml, rollback);
    }

    @Override
    public void rollbackAip(String sipId) throws StorageException {
        storageProcessor.rollbackAip(sipId);
    }

    @Override
    public void rollbackXml(String sipId, int version) throws StorageException {
        storageProcessor.rollbackXml(sipId, version);
    }

    @Override
    public List<InputStream> getAip(String sipId, Integer... xmlVersions) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getXml(String sipId, int version) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteSip(String sipId) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public AipStateInfo getAipInfo(String sipId, Checksum sipChecksum, AipState aipState, Map<Integer, Checksum> xmlVersions) throws StorageException {
        return storageProcessor.getAipInfo(sipId, sipChecksum, aipState, xmlVersions);
    }

    @Override
    public void remove(String sipId) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public StorageState getStorageState() {
        File anchor = new File(".");
        return new StorageState(anchor.getTotalSpace(), anchor.getFreeSpace(), true, StorageType.FS);
    }

    private boolean isLocalhost() {
        return storageConfig.getHost().equals("localhost") || storageConfig.getHost().equals("127.0.0.1");
    }
}
