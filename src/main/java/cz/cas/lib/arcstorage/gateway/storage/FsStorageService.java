package cz.cas.lib.arcstorage.gateway.storage;

import cz.cas.lib.arcstorage.domain.AipState;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.gateway.storage.exception.SshException;
import cz.cas.lib.arcstorage.gateway.storage.exception.StorageConnectionException;
import cz.cas.lib.arcstorage.gateway.storage.exception.StorageException;
import cz.cas.lib.arcstorage.gateway.storage.fs.FsStorageState;
import cz.cas.lib.arcstorage.gateway.storage.shared.LocalStorageProcessor;
import cz.cas.lib.arcstorage.gateway.storage.shared.RemoteStorageProcessor;
import cz.cas.lib.arcstorage.store.Transactional;
import lombok.extern.slf4j.Slf4j;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.ConnectionException;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import org.apache.commons.io.IOUtils;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cz.cas.lib.arcstorage.gateway.storage.shared.StorageUtils.keyFilePath;


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
@Slf4j
public class FsStorageService implements StorageService {

    private StorageConfig storageConfig;
    private JsonParser jsonParser;
    private StorageService storageProcessor;

    public FsStorageService(StorageConfig storageConfig) {
        this.storageConfig = storageConfig;
        jsonParser = JsonParserFactory.getJsonParser();
        String separator = storageConfig.getLocation().startsWith("/") ? "/" : "\\";
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
    public void storeXml(String sipId, XmlFileRef xml, AtomicBoolean rollback) throws StorageException {
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
    public List<InputStream> getAip(String sipId, Integer... xmlVersions) throws StorageException {
        return storageProcessor.getAip(sipId, xmlVersions);
    }

    @Override
    public InputStream getXml(String sipId, int version) throws StorageException {
        return storageProcessor.getXml(sipId, version);
    }

    @Override
    public void deleteSip(String sipId) throws StorageException {
        storageProcessor.deleteSip(sipId);
    }

    @Override
    public AipStateInfo getAipInfo(String sipId, Checksum sipChecksum, AipState aipState, Map<Integer, Checksum> xmlVersions) throws StorageException {
        return storageProcessor.getAipInfo(sipId, sipChecksum, aipState, xmlVersions);
    }

    @Override
    public void remove(String sipId) throws StorageException {
        storageProcessor.remove(sipId);
    }

    @Override
    public StorageState getStorageState() throws StorageException {
        if (isLocalhost()) {
            File anchor = new File(storageConfig.getLocation());
            long capacity = anchor.getTotalSpace();
            long free = anchor.getFreeSpace();
            return new FsStorageState(storageConfig, new SpaceInfo(capacity, capacity - free, free));
        }
        String dfResult;
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            ssh.authPublickey("root", keyFilePath);
            try (Session s = ssh.startSession()) {
                dfResult = IOUtils.toString(s.exec("df " + storageConfig.getLocation()).getInputStream(), Charset.defaultCharset());
            }
        } catch (ConnectionException e) {
            throw new StorageConnectionException(e);
        } catch (IOException e) {
            throw new SshException(e);
        }
        Matcher m = Pattern.compile(".+\\d+\\s+(\\d+)\\s+(\\d+)\\s+").matcher(dfResult);
        if (!m.find())
            throw new GeneralException("could not parse bytes from df command, cmd result: " + dfResult);
        long used = Long.parseLong(m.group(1));
        long free = Long.parseLong(m.group(2));
        return new FsStorageState(storageConfig, new SpaceInfo(used + free, used, free));
    }

    private boolean isLocalhost() {
        return storageConfig.getHost().equals("localhost") || storageConfig.getHost().equals("127.0.0.1");
    }
}
