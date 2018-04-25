package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.gateway.dto.SpaceInfo;
import cz.cas.lib.arcstorage.gateway.dto.StorageState;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.SshException;
import cz.cas.lib.arcstorage.storage.exception.StorageConnectionException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.store.Transactional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.ConnectionException;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cz.cas.lib.arcstorage.storage.StorageUtils.isLocalhost;


/**
 * File System implementation of {@link StorageService}. Files are stored to filesystem into <i>sip</i> and <i>xml</i> folders in <i>working directory</i>.
 * <p>Data are distributed into three level folder structure based on their uuid. E.g. sip file with id <i>38a4a26f-67fd-4e4c-8af3-1fd0f26465f6</i> will be stored into sip/38/a4/a2 folder</p>
 * <p>
 * Store files in that way that later it is possible to retrieve:
 * <ul>
 * <li>initial checksum of file and its type: checksums are stored during creation to the same directory as file into text file with file name and <i>.{@link cz.cas.lib.arcstorage.domain.ChecksumType}</i> suffix</li>
 * <li>creation time of file: provided by filesystem</li>
 * <li>info if file is being processed: new empty file with original file name and <i>.PROCESSING</i> suffix is created when processing starts and deleted when it ends</li>
 * <li>for SIP its ID and info if is {@link cz.cas.lib.arcstorage.domain.AipState#DELETED} or {@link cz.cas.lib.arcstorage.domain.AipState#REMOVED}:
 * SIP ID is its file name, when SIP is DELETED its files are no longer stored, when its REMOVED new empty file with SIP ID and <i>.REMOVED</i> sufffix is created</li>
 * <li>for XML its version and ID of SIP: XML file name follows <i>'SIPID'_xml_'XMLVERSION'</i> pattern</li>
 * </ul>
 */
@Transactional
@Slf4j
public class FsStorageService implements FsAdapter {

    @Getter
    private StorageConfig storageConfig;
    @Getter
    private StorageService fsProcessor;
    private String keyFilePath;

    public FsStorageService(StorageConfig storageConfig, String keyFilePath) {
        this.storageConfig = storageConfig;
        this.keyFilePath = keyFilePath;
        String separator = storageConfig.getLocation().startsWith("/") ? "/" : "\\";
        if (isLocalhost(storageConfig))
            this.fsProcessor = new LocalFsProcessor(storageConfig, separator);
        else
            this.fsProcessor = new RemoteFsProcessor(storageConfig, separator, keyFilePath);
    }

    /**
     * may be different for zfs/fs one day so that is why this is not implemented in adapter
     *
     * @return
     * @throws StorageException
     */
    @Override
    public StorageState getStorageState() throws StorageException {
        if (isLocalhost(storageConfig)) {
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
}
