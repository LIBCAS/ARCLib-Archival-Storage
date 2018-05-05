package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.domain.entity.StorageConfig;
import cz.cas.lib.arcstorage.dto.ChecksumType;
import cz.cas.lib.arcstorage.dto.StorageStateDto;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.CmdOutputParsingException;
import cz.cas.lib.arcstorage.storage.exception.SshException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cz.cas.lib.arcstorage.storage.StorageUtils.isLocalhost;


/**
 * File System implementation of {@link StorageService}. Files are stored to filesystem into <i>sip</i> and <i>xml</i> folders in <i>working directory</i>.
 * <p>Data are distributed into three level folder structure based on their uuid. E.g. sip file with id <i>38a4a26f-67fd-4e4c-8af3-1fd0f26465f6</i> will be stored into sip/38/a4/a2 folder</p>
 * <p>
 * Store files in that way that later it is possible to retrieve:
 * <ul>
 * <li>initial checksum of file and its type: checksums are stored during creation to the same directory as file into text file with file name and <i>.{@link ChecksumType}</i> suffix</li>
 * <li>creation time of file: provided by filesystem</li>
 * <li>info if file is being processed: new empty file with original file name and <i>.PROCESSING</i> suffix is created when processing starts and deleted when it ends</li>
 * <li>for SIP its ID and info if is {@link ObjectState#DELETED} or {@link ObjectState#REMOVED}:
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
            this.fsProcessor = new LocalFsProcessor(storageConfig);
        else
            this.fsProcessor = new RemoteFsProcessor(storageConfig, separator, keyFilePath);
    }

    @Override
    public StorageStateDto getStorageState() throws StorageException {
        if (isLocalhost(storageConfig)) {
            File anchor = new File(storageConfig.getLocation());
            long capacity = anchor.getTotalSpace();
            long free = anchor.getFreeSpace();
            Map<String, String> storageStateData = new HashMap<>();
            storageStateData.put("used", (capacity - free) / 1000000 + "MB");
            storageStateData.put("available", free / 1000000 + "MB");
            return new StorageStateDto(storageConfig, storageStateData);
        }
        String[] dfResult;
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            ssh.authPublickey("arcstorage", keyFilePath);
            try (Session s = ssh.startSession()) {
                dfResult = IOUtils.toString(s.exec("df -Ph " + storageConfig.getLocation()).getInputStream(), Charset.defaultCharset()).split("\\n");
            }
        } catch (IOException e) {
            throw new SshException(e);
        }
        if (dfResult.length < 2)
            throw new CmdOutputParsingException("df -Ph " + storageConfig.getLocation(), Arrays.asList(dfResult));
        Matcher m = Pattern.compile("\\S+\\s+\\S+\\s+(\\S+)\\s+(\\S+)").matcher(dfResult[1]);
        if (!m.find())
            throw new CmdOutputParsingException("df -Ph " + storageConfig.getLocation(), Arrays.asList(dfResult));
        Map<String, String> storageStateData = new HashMap<>();
        storageStateData.put("used", m.group(1));
        storageStateData.put("available", m.group(2));
        return new StorageStateDto(storageConfig, storageStateData);
    }
}
