package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.dto.ChecksumType;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.dto.StorageStateDto;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.CmdOutputParsingException;
import cz.cas.lib.arcstorage.storage.exception.SshException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
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
 * File System implementation of {@link StorageService}.
 * <p>Data are distributed into three level folder structure based on their uuid. E.g. sip file with id <i>38a4a26f-67fd-4e4c-8af3-1fd0f26465f6</i> will be stored into /38/a4/a2 folder</p>
 * Fulfillment of the requirements on the metadata storing specified by the interface:
 * <ul>
 * <li>initial checksum of the object: checksums are stored during creation to the same directory as file into text file with file name and <i>.{@link ChecksumType}</i> suffix</li>
 * <li>creation time of the object: provided by filesystem</li>
 * <li>state of object matching {@link ObjectState}: states are handled by empty files with original file name and <i>.STATE</i> suffix (e.g. fileId.PROCESSING)</li>
 * <li>for AIP XML its version and ID of SIP: id of XML is in form aipId_xml_versionNumber</li>
 * </ul>
 */
@Transactional
@Slf4j
public class FsStorageService implements FsAdapter {

    @Getter
    private Storage storage;
    @Getter
    private StorageService fsProcessor;
    private String keyFilePath;

    public FsStorageService(Storage storage, String keyFilePath, int connectionTimeout) {
        this.storage = storage;
        this.keyFilePath = keyFilePath;
        String separator = storage.getLocation().startsWith("/") ? "/" : "\\";
        if (isLocalhost(storage))
            this.fsProcessor = new LocalFsProcessor(storage);
        else
            this.fsProcessor = new RemoteFsProcessor(storage, separator, keyFilePath, connectionTimeout);
    }

    @Override
    public StorageStateDto getStorageState() throws StorageException {
        if (isLocalhost(storage)) {
            File anchor = new File(storage.getLocation());
            long capacity = anchor.getTotalSpace();
            long free = anchor.getFreeSpace();
            Map<String, String> storageStateData = new HashMap<>();
            storageStateData.put("used", (capacity - free) / 1000000 + "MB");
            storageStateData.put("available", free / 1000000 + "MB");
            return new StorageStateDto(storage, storageStateData);
        }
        String[] dfResult;
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storage.getHost(), storage.getPort());
            ssh.authPublickey("arcstorage", keyFilePath);
            try (Session s = ssh.startSession()) {
                dfResult = IOUtils.toString(s.exec("df -Ph " + storage.getLocation()).getInputStream(), Charset.defaultCharset()).split("\\n");
            }
        } catch (IOException e) {
            throw new SshException(e);
        }
        if (dfResult.length < 2)
            throw new CmdOutputParsingException("df -Ph " + storage.getLocation(), Arrays.asList(dfResult));
        Matcher m = Pattern.compile("\\S+\\s+\\S+\\s+(\\S+)\\s+(\\S+)").matcher(dfResult[1]);
        if (!m.find())
            throw new CmdOutputParsingException("df -Ph " + storage.getLocation(), Arrays.asList(dfResult));
        Map<String, String> storageStateData = new HashMap<>();
        storageStateData.put("used", m.group(1));
        storageStateData.put("available", m.group(2));
        return new StorageStateDto(storage, storageStateData);
    }
}
