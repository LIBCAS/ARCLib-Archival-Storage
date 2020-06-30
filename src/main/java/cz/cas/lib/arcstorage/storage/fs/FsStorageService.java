package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
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
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cz.cas.lib.arcstorage.storage.StorageUtils.isLocalhost;


/**
 * File System implementation of {@link StorageService}.
 * <p>Data are distributed into three level folder structure based on their uuid. E.g. sip file with id <i>38a4a26f-67fd-4e4c-8af3-1fd0f26465f6</i> will be stored into /38/a4/a2 folder
 * Metadata are stored in the same folder as data in a file with .meta suffix.
 * </p>
 * Fulfillment of the requirements on the metadata storing specified by the interface:
 * <ul>
 * <li>initial checksum of the object: stored in metadata file</li>
 * <li>creation time of the object: stored in metadata file in ISO-8601 UTC format</li>
 * <li>state of object matching {@link ObjectState}: stored in metadata file</li>
 * <li>for AIP XML its version and ID of SIP: id of XML is in form aipId_xml_versionNumber</li>
 * </ul>
 */
@Slf4j
public class FsStorageService implements FsAdapter {

    @Getter
    private Storage storage;
    @Getter
    private StorageService fsProcessor;
    private String sshKeyFilePath;
    private String sshUserName;
    private String rootDirPath;

    public FsStorageService(Storage storage, String rootDirPath, String sshKeyFilePath, String sshUserName, int connectionTimeout) {
        this.storage = storage;
        this.sshKeyFilePath = sshKeyFilePath;
        this.sshUserName = sshUserName;
        String separator = rootDirPath.startsWith("/") ? "/" : "\\";
        if (isLocalhost(storage))
            this.fsProcessor = new LocalFsProcessor(storage, rootDirPath);
        else
            this.fsProcessor = new RemoteFsProcessor(storage, rootDirPath, sshKeyFilePath, sshUserName, connectionTimeout);
        this.rootDirPath = rootDirPath;
    }

    @Override
    public StorageStateDto getStorageState() throws StorageException {
        if (isLocalhost(storage)) {
            File anchor = new File(rootDirPath);
            long capacity = anchor.getTotalSpace();
            long free = anchor.getFreeSpace();
            Map<String, Object> storageStateData = new HashMap<>();
            storageStateData.put("used", (capacity - free) / 1000000 + "MB");
            storageStateData.put("available", free / 1000000 + "MB");
            return new StorageStateDto(storage, storageStateData);
        }
        String[] dfResult;
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storage.getHost(), storage.getPort());
            ssh.authPublickey(sshUserName, sshKeyFilePath);
            try (Session s = ssh.startSession()) {
                dfResult = IOUtils.toString(s.exec("df -Ph " + rootDirPath).getInputStream(), Charset.defaultCharset()).split("\\n");
            }
        } catch (IOException e) {
            throw new SshException(e, storage);
        }
        if (dfResult.length < 2)
            throw new CmdOutputParsingException("df -Ph " + rootDirPath, Arrays.asList(dfResult), storage);
        Matcher m = Pattern.compile("\\S+\\s+\\S+\\s+(\\S+)\\s+(\\S+)").matcher(dfResult[1]);
        if (!m.find())
            throw new CmdOutputParsingException("df -Ph " + rootDirPath, Arrays.asList(dfResult), storage);
        Map<String, Object> storageStateData = new HashMap<>();
        storageStateData.put("used", m.group(1));
        storageStateData.put("available", m.group(2));
        return new StorageStateDto(storage, storageStateData);
    }


    @Override
    public List<ArchivalObjectDto> createDtosForAllObjects(String dataSpace) throws StorageException {
        return fsProcessor.createDtosForAllObjects(dataSpace);
    }
}
