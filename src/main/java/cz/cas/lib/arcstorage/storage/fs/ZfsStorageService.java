package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.dto.StorageStateDto;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.CmdOutputParsingException;
import cz.cas.lib.arcstorage.storage.exception.SshException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cz.cas.lib.arcstorage.storage.StorageUtils.isLocalhost;
import static cz.cas.lib.arcstorage.util.Utils.fetchDataFromLocal;
import static cz.cas.lib.arcstorage.util.Utils.fetchDataFromRemote;


/**
 * Zettabyte File System implementation of {@link StorageService}.
 * <p>Data are distributed into three level folder structure based on their uuid. E.g. sip file with id <i>38a4a26f-67fd-4e4c-8af3-1fd0f26465f6</i> will be stored into /38/a4/a2 folder</p>
 * Fulfillment of the requirements on the metadata storing specified by the interface:
 * Metadata are stored in the same folder as data in a file with .meta suffix.
 * <ul>
 * <li>initial checksum of the object: stored in metadata file</li>
 * <li>creation time of the object: stored in metadata file in ISO-8601 UTC format</li>
 * <li>state of object matching {@link ObjectState}: stored in metadata file</li>
 * <li>for AIP XML its version and ID of SIP: id of XML is in form aipId_xml_versionNumber</li>
 * </ul>
 */
@Slf4j
public class ZfsStorageService implements FsAdapter {

    @Getter
    private Storage storage;
    @Getter
    private StorageService fsProcessor;
    private String sshKeyFilePath;
    private String sshUserName;
    private String rootDirPath;
    private String poolName;
    public static final String CMD_LIST_POOLS = "zpool list";
    public static final String CMD_LIST_DATASETS = "zfs list";
    public static final String CMD_STATUS = "zpool status -v";
    private boolean isLocalStorage;

    /**
     * Creates a new ZFS storage service.
     *
     * @param storage     storage
     * @param sshKeyFilePath path to private key used for authentication to remote server
     */
    public ZfsStorageService(Storage storage, String rootDirPath, String poolName, String sshKeyFilePath, String sshUserName, int connectionTimeout) {
        this.storage = storage;
        this.rootDirPath = rootDirPath;
        this.poolName = poolName;
        if(isLocalhost(storage))
            this.fsProcessor = new LocalFsProcessor(storage, rootDirPath);
        else {
            this.fsProcessor = new RemoteFsProcessor(storage, rootDirPath, sshKeyFilePath, sshUserName, connectionTimeout);
            this.sshKeyFilePath = sshKeyFilePath;
            this.sshUserName = sshUserName;
        }
        this.isLocalStorage = (isLocalhost(storage));
    }

    @Override
    /**
     * retuns storage itself together with information about its pool and dataset free space and health
     */
    public StorageStateDto getStorageState() throws StorageException {
        String datasetCmd = CMD_LIST_DATASETS + " " + rootDirPath;
        String poolCmd = CMD_LIST_POOLS + " " + poolName;
        String stateAndScrubbingCmd = CMD_STATUS + " " + poolName;
        List<String> poolRawData;
        List<String> datasetRawData;
        List<String> stateAndScrubbingData;
        if(isLocalStorage) {
            poolRawData = fetchDataFromLocal(poolCmd, storage);
            datasetRawData = fetchDataFromLocal(datasetCmd, storage);
            stateAndScrubbingData = fetchDataFromLocal(stateAndScrubbingCmd, storage);
        } else {
            try(SSHClient ssh = new SSHClient()) {
                datasetCmd = "sudo " + datasetCmd;
                poolCmd = "sudo " + poolCmd;
                stateAndScrubbingCmd = "sudo " + stateAndScrubbingCmd;
                ssh.addHostKeyVerifier(new PromiscuousVerifier());
                ssh.connect(storage.getHost(), storage.getPort());
                ssh.authPublickey(sshUserName, sshKeyFilePath);
                datasetRawData = fetchDataFromRemote(ssh, datasetCmd, storage);
                poolRawData = fetchDataFromRemote(ssh, poolCmd, storage);
                stateAndScrubbingData = fetchDataFromRemote(ssh, stateAndScrubbingCmd, storage);
            } catch(IOException e) {
                throw new SshException(e, storage);
            }
        }
        Map<String, Object> stateData = new HashMap<>();
        parseAndFillStorageState(datasetRawData, datasetCmd, poolRawData, poolCmd, stateData);
        stateData.put("cmd: " + CMD_STATUS, stateAndScrubbingData);
        return new StorageStateDto(storage, stateData);
    }

    @Override
    public List<ArchivalObjectDto> createDtosForAllObjects(String dataSpace) throws StorageException {
        return fsProcessor.createDtosForAllObjects(dataSpace);
    }

    private void parseAndFillStorageState(List<String> datasetListOutput, String datasetCmd, List<String> poolListOutput, String poolCmd, Map<String, Object> map) throws CmdOutputParsingException {
        if(datasetListOutput.size() < 2)
            throw new CmdOutputParsingException(datasetCmd, datasetListOutput, storage);
        if(poolListOutput.size() < 2)
            throw new CmdOutputParsingException(poolCmd, poolListOutput, storage);
        Pattern poolRegex = Pattern.compile("([^\\s]+)\\s*([^\\s]+)\\s*([^\\s]+)\\s*([^\\s]+)\\s*([^\\s]+)\\s*([^\\s]+)\\s*([^\\s]+)\\s*([^\\s]+)\\s*([^\\s]+)\\s*([^\\s]+)");
        Pattern datasetRegex = Pattern.compile("([^\\s]+)\\s*([^\\s]+)\\s*([^\\s]+)\\s*([^\\s]+)\\s*([^\\s]+)");
        Matcher matcher = datasetRegex.matcher(datasetListOutput.get(1));
        boolean found = matcher.find();
        if(found) {
            map.put("dataset", new DatasetInfoDto(matcher.group(1), matcher.group(2), matcher.group(3)));
        } else {
            throw new CmdOutputParsingException(datasetCmd, datasetListOutput, storage);
        }
        Matcher poolHeaderMatcher = poolRegex.matcher(poolListOutput.get(0));
        Matcher poolInfoMatcher = poolRegex.matcher(poolListOutput.get(1));
        found = poolHeaderMatcher.find() && poolInfoMatcher.find();
        Map<String, String> poolMap = new HashMap<>();
        map.put("pool", poolMap);
        if(found) {
            for(int i = 0; i < poolHeaderMatcher.groupCount(); i++) {
                String key = poolHeaderMatcher.group(i + 1);
                poolMap.put(key, poolInfoMatcher.group(i + 1));
            }
        } else {
            throw new CmdOutputParsingException(poolCmd, poolListOutput, storage);
        }
    }

    @Setter
    @Getter
    @AllArgsConstructor
    public class DatasetInfoDto {
        private String NAME;
        private String USED;
        private String AVAILABLE;
    }
}
