package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.dto.ChecksumType;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.dto.StorageStateDto;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static cz.cas.lib.arcstorage.storage.StorageUtils.isLocalhost;


/**
 * Zettabyte File System implementation of {@link StorageService}. Files are stored to filesystem into <i>sip</i> and <i>xml</i> folders in <i>working directory</i>.
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
public class ZfsStorageService implements FsAdapter {

    @Getter
    private Storage storage;
    @Getter
    private StorageService fsProcessor;

    /**
     * Creates a new ZFS storage service.
     *
     * @param storage     storage
     * @param keyFilePath path to private key used for authentication to remote server
     */
    public ZfsStorageService(Storage storage, String keyFilePath,int connectionTimeout) {
        this.storage = storage;
        String separator = storage.getLocation().startsWith("/") ? "/" : "\\";
        if (isLocalhost(storage))
            this.fsProcessor = new LocalFsProcessor(storage);
        else
            this.fsProcessor = new RemoteFsProcessor(storage, separator, keyFilePath,connectionTimeout);
    }

    @Override
    public StorageStateDto getStorageState() throws StorageException {
        throw new UnsupportedOperationException();
//        List<String> lines;
//        if (isLocalhost(storage)) {
//            Utils.Pair<Integer, List<String>> processResult = executeProcessCustomResultHandle("zfs", "list");
//            lines = processResult.getR();
//            if (!processResult.getL().equals(0))
//                throw new CmdProcessException("zfs list", lines);
//        } else {
//            try (SSHClient ssh = new SSHClient()) {
//                ssh.addHostKeyVerifier(new PromiscuousVerifier());
//                ssh.connect(storage.getHost(), storage.getPort());
//                ssh.authPublickey("arcstorage", keyFilePath);
//                try (Session s = ssh.startSession()) {
//                    lines = Arrays.asList(IOUtils.toString(s.exec("sudo zfs list").getInputStream(), Charset.defaultCharset()).split(System.lineSeparator()));
//                }
//            } catch (IOException e) {
//                throw new SshException(e);
//            }
//        }
//        Map<String, String> storageStateData = new HashMap<>();
//        Pattern pattern = Pattern.compile("(\\w+)\\s+(\\w+)\\s+(\\w+)");
//        for (String line : lines) {
//            Matcher m = pattern.matcher(line);
//            if (!m.find())
//                throw new CmdOutputParsingException("sudo zfs list", lines);
//            if (m.group(1).equals(pool)) {
//                storageStateData.put(pool + " used", m.group(2));
//                storageStateData.put(pool + " available", m.group(3));
//                continue;
//            }
//            if (m.group(1).equals(dataset)) {
//                storageStateData.put(dataset + " used", m.group(2));
//                storageStateData.put(dataset + " available", m.group(3));
//            }
//        }
//        return new StorageStateDto(storage, storageStateData);
    }
}
