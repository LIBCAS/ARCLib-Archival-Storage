package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.StorageUtils;
import cz.cas.lib.arcstorage.storage.exception.*;
import cz.cas.lib.arcstorage.util.Utils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.SSHException;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.FilePermission;
import net.schmizz.sshj.xfer.InMemorySourceFile;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.lang.NonNull;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;

/**
 * implementation used by {@link FsAdapter} to provide {@link ZfsStorageService} and {@link FsStorageService} with methods
 * for access to the remote FS/ZFS over SFTP
 */
@Slf4j
public class RemoteFsProcessor implements StorageService {

    private static final String OPTIMIZED_CHECKSUM_CMD_FILEPATH_PLACEHOLDER = "$filePath";

    @Getter
    private final Storage storage;
    @Getter
    private final String separator;
    private final String sshKeyFilePath;
    private final String sshUserName;
    private final String rootDirPath;
    private final int connectionTimeout;
    /**
     * e.g. {
     * "MD5":"md5sum $filePath | awk '{print $1}'",
     * "SHA512":"sha512sum $filePath | awk '{print $1}'"
     * }
     */
    private final Map<ChecksumType, String> optimizedChecksumComputationCommands;

    public RemoteFsProcessor(Storage storage, String rootDirPath, String sshKeyFilePath, String sshUserName, int connectionTimeout, Map<ChecksumType, String> optimizedChecksumComputationCommands) {
        this.storage = storage;
        this.separator = rootDirPath.startsWith("/") ? "/" : "\\";
        this.sshKeyFilePath = sshKeyFilePath;
        this.sshUserName = sshUserName;
        this.connectionTimeout = connectionTimeout;
        this.rootDirPath = rootDirPath;
        this.optimizedChecksumComputationCommands = optimizedChecksumComputationCommands == null ? Map.of() : optimizedChecksumComputationCommands;
    }

    @Override
    public boolean testConnection() {
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                Set<net.schmizz.sshj.xfer.FilePermission> perms = sftp.perms(rootDirPath);
                return (perms.contains(FilePermission.GRP_R) || perms.contains(FilePermission.USR_R)) &&
                        perms.contains(FilePermission.GRP_W) || perms.contains(FilePermission.USR_W);
            }
        } catch (Exception e) {
            log.error(storage.getName() + " unable to connect: " + e.getClass() + " " + e.getMessage());
            return false;
        }
    }

    @Override
    public void storeAip(AipDto aip, AtomicBoolean rollback, String dataSpace) throws StorageException {
        String folder = getFolderPath(aip.getSip().getDatabaseId(), dataSpace);
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            listenForRollbackToKillSession(ssh, rollback);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                storeFile(ssh, sftp, folder, aip.getXml().getStorageId(), aip.getXml().getInputStream(), aip.getXml().getChecksum(), rollback, aip.getXml().getCreated());
                storeFile(ssh, sftp, folder, aip.getSip().getStorageId(), aip.getSip().getInputStream(), aip.getSip().getChecksum(), rollback, aip.getSip().getCreated());
            }
        } catch (IOException e) {
            rollback.set(true);
            throw new SshException(e, storage);
        } catch (Exception e) {
            rollback.set(true);
            throw new GeneralException(e);
        }
    }

    @Override
    public AipRetrievalResource getAip(String aipId, String dataSpace, Integer... xmlVersions) throws StorageException {
        SSHClient ssh = null;
        try {
            ssh = new SSHClient();
            connect(ssh);
            SFTPClient sftp = ssh.newSFTPClient();
            AipRetrievalResource aip = new AipRetrievalResource(ssh);
            String folder = getFolderPath(aipId, dataSpace);
            aip.setSip(getFile(ssh, sftp, folder + separator + aipId));
            for (Integer xmlVersion : xmlVersions) {
                aip.addXml(xmlVersion, getFile(ssh, sftp, folder + separator + toXmlId(aipId, xmlVersion)));
            }
            return aip;
        } catch (IOException e) {
            try {
                ssh.close();
            } catch (IOException ioEx) {
                throw new SshException(ioEx, storage);
            }
            throw new SshException(e, storage);
        } catch (Exception e) {
            try {
                if (ssh != null)
                    ssh.close();
            } catch (IOException ioEx) {
                throw new SshException(ioEx, storage);
            }
            throw e;
        }
    }

    @Override
    public void storeObject(ArchivalObjectDto objectDto, AtomicBoolean rollback, String dataSpace, @NonNull Instant operationTimestamp) throws StorageException {
        switch (objectDto.getState()) {
            case DELETION_FAILURE:
            case DELETED:
                delete(objectDto, dataSpace, operationTimestamp);
                break;
            case ARCHIVAL_FAILURE:
            case ROLLBACK_FAILURE:
            case ROLLED_BACK:
                rollbackObject(objectDto, dataSpace, operationTimestamp);
                break;
            case ARCHIVED:
            case PROCESSING:
            case REMOVED:
                try (SSHClient ssh = new SSHClient()) {
                    connect(ssh);
                    listenForRollbackToKillSession(ssh, rollback);
                    String objId = objectDto.getStorageId();
                    String folder = getFolderPath(objId, dataSpace);
                    try (SFTPClient sftp = ssh.newSFTPClient()) {
                        boolean newerObjectExists = checkIfNewerObjectExits(sftp, folder, objectDto.getStorageId(), operationTimestamp);
                        if (newerObjectExists) {
                            return;
                        }
                        storeFile(ssh, sftp, folder, objectDto.getStorageId(), objectDto.getInputStream(), objectDto.getChecksum(), rollback, objectDto.getCreated());
                        if (objectDto.getState() == ObjectState.REMOVED) {
                            setState(sftp, folder, objectDto, ObjectState.REMOVED, operationTimestamp);
                        }
                    }
                } catch (IOException e) {
                    rollback.set(true);
                    throw new SshException(e, storage);
                } catch (Exception e) {
                    rollback.set(true);
                    throw e;
                }
                break;
            default:
                throw new IllegalStateException(objectDto.toString());
        }
    }

    @Override
    public void storeObjectMetadata(ArchivalObjectDto objectDto, String dataSpace) throws SshException, IOStorageException {
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                String objFolderPath = getFolderPath(objectDto.getStorageId(), dataSpace);
                writeObjectMetadata(sftp, objFolderPath, new ObjectMetadata(objectDto.getStorageId(), objectDto.getState(), objectDto.getCreated(), objectDto.getChecksum()));
            }
        } catch (IOException e) {
            throw new SshException(e, storage);
        }
    }

    @Override
    public ObjectRetrievalResource getObject(String id, String dataSpace) throws StorageException {
        String objectFilePath = getFolderPath(id, dataSpace) + separator + id;
        SSHClient ssh = null;
        try {
            ssh = new SSHClient();
            connect(ssh);
            SFTPClient sftp = ssh.newSFTPClient();
            if (sftp.statExistence(objectFilePath) == null)
                throw new FileDoesNotExistException(objectFilePath, storage);
            InputStream inputStream = getFile(ssh, sftp, objectFilePath);
            return new ObjectRetrievalResource(inputStream, ssh);
        } catch (IOException e) {
            try {
                ssh.close();
            } catch (IOException ioEx) {
                throw new SshException(ioEx, storage);
            }
            throw new SshException(e, storage);
        } catch (Exception e) {
            try {
                if (ssh != null)
                    ssh.close();
            } catch (IOException ioEx) {
                throw new SshException(ioEx, storage);
            }
            throw e;
        }
    }

    @Override
    public void delete(ArchivalObjectDto sipDto, String dataSpace, @NonNull Instant operationTimestamp) throws StorageException {
        String sipFolder = getFolderPath(sipDto.getStorageId(), dataSpace);
        String sipFilePath = sipFolder + separator + sipDto.getStorageId();
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                boolean deleted = setState(sftp, sipFolder, sipDto, ObjectState.DELETED, operationTimestamp);
                if (deleted) {
                    deleteIfExistsSftp(sftp, sipFilePath);
                }
            }
        } catch (IOException e) {
            throw new SshException(e, storage);
        }
    }

    @Override
    public void remove(ArchivalObjectDto sipDto, String dataSpace, @NonNull Instant operationTimestamp) throws StorageException {
        String sipFolder = getFolderPath(sipDto.getStorageId(), dataSpace);
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                setState(sftp, sipFolder, sipDto, ObjectState.REMOVED, operationTimestamp);
            }
        } catch (IOException e) {
            throw new SshException(e, storage);
        }
    }

    @Override
    public void renew(ArchivalObjectDto sipDto, String dataSpace, @NonNull Instant operationTimestamp) throws StorageException {
        String sipFolder = getFolderPath(sipDto.getStorageId(), dataSpace);
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                setState(sftp, sipFolder, sipDto, ObjectState.ARCHIVED, operationTimestamp);
            }
        } catch (IOException e) {
            throw new SshException(e, storage);
        }
    }

    @Override
    public void rollbackAip(AipDto aipDto, String dataSpace, @NonNull Instant operationTimestamp) throws StorageException {
        String sipId = aipDto.getSip().getStorageId();
        String folder = getFolderPath(sipId, dataSpace);
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                rollbackFile(sftp, folder, aipDto.getSip(), operationTimestamp);
                for (ArchivalObjectDto xml : aipDto.getXmls()) {
                    rollbackFile(sftp, folder, xml, operationTimestamp);
                }
            }
        } catch (IOException e) {
            throw new SshException(e, storage);
        }
    }

    @Override
    public void rollbackObject(ArchivalObjectDto dto, String dataSpace, @NonNull Instant operationTimestamp) throws StorageException {
        String folder = getFolderPath(dto.getStorageId(), dataSpace);
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                rollbackFile(sftp, folder, dto, operationTimestamp);
            }
        } catch (IOException e) {
            throw new SshException(e, storage);
        }
    }

    @Override
    public void forgetObject(String objectIdAtStorage, String dataSpace, @NonNull Instant operationTimestamp) throws StorageException {
        String folder = getFolderPath(objectIdAtStorage, dataSpace);
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                ObjectMetadata objectMetadata = readObjectMetadata(sftp, folder, objectIdAtStorage);
                if (objectMetadata == null) {
                    objectMetadata = new ObjectMetadata(objectIdAtStorage, ObjectState.FORGOT, null, null);
                } else {
                    if (objectMetadata.getCreated() != null && objectMetadata.getCreated().truncatedTo(ChronoUnit.MILLIS).isAfter(operationTimestamp.truncatedTo(ChronoUnit.MILLIS))) {
                        log.info("skipped setting {} on {} as the object creation timestamp is newer then timestamp of the operation", ObjectState.FORGOT, objectIdAtStorage);
                        return;
                    }
                }
                objectMetadata.setState(ObjectState.FORGOT);
                writeObjectMetadata(sftp, folder, objectMetadata);
                deleteIfExistsSftp(sftp, folder + separator + objectIdAtStorage);
            }
        } catch (IOException e) {
            throw new SshException(e, storage);
        }
    }

    @Override
    public AipConsistencyVerificationResultDto getAipInfo(ArchivalObjectDto aip, Map<Integer, ArchivalObjectDto> xmls, String dataSpace) throws StorageException {
        AipConsistencyVerificationResultDto aipStateInfo = new AipConsistencyVerificationResultDto(storage.getName(), storage.getStorageType(), true);
        aipStateInfo.setAipState(fillObjectStateInfo(new ObjectConsistencyVerificationResultDto(), aip, dataSpace));
        for (Integer version : xmls.keySet()) {
            XmlConsistencyVerificationResultDto info = new XmlConsistencyVerificationResultDto();
            info.setVersion(version);
            fillObjectStateInfo(info, xmls.get(version), dataSpace);
            aipStateInfo.addXmlInfo(info);
        }
        return aipStateInfo;
    }

    @Override
    public StorageStateDto getStorageState() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createNewDataSpace(String dataSpace) throws IOStorageException {
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                sftp.mkdirs(rootDirPath + separator + dataSpace);
            }
        } catch (IOException e) {
            throw new IOStorageException(e, storage);
        }
    }

    @Override
    public List<ArchivalObjectDto> createDtosForAllObjects(String dataSpace) {
        throw new UnsupportedOperationException();
    }

    private ObjectConsistencyVerificationResultDto fillObjectStateInfo(ObjectConsistencyVerificationResultDto info, ArchivalObjectDto object, String dataSpace) throws FileDoesNotExistException, IOStorageException, SshException, CantParseMetadataFile, CmdProcessException {
        info.setStorageId(object.getStorageId());
        info.setState(object.getState());
        info.setDatabaseChecksum(object.getChecksum());
        info.setDatabaseId(object.getDatabaseId());
        info.setCreated(object.getCreated());
        if (!object.getState().metadataMustBeStoredAtLogicalStorage()) {
            return info;
        }
        String folder = getFolderPath(object.getStorageId(), dataSpace);

        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                ObjectMetadata metadataAtStorage = readObjectMetadata(sftp, folder, object.getStorageId());
                if (metadataAtStorage == null)
                    throw new FileDoesNotExistException(metadataFilePath(folder, object.getStorageId()), storage);
                boolean stateMetadataConsistent = metadataAtStorage.getState() == object.getState();
                boolean timestampMetadataConsistent = Objects.equals(object.getCreated().truncatedTo(ChronoUnit.MILLIS), metadataAtStorage.getCreated().truncatedTo(ChronoUnit.MILLIS));
                boolean checksumMetadataConsistent = object.getChecksum().equals(metadataAtStorage.getChecksum());
                info.setMetadataConsistent(stateMetadataConsistent && checksumMetadataConsistent && timestampMetadataConsistent);
                if (object.getState().contentMustBeStoredAtLogicalStorage()) {
                    checkFileExists(ssh, sftp, folder + separator + object.getStorageId());
                    Checksum storageFileChecksum = computeChecksum(ssh, sftp, folder + separator + object.getStorageId(), object.getChecksum().getType());
                    info.setStorageChecksum(storageFileChecksum);
                    info.setContentConsistent(object.getChecksum().equals(storageFileChecksum));
                }
            }
        } catch (IOException e) {
            throw new SshException(e, storage);
        }
        return info;
    }

    /**
     * pipes data from remote location to the consumer which consumes the returned inputstream
     * <p>
     * The pipe is needed because SSHJ API retrieves outputstream instead of inpustream (which is needed by consumer).
     * The file may be large, so it can't be written first and read afterwards.
     * Using pipes, one thread reads the data into memory until it fills the buffer and then waits for the second thread
     * (the main - consumer's thread) to read it. When consumer reads, the writing thread writes next bytes.
     * </p>
     *
     * @param ssh
     * @param sftp
     * @param pathToFile
     * @return
     * @throws IOException
     * @throws FileDoesNotExistException
     */
    private InputStream getFile(SSHClient ssh, SFTPClient sftp, String pathToFile) throws IOException, FileDoesNotExistException {
        PipedInputStream in = new PipedInputStream();
        checkFileExists(ssh, sftp, pathToFile);
        AtomicBoolean wait = new AtomicBoolean(true);
        new Thread(() -> {
            try (PipedOutputStream out = new PipedOutputStream(in)) {
                wait.set(false);
                sftp.get(pathToFile, new SshjOutputStreamSource(out));
            } catch (IOException e) {
                IOUtils.closeQuietly(ssh);
                throw new UncheckedIOException(e);
            } catch (Exception e) {
                IOUtils.closeQuietly(ssh);
                throw e;
            } finally {
                wait.set(false);
            }
        }).start();
        //wait for pipe to be connected
        while (wait.get()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return in;
    }

    @Override
    public ArchivalObjectDto verifyStateOfObjects(List<ArchivalObjectDto> objects, AtomicLong counter) throws StorageException {
        SSHClient ssh = null;
        try {
            ssh = new SSHClient();
            connect(ssh);
            SFTPClient sftp = ssh.newSFTPClient();
            for (ArchivalObjectDto inputObject : objects) {
                if (!inputObject.getState().metadataMustBeStoredAtLogicalStorage()) {
                    counter.incrementAndGet();
                    continue;
                }
                String folder = getFolderPath(inputObject.getStorageId(), inputObject.getDataSpace());
                ObjectMetadata metadataAtStorage = readObjectMetadata(sftp, folder, inputObject.getStorageId());
                if (metadataAtStorage == null || metadataAtStorage.getState() != inputObject.getState())
                    return inputObject;
                counter.incrementAndGet();
            }
        } catch (IOException e) {
            try {
                ssh.close();
            } catch (IOException ioEx) {
                throw new SshException(ioEx, storage);
            }
            throw new SshException(e, storage);
        } catch (Exception e) {
            try {
                if (ssh != null)
                    ssh.close();
            } catch (IOException ioEx) {
                throw new SshException(ioEx, storage);
            }
            throw e;
        }
        return null;
    }

    /**
     * Stores file and then reads it and verifies its fixity.
     * <p>
     * If rollback is set to true by another thread, this method returns ASAP (without throwing exception), leaving the file uncompleted but closing stream.  Uncompleted files are to be cleaned during rollback.
     * </p>
     * <p>
     * In case of any exception, rollback flag is set to true.
     * </p>
     */
    void storeFile(SSHClient ssh, SFTPClient sftp, String folder, String id, InputStream stream, Checksum checksum, AtomicBoolean rollback, Instant created) throws FileCorruptedAfterStoreException, IOStorageException, FileDoesNotExistException, CmdProcessException, SshException {
        try {
            if (rollback.get())
                return;
            writeObjectMetadata(sftp, folder, new ObjectMetadata(id, ObjectState.PROCESSING, created, checksum));
            sftp.put(new InputStreamSource(stream, id), folder);
            boolean rollbackInterruption = !verifyChecksum(ssh, sftp, folder + separator + id, checksum, rollback);
            if (rollbackInterruption) {
                return;
            }
            writeObjectMetadata(sftp, folder, new ObjectMetadata(id, ObjectState.ARCHIVED, created, checksum));
        } catch (IOException e) {
            rollback.set(true);
            throw new IOStorageException(e, storage);
        } catch (Exception e) {
            if (e instanceof FileCorruptedAfterStoreException)
                throw e;
            rollback.set(true);
            throw new GeneralException(e);
        }
    }

    /**
     * Called by saveAip methods. Checks for rollback flag each second and if rollback is set to true,
     * kills ssh connection so that method using that connection will immediately stop with ssh connection exception.
     *
     * @param ssh
     * @param rollback
     */
    private void listenForRollbackToKillSession(SSHClient ssh, AtomicBoolean rollback) {
        new Thread(() -> {
            try {
                while (ssh.isConnected()) {
                    if (rollback.get()) {
                        ssh.disconnect();
                        return;
                    }
                    Thread.sleep(1000);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (InterruptedException e) {
                Logger.getLogger(RemoteFsProcessor.class).error("Fatal error: parallel process interrupted");
            }
        }).start();
    }

    private void deleteIfExistsSftp(SFTPClient sftp, String filePath) throws IOException {
        if (sftp.statExistence(filePath) != null)
            sftp.rm(filePath);
    }

    String getFolderPath(String fileName, String dataSpace) {
        return rootDirPath + separator + dataSpace + separator + fileName.substring(0, 2) + separator + fileName.substring(2, 4) + separator + fileName.substring(4, 6);
    }

    /**
     * throws exception if file does not exists AND closes connection
     *
     * @param ssh
     * @param sftp
     * @param pathToFile
     * @throws FileDoesNotExistException
     * @throws SSHException
     */
    void checkFileExists(SSHClient ssh, SFTPClient sftp, String pathToFile) throws FileDoesNotExistException, SSHException {
        try {
            if (sftp.statExistence(pathToFile) == null) {
                IOUtils.closeQuietly(ssh);
                throw new FileDoesNotExistException(pathToFile, storage);
            }
        } catch (IOException e) {
            IOUtils.closeQuietly(ssh);
            throw new SSHException(e);
        }
    }

    private String readSmallFile(SSHClient ssh, SFTPClient sftp, String pathToFile) {
        String content = null;
        File tmp = null;
        try {
            tmp = File.createTempFile("sshread", null);
            tmp.deleteOnExit();
            sftp.get(pathToFile, tmp.getAbsolutePath());
            content = new String(Files.readAllBytes(tmp.toPath()), StandardCharsets.UTF_8);
        } catch (IOException ignored) {
        } finally {
            if (tmp != null)
                tmp.delete();
        }
        return content;
    }

    /**
     * @return true if the state was set, false if not (operation skipped)
     */
    private boolean setState(SFTPClient sftp, String folder, ArchivalObjectDto object, ObjectState state, @NonNull Instant operationTimestamp) throws CantParseMetadataFile, IOStorageException {
        ObjectMetadata objectMetadata = readObjectMetadata(sftp, folder, object.getStorageId());
        if (objectMetadata == null) {
            objectMetadata = new ObjectMetadata(object.getStorageId(), state, object.getCreated(), object.getChecksum());
        } else {
            if (objectMetadata.getCreated() != null && objectMetadata.getCreated().truncatedTo(ChronoUnit.MILLIS).isAfter(operationTimestamp.truncatedTo(ChronoUnit.MILLIS))) {
                log.info("skipped setting {} on {} as the object creation timestamp is newer then timestamp of the operation", state, object.getStorageId());
                return false;
            }
        }
        objectMetadata.setState(state);
        writeObjectMetadata(sftp, folder, objectMetadata);
        return true;
    }

    /**
     * @param sftp
     * @param folder
     * @param fileId
     * @return null if the metadata file does not exist
     * @throws IOStorageException
     * @throws CantParseMetadataFile
     */
    ObjectMetadata readObjectMetadata(SFTPClient sftp, String folder, String fileId) throws IOStorageException, CantParseMetadataFile {
        String filePath = metadataFilePath(folder, fileId);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            if (sftp.statExistence(filePath) == null)
                return null;
            sftp.get(filePath, new SshjOutputStreamSource(bos));
        } catch (IOException e) {
            throw new IOStorageException(e, storage);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bos.toByteArray())));
        List<String> lines = reader.lines().collect(Collectors.toList());
        return new ObjectMetadata(lines, fileId, storage);
    }

    /**
     * overwrites the whole metadata file
     *
     * @param sftp
     * @param folder
     * @param objectMetadata
     * @throws IOStorageException
     */
    void writeObjectMetadata(SFTPClient sftp, String folder, ObjectMetadata objectMetadata) throws IOStorageException {
        try {
            sftp.mkdirs(folder);
            sftp.put(new InputStreamSource(new ByteArrayInputStream(objectMetadata.serialize()), objectMetadata.getStorageId() + ".meta"), folder);
        } catch (IOException e) {
            throw new IOStorageException(e, null);
        }
    }

    private boolean checkIfNewerObjectExits(SFTPClient sftp, String folder, String objectIdAtStorage, Instant timestampToCompareWith) throws CantParseMetadataFile, IOStorageException {
        ObjectMetadata objectMetadata = readObjectMetadata(sftp, folder, objectIdAtStorage);
        if (objectMetadata != null) {
            if (objectMetadata.getCreated() != null && objectMetadata.getCreated().truncatedTo(ChronoUnit.MILLIS).isAfter(timestampToCompareWith.truncatedTo(ChronoUnit.MILLIS))) {
                return true;
            }
        }
        return false;
    }

    void rollbackFile(SFTPClient sftp, String folder, ArchivalObjectDto dto, @NonNull Instant operationTimestamp) throws IOException, CantParseMetadataFile, IOStorageException {
        boolean rolledBack = setState(sftp, folder, dto, ObjectState.ROLLED_BACK, operationTimestamp);
        if (rolledBack) {
            deleteIfExistsSftp(sftp, folder + separator + dto.getStorageId());
        }
    }

    private String metadataFilePath(String folder, String fileId) {
        return folder + separator + fileId + ".meta";
    }

    public static class InputStreamSource extends InMemorySourceFile {

        @Getter
        private final String name;
        private final InputStream inputStream;

        public InputStreamSource(InputStream is, String name) {
            this.inputStream = is;
            this.name = name;
        }


        public long getLength() {
            return -1;
        }


        public InputStream getInputStream() throws IOException {
            return inputStream;
        }
    }

    private void connect(SSHClient ssh) throws IOException {
        ssh.addHostKeyVerifier(new PromiscuousVerifier());
        ssh.setConnectTimeout(connectionTimeout);
        ssh.connect(storage.getHost(), storage.getPort());
        ssh.authPublickey(sshUserName, sshKeyFilePath);
    }

    private Checksum computeChecksum(SSHClient ssh, SFTPClient sftp, String pathToFile, ChecksumType checksumType) throws FileDoesNotExistException, IOException, IOStorageException, CmdProcessException, SshException {
        String cliCmd = this.optimizedChecksumComputationCommands.get(checksumType);
        if (cliCmd == null) {
            return StorageUtils.computeChecksum(getFile(ssh, sftp, pathToFile), checksumType);
        }
        cliCmd = cliCmd.replace(OPTIMIZED_CHECKSUM_CMD_FILEPATH_PLACEHOLDER, pathToFile);
        List<String> strings = Utils.fetchDataFromRemote(ssh, cliCmd, storage);
        return new Checksum(checksumType, strings.get(0));
    }

    /**
     * same as {@link StorageService#verifyChecksum(InputStream, Checksum, AtomicBoolean, Storage)} just with the option of optimized computation
     */
    private boolean verifyChecksum(SSHClient ssh, SFTPClient sftp, String pathToFile, Checksum expectedChecksum, AtomicBoolean rollback) throws IOStorageException, FileCorruptedAfterStoreException, IOException, FileDoesNotExistException, CmdProcessException, SshException {
        if (optimizedChecksumComputationCommands.containsKey(expectedChecksum.getType())) {
            Checksum checksumAtStorage = computeChecksum(ssh, sftp, pathToFile, expectedChecksum.getType());
            if (rollback.get()) {
                return false;
            } else {
                if (!checksumAtStorage.getValue().equalsIgnoreCase(expectedChecksum.getValue())) {
                    rollback.set(true);
                    throw new FileCorruptedAfterStoreException(checksumAtStorage, expectedChecksum, storage);
                }
                return true;
            }
        } else {
            PipedInputStream in = new PipedInputStream();
            PipedOutputStream out = new PipedOutputStream(in);
            new Thread(() -> {
                try {
                    sftp.get(pathToFile, new SshjOutputStreamSource(out));
                } catch (IOException e) {
                    rollback.set(true);
                    throw new GeneralException(new IOStorageException(e, storage));
                } catch (Exception e) {
                    rollback.set(true);
                    throw new GeneralException(e);
                }
            }).start();
            return verifyChecksum(in, expectedChecksum, rollback, storage);
        }
    }
}
