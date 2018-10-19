package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.StorageUtils;
import cz.cas.lib.arcstorage.storage.exception.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.SSHException;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.FilePermission;
import net.schmizz.sshj.xfer.InMemoryDestFile;
import net.schmizz.sshj.xfer.InMemorySourceFile;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;

/**
 * implementation used by {@link FsAdapter} to provide {@link ZfsStorageService} and {@link FsStorageService} with methods
 * for access to the remote FS/ZFS over SFTP
 */
@Slf4j
public class RemoteFsProcessor implements StorageService {

    @Getter
    private Storage storage;
    private String S;
    private String keyFilePath;
    private String rootDirPath;
    private static final String USER = "arcstorage";
    private int connectionTimeout;

    public RemoteFsProcessor(Storage storage, String rootDirPath, String separator, String keyFilePath, int connectionTimeout) {
        this.storage = storage;
        this.S = separator;
        this.keyFilePath = keyFilePath;
        this.connectionTimeout = connectionTimeout;
        this.rootDirPath = rootDirPath;
    }

    public String getSeparator() {
        return S;
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
                storeFile(sftp, folder, toXmlId(aip.getSip().getDatabaseId(), 1), aip.getXml().getInputStream(), aip.getXml().getChecksum(), rollback);
                storeFile(sftp, folder, aip.getSip().getDatabaseId(), aip.getSip().getInputStream(), aip.getSip().getChecksum(), rollback);
            }
        } catch (IOException e) {
            rollback.set(true);
            throw new SshException(e);
        } catch (Exception e) {
            rollback.set(true);
            throw new GeneralException(e);
        }
    }

    @Override
    public AipRetrievalResource getAip(String aipId, String dataSpace, Integer... xmlVersions) throws FileDoesNotExistException, StorageException {
        SSHClient ssh = null;
        try {
            ssh = new SSHClient();
            connect(ssh);
            SFTPClient sftp = ssh.newSFTPClient();
            AipRetrievalResource aip = new AipRetrievalResource(ssh);
            String folder = getFolderPath(aipId, dataSpace);
            aip.setSip(getFile(ssh, sftp, folder + S + aipId));
            for (Integer xmlVersion : xmlVersions) {
                aip.addXml(xmlVersion, getFile(ssh, sftp, folder + S + toXmlId(aipId, xmlVersion)));
            }
            return aip;
        } catch (IOException e) {
            try {
                ssh.close();
            } catch (IOException ioEx) {
                throw new SshException(ioEx);
            }
            throw new SshException(e);
        } catch (Exception e) {
            try {
                if (ssh != null)
                    ssh.close();
            } catch (IOException ioEx) {
                throw new SshException(ioEx);
            }
            throw e;
        }
    }

    @Override
    public void storeObject(ArchivalObjectDto objectDto, AtomicBoolean rollback, String dataSpace) throws StorageException {
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            listenForRollbackToKillSession(ssh, rollback);
            String objId = objectDto.getStorageId();
            String folderPath = getFolderPath(objId, dataSpace);
            try (SFTPClient sftp = ssh.newSFTPClient()) {

                switch (objectDto.getState()) {
                    case ARCHIVAL_FAILURE:
                        throw new IllegalArgumentException("trying to store object " + objId + " which is in failed state");
                    case DELETION_FAILURE:
                        objectDto.setState(ObjectState.DELETED);
                    case ROLLED_BACK:
                    case DELETED:
                        setState(sftp, folderPath, objId, objectDto.getState());
                        break;
                    case REMOVED:
                        storeFile(sftp, folderPath, objId, objectDto.getInputStream(), objectDto.getChecksum(), rollback);
                        remove(objId, dataSpace);
                        break;
                    case ARCHIVED:
                    case PROCESSING:
                        storeFile(sftp, folderPath, objId, objectDto.getInputStream(), objectDto.getChecksum(), rollback);
                        break;
                    default:
                        throw new IllegalStateException(objectDto.toString());
                }
            }
        } catch (IOException e) {
            rollback.set(true);
            throw new SshException(e);
        }
    }

    @Override
    public ObjectRetrievalResource getObject(String id, String dataSpace) throws FileDoesNotExistException, StorageException {
        String objectFilePath = getFolderPath(id, dataSpace) + S + id;
        SSHClient ssh = null;
        try {
            ssh = new SSHClient();
            connect(ssh);
            SFTPClient sftp = ssh.newSFTPClient();
            InputStream inputStream = getFile(ssh, sftp, objectFilePath);
            return new ObjectRetrievalResource(inputStream, ssh);
        } catch (IOException e) {
            try {
                ssh.close();
            } catch (IOException ioEx) {
                throw new SshException(ioEx);
            }
            throw new SshException(e);
        } catch (Exception e) {
            try {
                if (ssh != null)
                    ssh.close();
            } catch (IOException ioEx) {
                throw new SshException(ioEx);
            }
            throw e;
        }
    }

    @Override
    public void delete(String sipId, String dataSpace) throws StorageException {
        String sipFolder = getFolderPath(sipId, dataSpace);
        String sipFilePath = sipFolder + S + sipId;
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                setState(sftp, sipFolder, sipId, ObjectState.PROCESSING);
                deleteIfExistsSftp(sftp, sipFilePath);
                transitProcessingState(sftp, sipFolder, sipId, ObjectState.DELETED);
            }
        } catch (IOException e) {
            throw new SshException(e);
        }
    }

    @Override
    public void remove(String sipId, String dataSpace) throws StorageException {
        String sipFolder = getFolderPath(sipId, dataSpace);
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                transitState(sftp, sipFolder, sipId, ObjectState.ARCHIVED, ObjectState.REMOVED);
            }
        } catch (IOException e) {
            throw new SshException(e);
        }
    }

    @Override
    public void renew(String sipId, String dataSpace) throws StorageException {
        String sipFolder = getFolderPath(sipId, dataSpace);
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                transitState(sftp, sipFolder, sipId, ObjectState.REMOVED, ObjectState.ARCHIVED);

            }
        } catch (IOException e) {
            throw new SshException(e);
        }
    }

    @Override
    public void rollbackAip(String sipId, String dataSpace) throws StorageException {
        String folder = getFolderPath(sipId, dataSpace);
        String xmlId = toXmlId(sipId, 1);
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                rollbackFile(sftp, folder, sipId);
                rollbackFile(sftp, folder, xmlId);
            }
        } catch (IOException e) {
            throw new SshException(e);
        }
    }

    @Override
    public void rollbackObject(String id, String dataSpace) throws StorageException {
        String folder = getFolderPath(id, dataSpace);
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                rollbackFile(sftp, folder, id);
            }
        } catch (IOException e) {
            throw new SshException(e);
        }
    }

    @Override
    public AipStateInfoDto getAipInfo(String aipId, Checksum sipChecksum, ObjectState objectState, Map<Integer, Checksum> xmlVersions, String dataSpace) throws StorageException {
        String folder = getFolderPath(aipId, dataSpace);
        AipStateInfoDto info = new AipStateInfoDto(storage.getName(), storage.getStorageType(), objectState, sipChecksum, true);
        try (SSHClient ssh = new SSHClient()) {
            connect(ssh);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                if (objectState == ObjectState.ARCHIVED || objectState == ObjectState.REMOVED) {
                    Checksum storageFileChecksum = StorageUtils.computeChecksum(getFile(ssh, sftp, folder + S + aipId), sipChecksum.getType());
                    info.setSipStorageChecksum(storageFileChecksum);
                    info.setConsistent(sipChecksum.equals(storageFileChecksum));
                } else {
                    info.setSipStorageChecksum(null);
                    info.setConsistent(false);
                }

                for (Integer version : xmlVersions.keySet()) {
                    Checksum dbChecksum = xmlVersions.get(version);
                    Checksum storageFileChecksum = StorageUtils.computeChecksum(getFile(ssh, sftp, folder + S + toXmlId(aipId, version)), dbChecksum.getType());
                    info.addXmlInfo(new XmlStateInfoDto(version, dbChecksum.equals(storageFileChecksum), storageFileChecksum, dbChecksum));
                }
            }
        } catch (IOException e) {
            throw new SshException(e);
        }
        return info;
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
                sftp.mkdirs(rootDirPath + S + dataSpace);
            }
        } catch (IOException e) {
            throw new IOStorageException(e);
        }
    }

    private InputStream getFile(SSHClient ssh, SFTPClient sftp, String pathToFile) throws IOException, FileDoesNotExistException {
        PipedInputStream in = new PipedInputStream();
        checkFileExists(ssh, sftp, pathToFile);
        new Thread(() -> {
            try (PipedOutputStream out = new PipedOutputStream(in)) {
                sftp.get(pathToFile, new OutputStreamSource(out));
            } catch (IOException e) {
                IOUtils.closeQuietly(ssh);
                throw new UncheckedIOException(e);
            } catch (Exception e) {
                IOUtils.closeQuietly(ssh);
                throw e;
            }
        }).start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return in;
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
    void storeFile(SFTPClient sftp, String folder, String id, InputStream stream, Checksum checksum, AtomicBoolean rollback) throws FileCorruptedAfterStoreException, IOStorageException {
        try {
            if (rollback.get())
                return;
            sftp.mkdirs(folder);
            setState(sftp, folder, id, ObjectState.PROCESSING);
            sftp.put(new InputStreamSource(new ByteArrayInputStream(checksum.getValue().getBytes()), id + "." + checksum.getType()), folder);
            sftp.put(new InputStreamSource(stream, id), folder);
            PipedInputStream in = new PipedInputStream();
            PipedOutputStream out = new PipedOutputStream(in);
            new Thread(() -> {
                try {
                    sftp.get(folder + S + id, new OutputStreamSource(out));
                } catch (IOException e) {
                    rollback.set(true);
                    throw new GeneralException(new IOStorageException(e));
                } catch (Exception e) {
                    rollback.set(true);
                    throw new GeneralException(e);
                }
            }).start();
            boolean rollbackInterruption = !verifyChecksum(in, checksum, rollback);
            if (rollbackInterruption)
                return;
            transitProcessingState(sftp, folder, id, ObjectState.ARCHIVED);
        } catch (IOException e) {
            rollback.set(true);
            throw new IOStorageException(e);
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
    //todo: it will be better to write data using buffer & periodic rollback flag instead because:
    // a) using this approach, the exception is thrown twice: once by the method setting rollback and second time by this one so its not clear from the log which storage has failed
    // b) its better to solve bussiness error other way than using force
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
        return rootDirPath + S + dataSpace + S + fileName.substring(0, 2) + S + fileName.substring(2, 4) + S + fileName.substring(4, 6);
    }

    void checkFileExists(SSHClient ssh, SFTPClient sftp, String pathToFile) throws FileDoesNotExistException, SSHException {
        try {
            if (sftp.statExistence(pathToFile) == null) {
                IOUtils.closeQuietly(ssh);
                throw new FileDoesNotExistException(pathToFile);
            }
        } catch (IOException e) {
            IOUtils.closeQuietly(ssh);
            throw new SSHException(e);
        }
    }

    private String toStateStr(String fileId, ObjectState objectState) {
        return fileId + "." + objectState.toString();
    }

    private void setState(SFTPClient sftp, String folder, String fileId, ObjectState state) throws IOException {
        sftp.put(new InputStreamSource(new ByteArrayInputStream("".getBytes()), fileId + "." + state), folder);
    }

    private void transitProcessingState(SFTPClient sftp, String folder, String fileId, ObjectState newState) throws IOException {
        transitState(sftp, folder, fileId, ObjectState.PROCESSING, newState);
    }

    private void transitState(SFTPClient sftp, String folder, String fileId, ObjectState oldState, ObjectState newState) throws IOException {
        if (sftp.statExistence(folder + S + fileId + "." + newState) != null)
            deleteIfExistsSftp(sftp, folder + S + fileId + "." + oldState);
        else
            sftp.rename(folder + S + toStateStr(fileId, oldState), folder + S + toStateStr(fileId, newState));
    }

    void rollbackFile(SFTPClient sftp, String folder, String fileId) throws IOException {
        sftp.mkdirs(folder);
        setState(sftp, folder, fileId, ObjectState.PROCESSING);
        deleteIfExistsSftp(sftp, folder + S + toStateStr(fileId, ObjectState.REMOVED));
        deleteIfExistsSftp(sftp, folder + S + toStateStr(fileId, ObjectState.ROLLED_BACK));
        deleteIfExistsSftp(sftp, folder + S + toStateStr(fileId, ObjectState.DELETED));
        deleteIfExistsSftp(sftp, folder + S + fileId);
        transitProcessingState(sftp, folder, fileId, ObjectState.ROLLED_BACK);
    }

    public static class InputStreamSource extends InMemorySourceFile {

        private String name;
        private InputStream inputStream;

        public InputStreamSource(InputStream is, String name) {
            this.inputStream = is;
            this.name = name;
        }


        public String getName() {
            return name;
        }


        public long getLength() {
            return -1;
        }


        public InputStream getInputStream() throws IOException {
            return inputStream;
        }
    }

    public static class OutputStreamSource extends InMemoryDestFile {

        private OutputStream outputStream;

        public OutputStreamSource(OutputStream os) {
            this.outputStream = os;
        }


        public OutputStream getOutputStream() throws IOException {
            return outputStream;
        }
    }

    private void connect(SSHClient ssh) throws IOException {
        ssh.addHostKeyVerifier(new PromiscuousVerifier());
        ssh.setConnectTimeout(connectionTimeout);
        ssh.connect(storage.getHost(), storage.getPort());
        ssh.authPublickey(USER, keyFilePath);
    }
}
