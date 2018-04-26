package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.ChecksumType;
import cz.cas.lib.arcstorage.domain.ObjectState;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.gateway.dto.*;
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
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class RemoteFsProcessor implements StorageService {

    @Getter
    private StorageConfig storageConfig;
    private String S;
    private String keyFilePath;

    public RemoteFsProcessor(StorageConfig storageConfig, String separator, String keyFilePath) {
        this.storageConfig = storageConfig;
        this.S = separator;
        this.keyFilePath = keyFilePath;
    }

    @Override
    public boolean testConnection() {
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            ssh.authPublickey("arcstorage", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                Set<net.schmizz.sshj.xfer.FilePermission> perms = sftp.perms(storageConfig.getLocation());
                return (perms.contains(FilePermission.GRP_R) || perms.contains(FilePermission.USR_R)) &&
                        perms.contains(FilePermission.GRP_W) || perms.contains(FilePermission.USR_W);
            }
        } catch (Exception e) {
            log.error(storageConfig.getName() + " unable to connect: " + e.getClass() + " " + e.getMessage());
            return false;
        }
    }

    @Override
    public void storeAip(AipDto aip, AtomicBoolean rollback) throws StorageException {
        String sipFolder = getSipFolderPath(aip.getSip().getId());
        String xmlFolder = getXmlFolderPath(aip.getSip().getId());
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            listenForRollbackToKillSession(ssh, rollback);
            ssh.authPublickey("arcstorage", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                storeFile(sftp, xmlFolder, toXmlId(aip.getSip().getId(), 1), S, aip.getXml().getInputStream(), aip.getXml().getChecksum(), rollback);
                storeFile(sftp, sipFolder, aip.getSip().getId(), S, aip.getSip().getInputStream(), aip.getSip().getChecksum(), rollback);
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
    public List<FileContentDto> getAip(String sipId, Integer... xmlVersions) throws FileDoesNotExistException, StorageException {
        SSHClient ssh = null;
        List<FileContentDto> list = new ArrayList<>();
        try {
            ssh = new SSHClient();
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            ssh.authPublickey("arcstorage", keyFilePath);
            SFTPClient sftp = ssh.newSFTPClient();
            list.add(new FileContentDto(getFile(ssh, sftp, getSipFolderPath(sipId) + S + sipId), ssh));
            String xmlFolder = getXmlFolderPath(sipId);
            for (Integer xmlVersion : xmlVersions) {
                list.add(new FileContentDto(getFile(ssh, sftp, xmlFolder + S + toXmlId(sipId, xmlVersion)), ssh));
            }
            return list;
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
            throw new GeneralException(e);
        }
    }

    @Override
    public void storeXml(String sipId, XmlDto xml, AtomicBoolean rollback) throws StorageException {
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            listenForRollbackToKillSession(ssh, rollback);
            ssh.authPublickey("arcstorage", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                storeFile(sftp, getXmlFolderPath(sipId), toXmlId(sipId, xml.getVersion()), S, xml.getInputStream(), xml.getChecksum(), rollback);
            }
        } catch (IOException e) {
            rollback.set(true);
            throw new SshException(e);
        }
    }

    @Override
    public FileContentDto getXml(String sipId, int version) throws FileDoesNotExistException, StorageException {
        String xmlFilePath = getXmlFolderPath(sipId) + S + toXmlId(sipId, version);
        SSHClient ssh = null;
        try {
            ssh = new SSHClient();
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            ssh.authPublickey("arcstorage", keyFilePath);
            SFTPClient sftp = ssh.newSFTPClient();
            return new FileContentDto(getFile(ssh, sftp, xmlFilePath), ssh);
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
            throw new GeneralException(e);
        }
    }

    @Override
    public void storeSip(ArchivalObjectDto aipRef, AtomicBoolean rollback) throws StorageException {
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            listenForRollbackToKillSession(ssh, rollback);
            ssh.authPublickey("arcstorage", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                storeFile(sftp, getSipFolderPath(aipRef.getId()), aipRef.getId(), S, aipRef.getInputStream(), aipRef.getChecksum(), rollback);
            }
        } catch (IOException e) {
            rollback.set(true);
            throw new SshException(e);
        }
    }

    @Override
    public void deleteSip(String sipId) throws StorageException {
        String sipFolder = getSipFolderPath(sipId);
        String sipFilePath = sipFolder + S + sipId;
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            ssh.authPublickey("arcstorage", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                checkFixityMetadataExists(sftp, sipFilePath);
                setState(sftp, sipFolder, sipId, ObjectState.PROCESSING);
                deleteIfExistsSftp(sftp, sipFilePath);
                transitProcessingState(sftp, sipFolder, sipId, ObjectState.DELETED);
            }
        } catch (IOException e) {
            throw new SshException(e);
        }
    }

    @Override
    public void remove(String sipId) throws StorageException {
        String sipFolder = getSipFolderPath(sipId);
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            ssh.authPublickey("arcstorage", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                checkFixityMetadataExists(sftp, sipFolder + S + sipId);
                setState(sftp, sipFolder, sipId, ObjectState.REMOVED);
            }
        } catch (IOException e) {
            throw new SshException(e);
        }
    }

    @Override
    public void rollbackAip(String sipId) throws StorageException {
        String sipFolder = getSipFolderPath(sipId);
        String xmlFolder = getXmlFolderPath(sipId);
        String xmlId = toXmlId(sipId, 1);
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            ssh.authPublickey("arcstorage", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                rollbackFile(sftp, sipFolder, sipId);
                rollbackFile(sftp, xmlFolder, xmlId);
            }
        } catch (IOException e) {
            throw new SshException(e);
        }
    }

    @Override
    public void rollbackXml(String sipId, int version) throws StorageException {
        String xmlFolder = getXmlFolderPath(sipId);
        String xmlId = toXmlId(sipId, version);
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            ssh.authPublickey("arcstorage", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                rollbackFile(sftp, xmlFolder, xmlId);
            }
        } catch (IOException e) {
            throw new SshException(e);
        }
    }

    @Override
    public AipStateInfoDto getAipInfo(String sipId, Checksum sipChecksum, ObjectState objectState, Map<Integer, Checksum> xmlVersions) throws StorageException {
        String sipFolder = getSipFolderPath(sipId);
        String xmlFolder = getXmlFolderPath(sipId);
        AipStateInfoDto info = new AipStateInfoDto(storageConfig.getName(), storageConfig.getStorageType(), objectState, sipChecksum);
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            ssh.authPublickey("arcstorage", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                if (objectState == ObjectState.ARCHIVED || objectState == ObjectState.REMOVED) {
                    Checksum storageFileChecksum = StorageUtils.computeChecksum(getFile(ssh, sftp, sipFolder + S + sipId), sipChecksum.getType());
                    info.setStorageChecksum(storageFileChecksum);
                    info.setConsistent(sipChecksum.equals(storageFileChecksum));
                } else {
                    info.setStorageChecksum(null);
                    info.setConsistent(false);
                }

                for (Integer version : xmlVersions.keySet()) {
                    Checksum dbChecksum = xmlVersions.get(version);
                    Checksum storageFileChecksum = StorageUtils.computeChecksum(getFile(ssh, sftp, xmlFolder + S + toXmlId(sipId, version)), dbChecksum.getType());
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

    private InputStream getFile(SSHClient ssh, SFTPClient sftp, String pathToFile) throws IOException, IOStorageException, FileDoesNotExistException {
        PipedInputStream in = new PipedInputStream();
        AtomicBoolean wait = new AtomicBoolean(true);
        checkFileExists(ssh, sftp, pathToFile);
        new Thread(() -> {
            try (PipedOutputStream out = new PipedOutputStream(in)) {
                sftp.get(pathToFile, new OutputStreamSource(out));
            } catch (IOException e) {
                throw new RuntimeException(new IOStorageException());
            } catch (Exception e) {
                throw e;
            } finally {
                wait.set(false);
            }
        }).start();
        while (wait.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new GeneralException(e);
            }
        }
        return in;
    }

    /**
     * Stores file and then reads it and verifies its fixity.
     * <p>
     * If rollback is set to true by another thread, this method returns ASAP (it may throw exception), leaving the file uncompleted but closing stream. Uncompleted files are to be cleaned during rollback.
     * </p>
     * <p>
     * In case of any exception, rollback flag is set to true.
     * </p>
     *
     * @param folder   path to new file folder
     * @param id       id of new file
     * @param S        platform separator i.e. / or \
     * @param stream   new file stream
     * @param checksum storageChecksum of the file
     * @param rollback rollback flag to be periodically checked
     * @throws FileCorruptedAfterStoreException if fixity does not match after store
     * @throws IOStorageException               in case of {@link IOException}
     * @throws GeneralException                 in case of any unexpected error
     */
    private void storeFile(SFTPClient sftp, String folder, String id, String S, InputStream stream, Checksum checksum, AtomicBoolean rollback) throws FileCorruptedAfterStoreException, IOStorageException {
        try {
            if (rollback.get())
                return;
            sftp.mkdirs(folder);
            setState(sftp, folder, id, ObjectState.PROCESSING);
            sftp.put(new InputStreamSource(new ByteArrayInputStream(checksum.getHash().getBytes()), id + "." + checksum.getType()), folder);
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
            rollback.set(true);
            throw new GeneralException(e);
        }
    }

    /**
     * Called by store methods. Checks for rollback flag each second and if rollback is set to true,
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

    private String getSipFolderPath(String fileName) {
        return storageConfig.getLocation() + S + "sip" + S + fileName.substring(0, 2) + S + fileName.substring(2, 4) + S + fileName.substring(4, 6);
    }

    private String getXmlFolderPath(String fileName) {
        return storageConfig.getLocation() + S + "xml" + S + fileName.substring(0, 2) + S + fileName.substring(2, 4) + S + fileName.substring(4, 6);
    }

    private void checkFixityMetadataExists(SFTPClient sftp, String filePath) throws FileDoesNotExistException {
        boolean exists = Arrays.stream(ChecksumType.values())
                .anyMatch(ct -> {
                    try {
                        return
                                sftp.statExistence(filePath + "." + ct.toString()) != null;
                    } catch (IOException e) {
                        return false;
                    }
                });
        if (!exists)
            throw new FileDoesNotExistException("metadata of file " + filePath);
    }

    private void checkFileExists(SSHClient ssh, SFTPClient sftp, String pathToFile) throws FileDoesNotExistException, SSHException {
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
        sftp.put(new InputStreamSource(new ByteArrayInputStream("".getBytes()), fileId + state), folder);
    }

    private void transitProcessingState(SFTPClient sftp, String folder, String fileId, ObjectState newState) throws IOException {
        sftp.rename(folder + S + toStateStr(fileId, ObjectState.PROCESSING), folder + S + toStateStr(fileId, newState));
    }

    private void rollbackFile(SFTPClient sftp, String folder, String fileId) throws IOException {
        sftp.mkdirs(folder);
        sftp.put(new InputStreamSource(new ByteArrayInputStream("".getBytes()), toStateStr(fileId, ObjectState.PROCESSING)), folder);
        deleteIfExistsSftp(sftp, folder + S + toStateStr(fileId, ObjectState.REMOVED));
        deleteIfExistsSftp(sftp, folder + S + toStateStr(fileId, ObjectState.ROLLBACKED));
        deleteIfExistsSftp(sftp, folder + S + toStateStr(fileId, ObjectState.DELETED));
        deleteIfExistsSftp(sftp, folder + S + fileId);
        transitProcessingState(sftp, folder, fileId, ObjectState.ROLLBACKED);
    }

    private static class InputStreamSource extends InMemorySourceFile {

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
}
