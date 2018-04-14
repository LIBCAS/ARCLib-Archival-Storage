package cz.cas.lib.arcstorage.storage.shared;

import cz.cas.lib.arcstorage.domain.AipState;
import cz.cas.lib.arcstorage.domain.ChecksumType;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.*;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.ConnectionException;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.InMemoryDestFile;
import net.schmizz.sshj.xfer.InMemorySourceFile;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static cz.cas.lib.arcstorage.storage.shared.StorageUtils.*;

public class RemoteStorageProcessor implements StorageService {

    private StorageConfig storageConfig;
    private String S;

    public RemoteStorageProcessor(StorageConfig storageConfig, String separator) {
        this.storageConfig = storageConfig;
        this.S = separator;
    }


    @Override
    public StorageConfig getStorageConfig() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void storeAip(AipRef aip, AtomicBoolean rollback) throws StorageException {
        String sipFilePath = getSipPath(aip.getSip().getId());
        String xmlFilePath = getXmlPath(aip.getSip().getId());
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            listenForRollbackToKillSession(ssh, rollback);
            ssh.authPublickey("root", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                storeFile(sftp, xmlFilePath, toXmlId(aip.getSip().getId(), 1), S, aip.getXml().getInputStream(), aip.getXml().getChecksum(), rollback);
                storeFile(sftp, sipFilePath, aip.getSip().getId(), S, aip.getSip().getInputStream(), aip.getSip().getChecksum(), rollback);
            }
        } catch (ConnectionException e) {
            rollback.set(true);
            throw new StorageConnectionException(e);
        } catch (IOException e) {
            rollback.set(true);
            throw new SshException(e);
        } catch (Exception e) {
            rollback.set(true);
            throw new GeneralException(e);
        }
    }

    @Override
    public List<InputStream> getAip(String sipId, Integer... xmlVersions) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void storeXml(String sipId, XmlRef xml, AtomicBoolean rollback) throws StorageException {
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            listenForRollbackToKillSession(ssh, rollback);
            ssh.authPublickey("root", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                storeFile(sftp, getXmlPath(sipId), toXmlId(sipId, xml.getVersion()), S, xml.getInputStream(), xml.getChecksum(), rollback);
            }
        } catch (ConnectionException e) {
            rollback.set(true);
            throw new StorageConnectionException(e);
        } catch (IOException e) {
            rollback.set(true);
            throw new SshException(e);
        }
    }

    @Override
    public InputStream getXml(String sipId, int version) throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteSip(String sipId) throws StorageException {
        String sipPath = getSipPath(sipId);
        String sipFilePath = sipPath + S + sipId;
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            ssh.authPublickey("root", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                sftp.put(new InputStreamSource(new ByteArrayInputStream("".getBytes()), sipId + ".LOCK"), sipPath);
                deleteIfExistsSftp(sftp, sipFilePath);
                deleteIfExistsSftp(sftp, sipFilePath + ".REMOVED");
                deleteIfExistsSftp(sftp, sipFilePath + ".ROLLBACKED");
                for (ChecksumType checksumType : ChecksumType.values()) {
                    deleteIfExistsSftp(sftp, sipFilePath + "." + checksumType);
                }
                sftp.rm(sipFilePath + ".LOCK");
            }
        } catch (ConnectionException e) {
            throw new StorageConnectionException(e);
        } catch (IOException e) {
            throw new SshException(e);
        }
    }

    @Override
    public void remove(String sipId) throws StorageException {
        String sipPath = getSipPath(sipId);
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            ssh.authPublickey("root", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                sftp.put(new InputStreamSource(new ByteArrayInputStream("".getBytes()), sipId + ".REMOVED"), sipPath);
            }
        } catch (ConnectionException e) {
            throw new StorageConnectionException(e);
        } catch (IOException e) {
            throw new SshException(e);
        }
    }

    @Override
    public void rollbackAip(String sipId) throws StorageException {
        String sipPath = getSipPath(sipId);
        String xmlPath = getXmlPath(sipId);
        String xmlId = toXmlId(sipId, 1);
        String sipFilePath = sipPath + S + sipId;
        String xmlFilePath = xmlPath + S + xmlId;
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            ssh.authPublickey("root", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                sftp.mkdirs(sipPath);
                sftp.put(new InputStreamSource(new ByteArrayInputStream("".getBytes()), sipId + ".LOCK"), sipPath);
                deleteIfExistsSftp(sftp, sipFilePath);
                deleteIfExistsSftp(sftp, sipFilePath + ".REMOVED");
                deleteIfExistsSftp(sftp, sipFilePath + ".ROLLBACKED");
                for (ChecksumType checksumType : ChecksumType.values()) {
                    deleteIfExistsSftp(sftp, sipFilePath + "." + checksumType);
                }
                sftp.rename(sipFilePath + ".LOCK", sipFilePath + ".ROLLBACKED");

                sftp.mkdirs(xmlPath);
                sftp.put(new InputStreamSource(new ByteArrayInputStream("".getBytes()), xmlId + ".LOCK"), xmlPath);
                deleteIfExistsSftp(sftp, xmlFilePath);
                deleteIfExistsSftp(sftp, xmlFilePath + ".ROLLBACKED");
                for (ChecksumType checksumType : ChecksumType.values()) {
                    deleteIfExistsSftp(sftp, xmlFilePath + "." + checksumType);
                }
                sftp.rename(xmlFilePath + ".LOCK", xmlFilePath + ".ROLLBACKED");
            }
        } catch (ConnectionException e) {
            throw new StorageConnectionException(e);
        } catch (IOException e) {
            throw new SshException(e);
        }
    }

    @Override
    public void rollbackXml(String sipId, int version) throws StorageException {
        String xmlPath = getXmlPath(sipId);
        String xmlId = toXmlId(sipId, version);
        String xmlFilePath = xmlPath + S + xmlId;
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            ssh.authPublickey("root", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                sftp.mkdirs(xmlPath);
                sftp.put(new InputStreamSource(new ByteArrayInputStream("".getBytes()), xmlId + ".LOCK"), xmlPath);
                deleteIfExistsSftp(sftp, xmlFilePath);
                deleteIfExistsSftp(sftp, xmlFilePath + ".ROLLBACKED");
                for (ChecksumType checksumType : ChecksumType.values()) {
                    deleteIfExistsSftp(sftp, xmlFilePath + "." + checksumType);
                }
                sftp.rename(xmlFilePath + ".LOCK", xmlFilePath + ".ROLLBACKED");
            }
        } catch (ConnectionException e) {
            throw new StorageConnectionException(e);
        } catch (IOException e) {
            throw new SshException(e);
        }
    }

    @Override
    public AipStateInfo getAipInfo(String sipId, Checksum sipChecksum, AipState aipState, Map<Integer, Checksum> xmlVersions) throws StorageException {
        String sipPath = getSipPath(sipId);
        String xmlPath = getXmlPath(sipId);
        AipStateInfo info = new AipStateInfo(storageConfig.getName(), storageConfig.getStorageType(), aipState);
        try (SSHClient ssh = new SSHClient()) {
            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(storageConfig.getHost(), storageConfig.getPort());
            ssh.authPublickey("root", keyFilePath);
            try (SFTPClient sftp = ssh.newSFTPClient()) {
                if (aipState == AipState.ARCHIVED || aipState == AipState.REMOVED) {
                    Checksum storageFileChecksum = computeChecksum(getFile(sftp, sipPath + S + sipId), sipChecksum.getType());
                    info.setChecksum(storageFileChecksum);
                    info.setConsistent(sipChecksum.equals(storageFileChecksum));
                } else {
                    info.setChecksum(null);
                    info.setConsistent(false);
                }

                for (Integer version : xmlVersions.keySet()) {
                    Checksum dbChecksum = xmlVersions.get(version);
                    Checksum storageFileChecksum = computeChecksum(getFile(sftp, xmlPath + S + toXmlId(sipId, version)), dbChecksum.getType());
                    info.setChecksum(storageFileChecksum);
                    info.setConsistent(dbChecksum.equals(storageFileChecksum));
                }
            }
        } catch (ConnectionException e) {
            throw new StorageConnectionException(e);
        } catch (IOException e) {
            throw new SshException(e);
        }
        return info;
    }

    @Override
    public StorageState getStorageState() {
        throw new UnsupportedOperationException();
    }

    private InputStream getFile(SFTPClient sftp, String pathToFile) throws IOException, IOStorageException {
        PipedInputStream in = new PipedInputStream();
        AtomicBoolean wait = new AtomicBoolean(true);
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

    private void storeFile(SFTPClient sftp, String filePath, String id, String S, InputStream stream, Checksum checksum, AtomicBoolean rollback) throws StorageException {
        try {
            sftp.mkdirs(filePath);
            sftp.put(new InputStreamSource(new ByteArrayInputStream("".getBytes()), id + ".LOCK"), filePath);
            sftp.put(new InputStreamSource(stream, id), filePath);
            PipedInputStream in = new PipedInputStream();
            PipedOutputStream out = new PipedOutputStream(in);
            new Thread(() -> {
                try {
                    sftp.get(filePath + S + id, new OutputStreamSource(out));
                } catch (IOException e) {
                    rollback.set(true);
                    throw new GeneralException(new IOStorageException(e));
                } catch (Exception e) {
                    rollback.set(true);
                    throw new GeneralException(e);
                }
            }).start();
            Checksum storageFileChecksum = computeChecksum(in, checksum.getType());
            if (!checksum.equals(storageFileChecksum)) {
                rollback.set(true);
                throw new FileCorruptedAfterStoreException();
            }
            sftp.put(new InputStreamSource(new ByteArrayInputStream(checksum.getHash().getBytes()), id + "." + checksum.getType()), filePath);
            sftp.rm(filePath + S + id + ".LOCK");
        } catch (IOException e) {
            rollback.set(true);
            throw new IOStorageException();
        }
    }

    /**
     * Called by store methods. Checks for rollback flag each second and if rollback is set to true,
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
                Logger.getLogger(RemoteStorageProcessor.class).error("Fatal error: parallel process interrupted");
            }
        }).start();
    }

    private void deleteIfExistsSftp(SFTPClient sftp, String path) throws IOException {
        if (sftp.statExistence(path) != null)
            sftp.rm(path);
    }

    private String getSipPath(String fileName) {
        return storageConfig.getLocation() + S + "sip" + S + fileName.substring(0, 2) + S + fileName.substring(2, 4) + S + fileName.substring(4, 6);
    }

    private String getXmlPath(String fileName) {
        return storageConfig.getLocation() + S + "xml" + S + fileName.substring(0, 2) + S + fileName.substring(2, 4) + S + fileName.substring(4, 6);
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
