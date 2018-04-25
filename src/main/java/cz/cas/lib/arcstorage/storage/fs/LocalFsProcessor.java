package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.AipState;
import cz.cas.lib.arcstorage.domain.ChecksumType;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.StorageUtils;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import cz.cas.lib.arcstorage.storage.exception.FileDoesNotExistException;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static cz.cas.lib.arcstorage.util.Utils.strSF;
import static cz.cas.lib.arcstorage.util.Utils.strSX;

@Slf4j
public class LocalFsProcessor implements StorageService {

    @Getter
    private StorageConfig storageConfig;
    private String S;

    public LocalFsProcessor(StorageConfig storageConfig, String separator) {
        this.storageConfig = storageConfig;
        this.S = separator;
    }

    @Override
    public void storeAip(AipRef aip, AtomicBoolean rollback) throws StorageException {
        Path sipFolder = getSipFolderPath(aip.getSip().getId());
        Path xmlFolder = getXmlFolderPath(aip.getSip().getId());
        storeFile(xmlFolder, toXmlId(aip.getSip().getId(), 1), aip.getXml().getInputStream(), aip.getXml().getChecksum(), rollback);
        storeFile(sipFolder, aip.getSip().getId(), aip.getSip().getInputStream(), aip.getSip().getChecksum(), rollback);
    }

    @Override
    public List<FileRef> getAip(String sipId, Integer... xmlVersions) throws FileDoesNotExistException {
        List<FileRef> refs = new ArrayList<>();
        String fileToOpen = sipId;
        try {
            File sipFile = getSipFolderPath(sipId).resolve(sipId).toFile();
            refs.add(new FileRef(new FileInputStream(sipFile)));
            for (int version : xmlVersions) {
                fileToOpen = toXmlId(sipId, version);
                refs.add(new FileRef(new FileInputStream(getXmlFolderPath(sipId).resolve(fileToOpen).toFile())));
            }
        } catch (FileNotFoundException e) {
            for (FileRef ref : refs) {
                try {
                    ref.getInputStream().close();
                } catch (IOException ex) {
                    log.error("could not close inputStream: " + ex.getMessage());
                }
            }
            throw new FileDoesNotExistException(strSF(storageConfig.getName(), fileToOpen));
        }
        return refs;
    }

    @Override
    public void storeXml(String sipId, XmlRef xml, AtomicBoolean rollback) throws StorageException {
        storeFile(getXmlFolderPath(sipId), toXmlId(sipId, xml.getVersion()), xml.getInputStream(), xml.getChecksum(), rollback);
    }

    @Override
    public FileRef getXml(String sipId, int version) throws FileDoesNotExistException {
        String xmlId = toXmlId(sipId, version);
        try {
            return new FileRef(new FileInputStream(getSipFolderPath(sipId).resolve(sipId).toFile()));
        } catch (FileNotFoundException e) {
            throw new FileDoesNotExistException(strSX(storageConfig.getName(), xmlId));
        }
    }

    @Override
    public void deleteSip(String sipId) throws IOStorageException, FileDoesNotExistException {
        Path sipFolder = getSipFolderPath(sipId);
        Path sipFilePath = sipFolder.resolve(sipId);
        try {
            checkFixityMetadataExists(sipFilePath);
            setState(sipFolder, sipId, AipState.PROCESSING);
            Files.deleteIfExists(sipFilePath);
            transitProcessingState(sipFolder, sipId, AipState.DELETED);
        } catch (IOException ex) {
            throw new IOStorageException(ex);
        }
    }

    @Override
    public void remove(String sipId) throws IOStorageException, FileDoesNotExistException {
        Path sipFolder = getSipFolderPath(sipId);
        try {
            checkFixityMetadataExists(sipFolder);
            setState(sipFolder, sipId, AipState.REMOVED);
            Files.createFile(sipFolder.resolve(String.format("%s.REMOVED", sipId)));
        } catch (FileAlreadyExistsException e) {
        } catch (IOException ex) {
            throw new IOStorageException(ex);
        }
    }

    @Override
    public void rollbackAip(String sipId) throws StorageException {
        try {
            rollbackFile(getSipFolderPath(sipId), sipId);
            rollbackFile(getXmlFolderPath(sipId), toXmlId(sipId, 1));
        } catch (IOException e) {
            throw new IOStorageException(e);
        }
    }

    @Override
    public void rollbackXml(String sipId, int version) throws StorageException {
        try {
            rollbackFile(getXmlFolderPath(sipId), toXmlId(sipId, version));
        } catch (IOException e) {
            throw new IOStorageException(e);
        }
    }

    @Override
    public AipStateInfo getAipInfo(String sipId, Checksum sipChecksum, AipState aipState, Map<Integer, Checksum> xmlVersions) throws StorageException {
        Path sipFolder = getSipFolderPath(sipId);
        Path xmlFolder = getXmlFolderPath(sipId);
        AipStateInfo info = new AipStateInfo(storageConfig.getName(), storageConfig.getStorageType(), aipState, sipChecksum);
        if (aipState == AipState.ARCHIVED || aipState == AipState.REMOVED) {
            try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(sipFolder.resolve(sipId).toFile()))) {
                Checksum storageSipChecksum = StorageUtils.computeChecksum(bis, sipChecksum.getType());
                info.setStorageChecksum(storageSipChecksum);
                info.setConsistent(sipChecksum.equals(storageSipChecksum));
            } catch (FileNotFoundException e) {
                throw new FileDoesNotExistException(sipFolder.resolve(sipId).toAbsolutePath().toString());
            } catch (IOException e) {
                throw new IOStorageException(e);
            }
        } else {
            info.setStorageChecksum(null);
            info.setConsistent(false);
        }
        for (Integer version : xmlVersions.keySet()) {
            Path xmlFilePath = xmlFolder.resolve(toXmlId(sipId, version));
            try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(xmlFilePath.toFile()))) {
                Checksum dbChecksum = xmlVersions.get(version);
                Checksum storageChecksum = StorageUtils.computeChecksum(bis, dbChecksum.getType());
                info.addXmlInfo(new XmlStateInfo(version, dbChecksum.equals(storageChecksum), storageChecksum, dbChecksum));
            } catch (FileNotFoundException e) {
                throw new FileDoesNotExistException(xmlFilePath.toAbsolutePath().toString());
            } catch (IOException e) {
                throw new IOStorageException(e);
            }
        }
        return info;
    }

    @Override
    public StorageState getStorageState() {
        throw new UnsupportedOperationException();
    }

    /**
     * Stores file and then reads it and verifies its fixity.
     * <p>
     * If rollback is set to true by another thread, this method returns ASAP (without throwing exception), leaving the file uncompleted but closing stream.  Uncompleted files are to be cleaned during rollback.
     * </p>
     * <p>
     * In case of any exception, rollback flag is set to true.
     * </p>
     *
     * @param folder   path to new file
     * @param id       id of new file
     * @param stream   new file stream
     * @param checksum storageChecksum of the file
     * @param rollback rollback flag to be periodically checked
     * @throws FileCorruptedAfterStoreException if fixity does not match after store
     * @throws IOStorageException               in case of any {@link IOException}
     * @throws GeneralException                 in case of any unexpected error
     */
    private void storeFile(Path folder, String id, InputStream stream, Checksum checksum, AtomicBoolean rollback) throws FileCorruptedAfterStoreException, IOStorageException {
        if (rollback.get())
            return;
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(folder.resolve(id).toFile()))) {
            Files.createDirectories(folder);
            setState(folder, id, AipState.PROCESSING);

            byte[] buffer = new byte[8192];
            int read = stream.read(buffer);
            while (read > 0) {
                if (rollback.get())
                    return;
                bos.write(buffer, 0, read);
                read = stream.read(buffer);
            }

            boolean rollbackInterruption = !verifyChecksum(new FileInputStream(folder.resolve(id).toFile()), checksum, rollback);
            if (rollbackInterruption)
                return;

            Files.copy(new ByteArrayInputStream(checksum.getHash().getBytes()), folder.resolve(id + "." + checksum.getType()));
            transitProcessingState(folder, id, AipState.ARCHIVED);
        } catch (IOException e) {
            rollback.set(true);
            throw new IOStorageException(e);
        } catch (Exception e) {
            rollback.set(true);
            throw new GeneralException(e);
        }
    }

    private void rollbackFile(Path folder, String fileId) throws StorageException, IOException {
        String processingStateId = toStateStr(fileId, AipState.PROCESSING);
        if (Files.notExists(folder.resolve(processingStateId)))
            Files.createFile(folder.resolve(processingStateId));
        Files.deleteIfExists(folder.resolve(toStateStr(fileId, AipState.REMOVED)));
        Files.deleteIfExists(folder.resolve(toStateStr(fileId, AipState.DELETED)));
        Files.deleteIfExists(folder.resolve(toStateStr(fileId, AipState.ROLLBACKED)));
        Files.deleteIfExists(folder.resolve(fileId));
        transitProcessingState(folder, fileId, AipState.ROLLBACKED);
    }

    private void checkFixityMetadataExists(Path sipFilePath) throws FileDoesNotExistException {
        boolean exists = Arrays.stream(ChecksumType.values())
                .anyMatch(ct -> sipFilePath.resolveSibling(sipFilePath.getFileName() + "." + ct.toString()).toFile().exists());
        if (!exists)
            throw new FileDoesNotExistException("metadata of file " + sipFilePath.toAbsolutePath().toString());
    }

    private Path getSipFolderPath(String fileName) {
        return Paths.get(storageConfig.getLocation() + S + "sip" + S + fileName.substring(0, 2) + S + fileName.substring(2, 4) + S + fileName.substring(4, 6));
    }

    private Path getXmlFolderPath(String fileName) {
        return Paths.get(storageConfig.getLocation() + S + "xml" + S + fileName.substring(0, 2) + S + fileName.substring(2, 4) + S + fileName.substring(4, 6));
    }

    private void setState(Path folder, String fileId, AipState state) throws IOException {
        Files.createFile(folder.resolve(toStateStr(fileId, state)));
    }

    private String toStateStr(String fileId, AipState aipState) {
        return fileId + "." + aipState.toString();
    }

    private void transitProcessingState(Path folder, String fileId, AipState newState) throws IOException {
        File oldFile = folder
                .resolve(toStateStr(fileId, AipState.PROCESSING))
                .toFile();
        File newFile = folder.resolve(toStateStr(fileId, newState)).toFile();
        oldFile.renameTo(newFile);
    }
}
