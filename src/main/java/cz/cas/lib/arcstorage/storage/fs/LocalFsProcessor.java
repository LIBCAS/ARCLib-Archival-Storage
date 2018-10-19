package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.StorageUtils;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import cz.cas.lib.arcstorage.storage.exception.FileDoesNotExistException;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.*;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static cz.cas.lib.arcstorage.util.Utils.strSF;
import static cz.cas.lib.arcstorage.util.Utils.strSX;


/**
 * implementation used by {@link FsAdapter} to provide {@link ZfsStorageService} and {@link FsStorageService} with methods
 * for access to the Local FS/ZFS or FS/ZFS mapped with NFS
 */
@Slf4j
public class LocalFsProcessor implements StorageService {

    @Getter
    private Storage storage;
    private String rootDirPath;

    public LocalFsProcessor(Storage storage, String rootDirPath) {
        this.storage = storage;
        this.rootDirPath = rootDirPath;
    }

    @Override
    public boolean testConnection() {
        try {
            Path path = Paths.get(rootDirPath);
            if (Files.isWritable(path) && Files.isReadable(path))
                return true;
            return false;
        } catch (Exception e) {
            log.error(storage.getName() + " unable to connect: " + e.getClass() + " " + e.getMessage());
            return false;
        }
    }

    @Override
    public void storeAip(AipDto aip, AtomicBoolean rollback, String dataSpace) throws StorageException {
        Path folder = getFolderPath(aip.getSip().getDatabaseId(), dataSpace);
        storeFile(folder, toXmlId(aip.getSip().getDatabaseId(), 1), aip.getXml().getInputStream(), aip.getXml().getChecksum(), rollback);
        storeFile(folder, aip.getSip().getDatabaseId(), aip.getSip().getInputStream(), aip.getSip().getChecksum(), rollback);
    }

    @Override
    public AipRetrievalResource getAip(String aipId, String dataSpace, Integer... xmlVersions) throws FileDoesNotExistException {
        String fileToOpen = aipId;
        AipRetrievalResource aip = new AipRetrievalResource(null);
        try {
            File sipFile = getFolderPath(aipId, dataSpace).resolve(aipId).toFile();
            aip.setSip(new FileInputStream(sipFile));
            for (int version : xmlVersions) {
                fileToOpen = toXmlId(aipId, version);
                aip.addXml(version, new FileInputStream(getFolderPath(aipId, dataSpace).resolve(fileToOpen).toFile()));
            }
        } catch (FileNotFoundException e) {
            for (InputStream ref : aip.getXmls().values()) {
                try {
                    ref.close();
                } catch (IOException ex) {
                    log.error("could not close input stream: " + ex.getMessage());
                }
            }
            throw new FileDoesNotExistException(strSF(storage.getName(), fileToOpen));
        }
        return aip;
    }

    @Override
    public void storeObject(ArchivalObjectDto objectDto, AtomicBoolean rollback, String dataSpace) throws StorageException {
        String id = objectDto.getStorageId();
        Path folder = getFolderPath(id, dataSpace);
        try {
            switch (objectDto.getState()) {
                case ARCHIVAL_FAILURE:
                    throw new IllegalArgumentException("trying to store object " + id + " which is in failed state");
                case DELETION_FAILURE:
                    objectDto.setState(ObjectState.DELETED);
                case ROLLED_BACK:
                case DELETED:
                    setState(folder, id, objectDto.getState());
                    break;
                case REMOVED:
                    storeFile(getFolderPath(id, dataSpace), id, objectDto.getInputStream(), objectDto.getChecksum(), rollback);
                    remove(id, dataSpace);
                    break;
                case ARCHIVED:
                case PROCESSING:
                    storeFile(getFolderPath(id, dataSpace), id, objectDto.getInputStream(), objectDto.getChecksum(), rollback);
                break;
                default:
                    throw new IllegalStateException(objectDto.toString());
            }
        } catch (IOException e) {
            rollback.set(true);
            throw new IOStorageException(e);
        }
    }

    @Override
    public ObjectRetrievalResource getObject(String id, String dataSpace) throws FileDoesNotExistException {
        try {
            return new ObjectRetrievalResource(new FileInputStream(getFolderPath(id, dataSpace).resolve(id).toFile()), null);
        } catch (FileNotFoundException e) {
            throw new FileDoesNotExistException(strSX(storage.getName(), id));
        }
    }

    @Override
    public void delete(String sipId, String dataSpace) throws IOStorageException, FileDoesNotExistException {
        Path sipFolder = getFolderPath(sipId, dataSpace);
        Path sipFilePath = sipFolder.resolve(sipId);
        try {
            setState(sipFolder, sipId, ObjectState.PROCESSING);
            Files.deleteIfExists(sipFilePath);
            transitState(sipFolder, sipId, ObjectState.PROCESSING, ObjectState.DELETED);
        } catch (IOException ex) {
            throw new IOStorageException(ex);
        }
    }

    @Override
    public void remove(String sipId, String dataSpace) throws IOStorageException, FileDoesNotExistException {
        Path sipFolder = getFolderPath(sipId, dataSpace);
        try {
            transitState(sipFolder, sipId, ObjectState.ARCHIVED, ObjectState.REMOVED);
        } catch (FileAlreadyExistsException e) {
        } catch (IOException ex) {
            throw new IOStorageException(ex);
        }
    }

    @Override
    public void renew(String sipId, String dataSpace) throws IOStorageException, FileDoesNotExistException {
        Path sipFolder = getFolderPath(sipId, dataSpace);
        try {
            transitState(sipFolder, sipId, ObjectState.REMOVED, ObjectState.ARCHIVED);
        } catch (FileAlreadyExistsException e) {
        } catch (IOException ex) {
            throw new IOStorageException(ex);
        }
    }

    @Override
    public void rollbackAip(String sipId, String dataSpace) throws StorageException {
        try {
            rollbackFile(getFolderPath(sipId, dataSpace), sipId);
            rollbackFile(getFolderPath(sipId, dataSpace), toXmlId(sipId, 1));
        } catch (IOException e) {
            throw new IOStorageException(e);
        }
    }

    @Override
    public void rollbackObject(String id, String dataSpace) throws StorageException {
        try {
            rollbackFile(getFolderPath(id, dataSpace), id);
        } catch (IOException e) {
            throw new IOStorageException(e);
        }
    }

    @Override
    public AipStateInfoDto getAipInfo(String aipId, Checksum sipChecksum, ObjectState objectState, Map<Integer, Checksum> xmlVersions, String dataSpace) throws StorageException {
        Path folder = getFolderPath(aipId, dataSpace);
        AipStateInfoDto info = new AipStateInfoDto(storage.getName(), storage.getStorageType(), objectState, sipChecksum, true);
        if (objectState == ObjectState.ARCHIVED || objectState == ObjectState.REMOVED) {
            try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(folder.resolve(aipId).toFile()))) {
                Checksum storageSipChecksum = StorageUtils.computeChecksum(bis, sipChecksum.getType());
                info.setSipStorageChecksum(storageSipChecksum);
                info.setConsistent(sipChecksum.equals(storageSipChecksum));
            } catch (FileNotFoundException e) {
                throw new FileDoesNotExistException(folder.resolve(aipId).toAbsolutePath().toString());
            } catch (IOException e) {
                throw new IOStorageException(e);
            }
        } else {
            info.setSipStorageChecksum(null);
            info.setConsistent(false);
        }
        for (Integer version : xmlVersions.keySet()) {
            Path xmlFilePath = folder.resolve(toXmlId(aipId, version));
            try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(xmlFilePath.toFile()))) {
                Checksum dbChecksum = xmlVersions.get(version);
                Checksum storageChecksum = StorageUtils.computeChecksum(bis, dbChecksum.getType());
                info.addXmlInfo(new XmlStateInfoDto(version, dbChecksum.equals(storageChecksum), storageChecksum, dbChecksum));
            } catch (FileNotFoundException e) {
                throw new FileDoesNotExistException(xmlFilePath.toAbsolutePath().toString());
            } catch (IOException e) {
                throw new IOStorageException(e);
            }
        }
        return info;
    }

    @Override
    public StorageStateDto getStorageState() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createNewDataSpace(String dataSpace) throws IOStorageException {
        try {
            Files.createDirectories(Paths.get(rootDirPath).resolve(dataSpace));
        } catch (IOException e) {
            throw new IOStorageException(e);
        }
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
    void storeFile(Path folder, String id, InputStream stream, Checksum checksum, AtomicBoolean rollback) throws FileCorruptedAfterStoreException, IOStorageException {
        if (rollback.get())
            return;
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(folder.resolve(id).toFile()))) {
            setState(folder, id, ObjectState.PROCESSING);
            Files.copy(new ByteArrayInputStream(checksum.getValue().getBytes()), folder.resolve(id + "." + checksum.getType()), StandardCopyOption.REPLACE_EXISTING);

            byte[] buffer = new byte[8192];
            int read = stream.read(buffer);
            while (read > 0) {
                if (rollback.get())
                    return;
                bos.write(buffer, 0, read);
                read = stream.read(buffer);
            }
            bos.flush();
            boolean rollbackInterruption = !verifyChecksum(new FileInputStream(folder.resolve(id).toFile()), checksum, rollback);
            if (rollbackInterruption)
                return;
            transitState(folder, id, ObjectState.PROCESSING, ObjectState.ARCHIVED);
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

    void rollbackFile(Path folder, String fileId) throws StorageException, IOException {
        String processingStateId = toStateStr(fileId, ObjectState.PROCESSING);
        if (Files.notExists(folder.resolve(processingStateId)))
            Files.createFile(folder.resolve(processingStateId));
        Files.deleteIfExists(folder.resolve(toStateStr(fileId, ObjectState.REMOVED)));
        Files.deleteIfExists(folder.resolve(toStateStr(fileId, ObjectState.DELETED)));
        Files.deleteIfExists(folder.resolve(toStateStr(fileId, ObjectState.ROLLED_BACK)));
        Files.deleteIfExists(folder.resolve(fileId));
        transitState(folder, fileId, ObjectState.PROCESSING, ObjectState.ROLLED_BACK);
    }

    Path getFolderPath(String fileName, String dataSpace) {
        Path path = Paths.get(rootDirPath).resolve(dataSpace).resolve(fileName.substring(0, 2)).resolve(fileName.substring(2, 4)).resolve(fileName.substring(4, 6));
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return path;
    }

    private void setState(Path folder, String fileId, ObjectState state) throws IOException {
        if (!Files.exists(folder.resolve(toStateStr(fileId, state))))
            Files.createFile(folder.resolve(toStateStr(fileId, state)));
    }

    private String toStateStr(String fileId, ObjectState objectState) {
        return fileId + "." + objectState.toString();
    }

    private void transitState(Path folder, String fileId, ObjectState oldState, ObjectState newState) throws IOException {
        File oldFile = folder
                .resolve(toStateStr(fileId, oldState))
                .toFile();
        File newFile = folder.resolve(toStateStr(fileId, newState)).toFile();
        if (newFile.exists())
            oldFile.delete();
        else
            oldFile.renameTo(newFile);
    }
}
