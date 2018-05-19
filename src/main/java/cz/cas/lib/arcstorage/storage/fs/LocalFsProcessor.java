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

@Slf4j
public class LocalFsProcessor implements StorageService {

    @Getter
    private Storage storage;

    public LocalFsProcessor(Storage storage) {
        this.storage = storage;
    }

    @Override
    public boolean testConnection() {
        try {
            Path path = Paths.get(storage.getLocation());
            if (Files.isWritable(path) && Files.isReadable(path))
                return true;
            return false;
        } catch (Exception e) {
            log.error(storage.getName() + " unable to connect: " + e.getClass() + " " + e.getMessage());
            return false;
        }
    }

    @Override
    public void storeAip(AipDto aip, AtomicBoolean rollback) throws StorageException {
        Path folder = getFolderPath(aip.getSip().getId());
        storeFile(folder, toXmlId(aip.getSip().getId(), 1), aip.getXml().getInputStream(), aip.getXml().getChecksum(), rollback);
        storeFile(folder, aip.getSip().getId(), aip.getSip().getInputStream(), aip.getSip().getChecksum(), rollback);
    }

    @Override
    public AipRetrievalResource getAip(String sipId, Integer... xmlVersions) throws FileDoesNotExistException {
        String fileToOpen = sipId;
        AipRetrievalResource aip = new AipRetrievalResource(null);
        try {
            File sipFile = getFolderPath(sipId).resolve(sipId).toFile();
            aip.setSip(new FileInputStream(sipFile));
            for (int version : xmlVersions) {
                fileToOpen = toXmlId(sipId, version);
                aip.addXml(version, new FileInputStream(getFolderPath(sipId).resolve(fileToOpen).toFile()));
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
    public void storeObject(ArchivalObjectDto objectDto, AtomicBoolean rollback) throws StorageException {
        String id = objectDto.getStorageId();
        storeFile(getFolderPath(id), id, objectDto.getInputStream(), objectDto.getChecksum(), rollback);
    }

    @Override
    public ObjectRetrievalResource getObject(String id) throws FileDoesNotExistException {
        try {
            return new ObjectRetrievalResource(new FileInputStream(getFolderPath(id).resolve(id).toFile()), null);
        } catch (FileNotFoundException e) {
            throw new FileDoesNotExistException(strSX(storage.getName(), id));
        }
    }

    @Override
    public void deleteSip(String sipId) throws IOStorageException, FileDoesNotExistException {
        Path sipFolder = getFolderPath(sipId);
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
    public void remove(String sipId) throws IOStorageException, FileDoesNotExistException {
        Path sipFolder = getFolderPath(sipId);
        try {
            transitState(sipFolder, sipId, ObjectState.ARCHIVED, ObjectState.REMOVED);
        } catch (FileAlreadyExistsException e) {
        } catch (IOException ex) {
            throw new IOStorageException(ex);
        }
    }

    @Override
    public void renew(String sipId) throws IOStorageException, FileDoesNotExistException {
        Path sipFolder = getFolderPath(sipId);
        try {
            transitState(sipFolder, sipId, ObjectState.REMOVED, ObjectState.ARCHIVED);
        } catch (FileAlreadyExistsException e) {
        } catch (IOException ex) {
            throw new IOStorageException(ex);
        }
    }

    @Override
    public void rollbackAip(String sipId) throws StorageException {
        try {
            rollbackFile(getFolderPath(sipId), sipId);
            rollbackFile(getFolderPath(sipId), toXmlId(sipId, 1));
        } catch (IOException e) {
            throw new IOStorageException(e);
        }
    }

    @Override
    public void rollbackObject(String id) throws StorageException {
        try {
            rollbackFile(getFolderPath(id), id);
        } catch (IOException e) {
            throw new IOStorageException(e);
        }
    }

    @Override
    public AipStateInfoDto getAipInfo(String sipId, Checksum sipChecksum, ObjectState objectState, Map<Integer, Checksum> xmlVersions) throws StorageException {
        Path folder = getFolderPath(sipId);
        AipStateInfoDto info = new AipStateInfoDto(storage.getName(), storage.getStorageType(), objectState, sipChecksum);
        if (objectState == ObjectState.ARCHIVED || objectState == ObjectState.REMOVED) {
            try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(folder.resolve(sipId).toFile()))) {
                Checksum storageSipChecksum = StorageUtils.computeChecksum(bis, sipChecksum.getType());
                info.setSipStorageChecksum(storageSipChecksum);
                info.setConsistent(sipChecksum.equals(storageSipChecksum));
            } catch (FileNotFoundException e) {
                throw new FileDoesNotExistException(folder.resolve(sipId).toAbsolutePath().toString());
            } catch (IOException e) {
                throw new IOStorageException(e);
            }
        } else {
            info.setSipStorageChecksum(null);
            info.setConsistent(false);
        }
        for (Integer version : xmlVersions.keySet()) {
            Path xmlFilePath = folder.resolve(toXmlId(sipId, version));
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
     * @param id       storageId of new file
     * @param stream   new file stream
     * @param checksum sipStorageChecksum of the file
     * @param rollback rollback flag to be periodically checked
     * @throws FileCorruptedAfterStoreException if fixity does not match after save
     * @throws IOStorageException               in case of any {@link IOException}
     * @throws GeneralException                 in case of any unexpected error
     */
    void storeFile(Path folder, String id, InputStream stream, Checksum checksum, AtomicBoolean rollback) throws FileCorruptedAfterStoreException, IOStorageException {
        if (rollback.get())
            return;
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(folder.resolve(id).toFile()))) {
            Files.createDirectories(folder);
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

    Path getFolderPath(String fileName) {
        Path path = Paths.get(storage.getLocation()).resolve(fileName.substring(0, 2)).resolve(fileName.substring(2, 4)).resolve(fileName.substring(4, 6));
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
