package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.StorageUtils;
import cz.cas.lib.arcstorage.storage.exception.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;

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
        storeFile(folder, aip.getXml().getStorageId(), aip.getXml().getInputStream(), aip.getXml().getChecksum(), rollback, aip.getXml().getCreated());
        storeFile(folder, aip.getSip().getStorageId(), aip.getSip().getInputStream(), aip.getSip().getChecksum(), rollback, aip.getSip().getCreated());
    }

    @Override
    public AipRetrievalResource getAip(String aipId, String dataSpace, Integer... xmlVersions) throws FileDoesNotExistException, IOStorageException {
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
            throw new FileDoesNotExistException(fileToOpen, storage);
        }
        return aip;
    }

    @Override
    public void storeObject(ArchivalObjectDto objectDto, AtomicBoolean rollback, String dataSpace) throws StorageException {
        String id = objectDto.getStorageId();
        Path folder = getFolderPath(id, dataSpace);
        try {
            switch (objectDto.getState()) {
                case DELETION_FAILURE:
                    writeObjectMetadata(folder, new ObjectMetadata(id, ObjectState.DELETED, objectDto.getCreated(), objectDto.getChecksum()));
                    break;
                case ARCHIVAL_FAILURE:
                case ROLLBACK_FAILURE:
                    writeObjectMetadata(folder, new ObjectMetadata(id, ObjectState.ROLLED_BACK, objectDto.getCreated(), objectDto.getChecksum()));
                    break;
                case ROLLED_BACK:
                case DELETED:
                    writeObjectMetadata(folder, new ObjectMetadata(id, objectDto.getState(), objectDto.getCreated(), objectDto.getChecksum()));
                    break;
                case REMOVED:
                    storeFile(getFolderPath(id, dataSpace), id, objectDto.getInputStream(), objectDto.getChecksum(), rollback, objectDto.getCreated());
                    remove(id, dataSpace);
                    break;
                case ARCHIVED:
                case PROCESSING:
                    storeFile(getFolderPath(id, dataSpace), id, objectDto.getInputStream(), objectDto.getChecksum(), rollback, objectDto.getCreated());
                    break;
                default:
                    throw new IllegalStateException(objectDto.toString());
            }
        } catch (Exception e) {
            rollback.set(true);
            throw e;
        }
    }

    @Override
    public void storeObjectMetadata(ArchivalObjectDto objectDto, String dataSpace) throws IOStorageException {
        Path folder = getFolderPath(objectDto.getStorageId(), dataSpace);
        writeObjectMetadata(folder, new ObjectMetadata(objectDto.getStorageId(), objectDto.getState(), objectDto.getCreated(), objectDto.getChecksum()));
    }

    @Override
    public ObjectRetrievalResource getObject(String id, String dataSpace) throws FileDoesNotExistException, IOStorageException {
        try {
            return new ObjectRetrievalResource(new FileInputStream(getFolderPath(id, dataSpace).resolve(id).toFile()), null);
        } catch (FileNotFoundException e) {
            throw new FileDoesNotExistException(id, storage);
        }
    }

    @Override
    public void delete(String sipId, String dataSpace) throws IOStorageException, CantParseMetadataFile, FileDoesNotExistException {
        Path sipFolder = getFolderPath(sipId, dataSpace);
        Path sipFilePath = sipFolder.resolve(sipId);
        try {
            setState(sipFolder, sipId, ObjectState.DELETED);
            Files.deleteIfExists(sipFilePath);
        } catch (IOException ex) {
            throw new IOStorageException(ex, storage);
        }
    }

    @Override
    public void remove(String sipId, String dataSpace) throws IOStorageException, CantParseMetadataFile, FileDoesNotExistException {
        Path sipFolder = getFolderPath(sipId, dataSpace);
        setState(sipFolder, sipId, ObjectState.REMOVED);
    }

    @Override
    public void renew(String sipId, String dataSpace) throws IOStorageException, CantParseMetadataFile, FileDoesNotExistException {
        Path sipFolder = getFolderPath(sipId, dataSpace);
        setState(sipFolder, sipId, ObjectState.ARCHIVED);
    }

    @Override
    public void rollbackAip(AipDto aipDto, String dataSpace) throws StorageException {
        try {
            rollbackFile(getFolderPath(aipDto.getSip().getStorageId(), dataSpace), aipDto.getSip());
            for (ArchivalObjectDto xml : aipDto.getXmls()) {
                rollbackFile(getFolderPath(xml.getStorageId(), dataSpace), xml);
            }
        } catch (IOException e) {
            throw new IOStorageException(e, storage);
        }
    }

    @Override
    public void rollbackObject(ArchivalObjectDto dto, String dataSpace) throws StorageException {
        try {
            rollbackFile(getFolderPath(dto.getStorageId(), dataSpace), dto);
        } catch (IOException e) {
            throw new IOStorageException(e, storage);
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
        try {
            Files.createDirectories(Paths.get(rootDirPath).resolve(dataSpace));
        } catch (IOException e) {
            throw new IOStorageException(e, storage);
        }
    }

    @Override
    public ArchivalObjectDto verifyStateOfObjects(List<ArchivalObjectDto> objects, AtomicLong counter) throws StorageException {
        for (ArchivalObjectDto inputObject : objects) {
            if (!inputObject.getState().metadataMustBeStoredAtLogicalStorage()) {
                counter.incrementAndGet();
                continue;
            }
            Path folderPath = getFolderPath(inputObject.getStorageId(), inputObject.getOwner().getDataSpace());
            ObjectMetadata metadataAtStorage = readObjectMetadata(folderPath, inputObject.getStorageId());
            if (metadataAtStorage == null || metadataAtStorage.getState() != inputObject.getState())
                return inputObject;
            counter.incrementAndGet();
        }
        return null;
    }

    @Override
    public List<ArchivalObjectDto> createDtosForAllObjects(String dataSpace) throws StorageException {
        Path rootDir = Paths.get(rootDirPath);
        File dataSpaceFile = new File(rootDir.resolve(dataSpace).toString());

        List<Pair<ObjectMetadata, Path>> objectsAndTheirPaths = getStoredObjects(dataSpaceFile);
        List<ArchivalObjectDto> allStoredArchivalObjects = new ArrayList<>();

        for (Pair<ObjectMetadata, Path> objectAndItsPath : objectsAndTheirPaths) {
            ObjectMetadata object = objectAndItsPath.getLeft();
            Path pathToObject = objectAndItsPath.getRight();
            List<ObjectMetadata> xmls = lookForXmls(object.getStorageId(), pathToObject);
            if (xmls.isEmpty())
                allStoredArchivalObjects.add(object.toDto(ObjectType.OBJECT));
            else {
                allStoredArchivalObjects.add(object.toDto(ObjectType.SIP));
                allStoredArchivalObjects.addAll(xmls.stream().map(o -> o.toDto(ObjectType.XML)).collect(Collectors.toList()));
            }
        }
        return allStoredArchivalObjects;
    }

    public Path getAipDataFilePath(String sipId, String dataSpace) throws IOStorageException {
        return getFolderPath(sipId, dataSpace).resolve(sipId);
    }

    /**
     * Gets objects and respective paths to their folders stored in the given dataSpace folder
     * <p>
     *     Scans the dataSpace folder for object metadata files: considers only files related to SIPs and general objects (not XMLs)
     *     and for every object found creates {@link ArchivalObject} out of the given data.
     * </p>
     *
     * @param dataSpace dataspace folder
     * @return list of pairs of found objects and respective paths to their folders
     * @throws IOStorageException
     * @throws CantParseMetadataFile
     */
    private List<Pair<ObjectMetadata, Path>> getStoredObjects(File dataSpace) throws IOStorageException, CantParseMetadataFile {
        List<File> objectMetadataFiles;
        try {
            objectMetadataFiles = Files.walk(dataSpace.toPath())
                    .map(Path::toFile)
                    .filter(f -> f.isFile() && !(f.getName().contains("xml")) && f.getName().matches("^.+\\.meta$"))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new IOStorageException(e, getStorage());
        }

        List<Pair<ObjectMetadata, Path>> objectsAndTheirPaths = new ArrayList<>();
        for (File objectMetadataFile : objectMetadataFiles) {
            String storageId = objectMetadataFile.getName().substring(0, objectMetadataFile.getName().length() - 5);
            ObjectMetadata metadata = readObjectMetadata(objectMetadataFile.getParentFile().toPath(), storageId);
            Pair<ObjectMetadata, Path> objectAndItsPath = Pair.of(metadata, objectMetadataFile.getParentFile().toPath());
            objectsAndTheirPaths.add(objectAndItsPath);
        }
        return objectsAndTheirPaths;
    }

    /**
     * Checks whether the passed {@link ArchivalObject} should be considered as {@link AipSip} (which is determined by finding / not finding any XML)
     * and if so then returns list of XMLs, otherwise returns empty list
     * <p>
     * Scans the dataSpace folder for object metadata files: considers only files related to XMLs (not SIPs or general objects)
     * and for every object found creates {@link ArchivalObject} out of the given data + XML version derived from the file name.
     * </p>
     *
     * @param possibleSipStorageId storage id of the archival object (possibly SIP) for which the respective XMLs should be searched for
     * @param pathToArchivalObject path to the folder where the object and possibly its XMLs reside
     * @return {@link List<ArchivalObject>} populated with found XMLs or empty list indicating that the passed {@link ArchivalObject} is not SIP but a general object
     * @throws IOStorageException
     * @throws CantParseMetadataFile
     */
    private List<ObjectMetadata> lookForXmls(String possibleSipStorageId, Path pathToArchivalObject) throws IOStorageException, CantParseMetadataFile {
        List<File> xmlMetadataFiles;
        try {
            xmlMetadataFiles = Files.walk(pathToArchivalObject)
                    .map(Path::toFile)
                    .filter(f -> f.isFile() &&
                            f.getName().matches("^" + possibleSipStorageId + "_xml_[0-9]+.meta$"))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new IOStorageException(e, getStorage());
        }

        List<ObjectMetadata> xmls = new ArrayList<>();
        for (File xmlMetadataFile : xmlMetadataFiles) {
            String storageId = xmlMetadataFile.getName().substring(0, xmlMetadataFile.getName().length() - 5);
            ObjectMetadata metadata = readObjectMetadata(xmlMetadataFile.getParentFile().toPath(), storageId);
            xmls.add(metadata);
        }
        return xmls;
    }

    private ObjectConsistencyVerificationResultDto fillObjectStateInfo(ObjectConsistencyVerificationResultDto info, ArchivalObjectDto object, String dataSpace) throws FileDoesNotExistException, IOStorageException, CantParseMetadataFile {
        Path folder = getFolderPath(object.getStorageId(), dataSpace);
        info.setDatabaseId(object.getDatabaseId());
        info.setCreated(object.getCreated());
        info.setStorageId(object.getStorageId());
        info.setState(object.getState());
        info.setDatabaseChecksum(object.getChecksum());
        if (!object.getState().metadataMustBeStoredAtLogicalStorage()) {
            return info;
        }
        ObjectMetadata metadataAtStorage = readObjectMetadata(folder, object.getStorageId());
        if (metadataAtStorage == null)
            throw new FileDoesNotExistException(metadataFilePath(folder, object.getStorageId()).toString(), storage);
        boolean stateMetadataConsistent = metadataAtStorage.getState() == object.getState();
        boolean timestampMetadataConsistent = object.getCreated().equals(metadataAtStorage.getCreated());
        boolean checksumMetadataConsistent = object.getChecksum().equals(metadataAtStorage.getChecksum());
        info.setMetadataConsistent(stateMetadataConsistent && checksumMetadataConsistent && timestampMetadataConsistent);
        if (object.getState().contentMustBeStoredAtLogicalStorage()) {
            try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(folder.resolve(object.getStorageId()).toFile()))) {
                Checksum storageChecksum = StorageUtils.computeChecksum(bis, object.getChecksum().getType());
                info.setStorageChecksum(storageChecksum);
                info.setContentConsistent(object.getChecksum().equals(storageChecksum));
            } catch (FileNotFoundException e) {
                throw new FileDoesNotExistException(folder.resolve(object.getStorageId()).toAbsolutePath().toString(), storage);
            } catch (IOException e) {
                throw new IOStorageException(e, storage);
            }
        }
        return info;
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
    void storeFile(Path folder, String id, InputStream stream, Checksum checksum, AtomicBoolean rollback, Instant created) throws FileCorruptedAfterStoreException, IOStorageException, CantParseMetadataFile, FileDoesNotExistException {
        if (rollback.get())
            return;
        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(folder.resolve(id).toFile()))) {
            writeObjectMetadata(folder, new ObjectMetadata(id, ObjectState.PROCESSING, created, checksum));
            byte[] buffer = new byte[8192];
            int read = stream.read(buffer);
            while (read > 0) {
                if (rollback.get())
                    return;
                bos.write(buffer, 0, read);
                read = stream.read(buffer);
            }
            bos.flush();
            boolean rollbackInterruption = !verifyChecksum(new FileInputStream(folder.resolve(id).toFile()), checksum, rollback, storage);
            if (rollbackInterruption)
                return;
            setState(folder, id, ObjectState.ARCHIVED);
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

    void rollbackFile(Path folder, ArchivalObjectDto dto) throws StorageException, IOException {
        ObjectMetadata objectMetadata = readObjectMetadata(folder, dto.getStorageId());
        if (objectMetadata == null)
            objectMetadata = new ObjectMetadata(dto.getStorageId(), ObjectState.ROLLED_BACK, dto.getCreated(), dto.getChecksum());
        else
            objectMetadata.setState(ObjectState.ROLLED_BACK);
        writeObjectMetadata(folder, objectMetadata);
        Files.deleteIfExists(folder.resolve(dto.getStorageId()));
    }

    /**
     * Returns path of folder containing the file, not the path to the file itself. Creates directories if needed.
     *
     * @param fileName
     * @param dataSpace
     * @return
     * @throws IOStorageException
     */
    Path getFolderPath(String fileName, String dataSpace) throws IOStorageException {
        Path path = Paths.get(rootDirPath).resolve(dataSpace).resolve(fileName.substring(0, 2)).resolve(fileName.substring(2, 4)).resolve(fileName.substring(4, 6));
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            throw new IOStorageException("cant create or access folder of object: " + fileName + " at dataspace: " + dataSpace, e, storage);
        }
        return path;
    }

    /**
     * Use only in case of already registered objects, i.e. metadata file is already present.
     * @param folder
     * @param fileId
     * @param state
     * @throws IOStorageException
     * @throws CantParseMetadataFile
     * @throws FileDoesNotExistException if there is no metadata file for the file yet
     */
    void setState(Path folder, String fileId, ObjectState state) throws IOStorageException, CantParseMetadataFile, FileDoesNotExistException {
        ObjectMetadata objectMetadata = readObjectMetadata(folder, fileId);
        if (objectMetadata == null)
            throw new FileDoesNotExistException(metadataFilePath(folder, fileId).toString(), storage);
        objectMetadata.setState(state);
        writeObjectMetadata(folder, objectMetadata);
    }

    /**
     * @param folder
     * @param fileId
     * @return null if the metadata file does not exist
     * @throws IOStorageException
     * @throws CantParseMetadataFile
     */
    ObjectMetadata readObjectMetadata(Path folder, String fileId) throws IOStorageException, CantParseMetadataFile {
        Path file = metadataFilePath(folder, fileId);
        if (!Files.exists(file))
            return null;
        List<String> lines;
        try {
            lines = Files.readAllLines(file);
        } catch (IOException e) {
            throw new IOStorageException(e, null);
        }
        return new ObjectMetadata(lines, fileId, storage);
    }

    /**
     * overwrites the whole metadata file
     *
     * @param folder
     * @param objectMetadata
     * @throws IOStorageException
     */
    void writeObjectMetadata(Path folder, ObjectMetadata objectMetadata) throws IOStorageException {
        File file = metadataFilePath(folder, objectMetadata.getStorageId()).toFile();
        try {
            Files.write(file.toPath(), objectMetadata.serialize(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new IOStorageException(e, null);
        }
    }

    private Path metadataFilePath(Path folder, String fileId) {
        return folder.resolve(fileId + ".meta");
    }
}