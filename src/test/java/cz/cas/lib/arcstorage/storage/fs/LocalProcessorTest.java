package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.storage.StorageServiceTest;
import cz.cas.lib.arcstorage.storage.exception.CantParseMetadataFile;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import cz.cas.lib.arcstorage.storage.exception.FileDoesNotExistException;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static helper.ThrowableAssertion.assertThrown;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class LocalProcessorTest extends StorageServiceTest {
    @Getter
    private LocalFsProcessor service = new LocalFsProcessor(storage, rootDirPath);
    private static Storage storage = new Storage();
    private static String rootDirPath;
    private static Properties props;
    private static String dataSpace;

    @BeforeClass
    public static void beforeClass() throws IOException {
        props = new Properties();
        props.load(ClassLoader.getSystemResourceAsStream("application.properties"));
        rootDirPath = props.getProperty("test.local.folderpath");
        dataSpace = props.getProperty("test.local.dataspace");
        Path testFolder = Paths.get(rootDirPath).resolve(dataSpace);
        FileUtils.deleteDirectory(testFolder.toFile());
        Files.createDirectories(testFolder);
        storage.setName("local storage");
    }

    @Override
    public String getDataSpace() {
        return dataSpace;
    }

    @Test
    @Override
    public void storeFileSuccessTest() throws Exception {
        String id = testName.getMethodName();
        Path path = getFolderPath(id).resolve(id);
        ArchivalObjectDto archivalObjectDto = new ArchivalObjectDto(id, id, SIP_CHECKSUM, null, getSipStream(), ObjectState.PROCESSING, Instant.now(), ObjectType.SIP);
        service.storeFile(getFolderPath(id), archivalObjectDto, new AtomicBoolean(false));
        assertThat(streamToString(new FileInputStream(path.toFile())), is(SIP_CONTENT));
        ObjectMetadata objectMetadata = readObjectMetadata(getFolderPath(id), id);
        assertThat(objectMetadata.getChecksum(), is(SIP_CHECKSUM));
        assertThat(objectMetadata.getState(), is(ObjectState.ARCHIVED));
    }

    @Test
    @Override
    public void storeFileRollbackAware() throws Exception {
        String fileId = testName.getMethodName();

        File file = new File(LARGE_SIP_PATH);
        AtomicBoolean rollback = new AtomicBoolean(false);

        new Thread(() -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            rollback.set(true);
        }).start();

        LocalFsProcessor service = new TestServiceCatchingRollback(this.service.getStorage());
        Path path = getFolderPath(fileId).resolve(fileId);

        try (BufferedInputStream bos = new BufferedInputStream(new FileInputStream(file))) {
            ArchivalObjectDto dto = new ArchivalObjectDto(fileId, fileId, LARGE_SIP_CHECKSUM, null, bos, ObjectState.PROCESSING, Instant.now(), ObjectType.SIP);
            service.storeFile(getFolderPath(fileId), dto, rollback);
        }
        assertThat(isInState(path, ObjectState.PROCESSING), is(true));
    }

    @Test
    @Override
    public void storeFileSettingRollback() throws Exception {
        String fileId = testName.getMethodName();

        LocalFsProcessor service = new TestServiceSettingRollback(storage);

        AtomicBoolean rollback = new AtomicBoolean(false);

        ArchivalObjectDto dto = new ArchivalObjectDto(fileId, fileId, SIP_CHECKSUM, null, getSipStream(), ObjectState.PROCESSING, Instant.now(), ObjectType.SIP);

        assertThrown(() -> service.storeFile(getFolderPath(fileId), dto, rollback))
                .isInstanceOf(FileCorruptedAfterStoreException.class);
        assertThat(rollback.get(), is(true));

        rollback.set(false);

        assertThrown(() -> service.storeFile(getFolderPath(fileId), dto, rollback))
                .isInstanceOf(Throwable.class);
        assertThat(rollback.get(), is(true));
    }

    @Test
    @Override
    public void storeAipOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        Path path = getFolderPath(sipId);
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, dataSpace);

        assertThat(isInState(path.resolve(sipId), ObjectState.ARCHIVED), is(true));
        assertThat(streamToString(new FileInputStream(path.resolve(sipId).toFile())), is(SIP_CONTENT));
        assertThat(isInState(path.resolve(xmlId), ObjectState.ARCHIVED), is(true));
        assertThat(streamToString(new FileInputStream(path.resolve(xmlId).toFile())), is(XML_CONTENT));

        assertThat(rollback.get(), is(false));
    }

    @Test
    @Override
    public void storeXmlOk() throws Exception {
        String sipId = testName.getMethodName();
        AtomicBoolean rollback = new AtomicBoolean(false);
        String xmlId = toXmlId(sipId, 99);
        service.storeObject(new ArchivalObjectDto(xmlId, "databaseId", XML_CHECKSUM, new User("ownerId"), getXmlStream(), ObjectState.PROCESSING, Instant.now(), ObjectType.XML), rollback, dataSpace);
        Path path = getFolderPath(xmlId);
        assertThat(isInState(path.resolve(xmlId), ObjectState.ARCHIVED), is(true));
        assertThat(streamToString(new FileInputStream(path.resolve(xmlId).toFile())), is(XML_CONTENT));
        assertThat(rollback.get(), is(false));
    }

    @Test
    @Override
    public void removeSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, dataSpace);

        service.remove(aip.getSip(), dataSpace, false);
        service.remove(aip.getSip(), dataSpace, false);

        Path path = getFolderPath(sipId);
        assertThat(streamToString(new FileInputStream(path.resolve(sipId).toFile())), is(SIP_CONTENT));
        assertThat(isInState(path.resolve(sipId), ObjectState.REMOVED), is(true));
    }

    @Test
    @Override
    public void renewSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, dataSpace);

        service.remove(aip.getSip(), dataSpace, false);
        service.renew(aip.getSip(), dataSpace, false);
        service.renew(aip.getSip(), dataSpace, false);

        Path path = getFolderPath(sipId);
        assertThat(streamToString(new FileInputStream(path.resolve(sipId).toFile())), is(SIP_CONTENT));
        assertThat(isInState(path.resolve(sipId), ObjectState.ARCHIVED), is(true));
    }

    @Test
    @Override
    public void deleteSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, dataSpace);

        service.delete(aip.getSip(), dataSpace, false);
        service.delete(aip.getSip(), dataSpace, false);

        Path path = getFolderPath(sipId);
        assertThat(Files.exists(path.resolve(sipId)), is(false));
        assertThat(isInState(path.resolve(sipId), ObjectState.DELETED), is(true));

        assertThat(streamToString(new FileInputStream(path.resolve(xmlId).toFile())), is(XML_CONTENT));
        assertThat(isInState(path.resolve(xmlId), ObjectState.ARCHIVED), is(true));
    }

    @Test
    @Override
    public void rollbackProcessingFile() throws Exception {
        String fileId = testName.getMethodName();

//preparation phase copied from rollbackAwareTest
        File file = new File(LARGE_SIP_PATH);
        AtomicBoolean rollback = new AtomicBoolean(false);

        new Thread(() -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            rollback.set(true);
        }).start();
//actual test
        LocalFsProcessor service = new TestServiceCatchingRollback(this.service.getStorage());
        Path path = getFolderPath(fileId);

        ArchivalObjectDto dto;
        try (BufferedInputStream bos = new BufferedInputStream(new FileInputStream(file))) {
            dto = new ArchivalObjectDto(fileId, null, LARGE_SIP_CHECKSUM, null, bos, ObjectState.PROCESSING, Instant.now(), ObjectType.SIP);
            service.storeFile(getFolderPath(fileId), dto, rollback);
        }
        assertThat(isInState(path.resolve(fileId), ObjectState.PROCESSING), is(true));
        service.rollbackFile(path, dto);

        assertThat(Files.exists(path.resolve(fileId)), is(false));
        assertThat(isInState(path.resolve(fileId), ObjectState.ROLLED_BACK), is(true));
    }

    @Test
    @Override
    public void rollbackStoredFileMultipleTimes() throws Exception {
        String fileId = testName.getMethodName();
        Path path = getFolderPath(fileId);

        ArchivalObjectDto dto = new ArchivalObjectDto(fileId, null, SIP_CHECKSUM, null, getSipStream(), ObjectState.ARCHIVED, Instant.now(), ObjectType.SIP);
        service.storeFile(path, dto, new AtomicBoolean(false));
        service.rollbackFile(path, dto);
        service.rollbackFile(path, dto);

        assertThat(Files.exists(path.resolve(fileId)), is(false));
        assertThat(isInState(path.resolve(fileId), ObjectState.ROLLED_BACK), is(true));
    }

    @Test
    @Override
    public void rollbackCompletlyMissingFile() throws Exception {
        String fileId = testName.getMethodName();
        Path path = getFolderPath(fileId);
        ArchivalObjectDto dto = new ArchivalObjectDto(fileId, null, XML_CHECKSUM, null, null, ObjectState.ARCHIVED, Instant.now(), ObjectType.XML);
        service.rollbackFile(path, dto);
        assertThat(isInState(path.resolve(fileId), ObjectState.ROLLED_BACK), is(true));
    }

    @Test
    @Override
    public void rollbackAipOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, dataSpace);

        service.rollbackAip(aip, dataSpace);

        Path path = getFolderPath(sipId);
        assertThat(Files.exists(path.resolve(sipId)), is(false));
        assertThat(Files.exists(path.resolve(xmlId)), is(false));

        assertThat(isInState(path.resolve(sipId), ObjectState.ROLLED_BACK), is(true));
        assertThat(isInState(path.resolve(xmlId), ObjectState.ROLLED_BACK), is(true));
    }

    @Test
    @Override
    public void rollbackXmlOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, dataSpace);
        service.rollbackObject(aip.getXml(), dataSpace);

        Path path = getFolderPath(sipId);

        assertThat(Files.exists(path.resolve(xmlId)), is(false));

        assertThat(streamToString(new FileInputStream(path.resolve(sipId).toFile())), is(SIP_CONTENT));
        assertThat(isInState(path.resolve(sipId), ObjectState.ARCHIVED), is(true));
        assertThat(isInState(path.resolve(xmlId), ObjectState.ROLLED_BACK), is(true));
    }

    @Test
    @Override
    public void testConnection() {
        Storage badConfig = new Storage();
        badConfig.setName("bad storage");
        LocalFsProcessor badService = new LocalFsProcessor(badConfig, "/blah");
        assertThat(service.testConnection(), is(true));
        assertThat(badService.testConnection(), is(false));
    }

    @Test
    public void testCreateControlFile() throws IOException {
        String controlFileName = "TEST.TEST";
        Path controlFilePath = Paths.get(rootDirPath).resolve(controlFileName);
        service.createControlFile(controlFileName);
        assertThat(controlFilePath.toFile().isFile(), is(true));
        Files.delete(controlFilePath);
    }

    private String getChecksumValue(Path fileBasePath, ChecksumType checksumType) throws FileNotFoundException {
        return streamToString(new FileInputStream(fileBasePath.resolveSibling(fileBasePath.getFileName() + "." + checksumType).toFile()));
    }

    private boolean isInState(Path fileBasePath, ObjectState state) throws CantParseMetadataFile, IOStorageException, FileDoesNotExistException {
        ObjectMetadata metadataAtStorage = readObjectMetadata(fileBasePath.getParent(), fileBasePath.getFileName().toString());
        return state == metadataAtStorage.getState();
    }

    private static final class TestServiceSettingRollback extends LocalFsProcessor {
        public TestServiceSettingRollback(Storage storage) {
            super(storage, props.getProperty("test.local.folderpath"));
        }

        @Override
        public Checksum computeChecksumRollbackAware(InputStream fileStream, ChecksumType checksumType, AtomicBoolean rollback) throws IOException {
            return new Checksum(ChecksumType.MD5, "alwayswrong");
        }
    }

    private Path getFolderPath(String fileName) throws IOStorageException {
        return service.getFolderPath(fileName, dataSpace);
    }

    private static final class TestServiceCatchingRollback extends LocalFsProcessor {
        public TestServiceCatchingRollback(Storage storage) {
            super(storage, props.getProperty("test.local.folderpath"));
        }

        @Override
        void storeFile(Path folder, ArchivalObjectDto dto, AtomicBoolean rollback) throws IOStorageException, CantParseMetadataFile, FileCorruptedAfterStoreException, FileDoesNotExistException {
            if (rollback.get())
                return;
            try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(folder.resolve(dto.getStorageId()).toFile()))) {
                writeObjectMetadata(folder, new ObjectMetadata(dto.getStorageId(), ObjectState.PROCESSING, dto.getCreated(), dto.getChecksum()));
                byte[] buffer = new byte[8192];
                int read = dto.getInputStream().read(buffer);
                while (read > 0) {
                    if (rollback.get())
                        return;
                    Thread.sleep(1000);
                    bos.write(buffer, 0, read);
                    read = dto.getInputStream().read(buffer);
                }
                bos.flush();
                boolean rollbackInterruption = !verifyChecksum(new FileInputStream(folder.resolve(dto.getStorageId()).toFile()), dto.getChecksum(), rollback, storage);
                if (rollbackInterruption)
                    return;
                setState(folder, dto, ObjectState.ARCHIVED, false);
            } catch (IOException e) {
                rollback.set(true);
                throw new IOStorageException(e, storage);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (Exception e) {
                if (e instanceof FileCorruptedAfterStoreException)
                    throw e;
                rollback.set(true);
                throw new GeneralException(e);
            }
        }
    }

    private ObjectMetadata readObjectMetadata(Path folder, String fileId) throws IOStorageException, CantParseMetadataFile, FileDoesNotExistException {
        Path file = folder.resolve(fileId + ".meta");
        List<String> lines;
        try {
            lines = Files.readAllLines(file);
        } catch (IOException e) {
            throw new IOStorageException(e, null);
        }
        return new ObjectMetadata(lines, fileId, getService().getStorage());
    }
}
