package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.storage.StorageServiceTest;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static helper.ThrowableAssertion.assertThrown;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class LocalProcessorTest extends StorageServiceTest {
    @Getter
    private LocalFsProcessor service = new LocalFsProcessor(storage);
    private static Storage storage = new Storage();

    @BeforeClass
    public static void beforeClass() throws IOException {
        Properties props = new Properties();
        props.load(ClassLoader.getSystemResourceAsStream("application.properties"));
        Path testFolder = Paths.get(props.getProperty("test.local.folderpath"));
        FileUtils.deleteQuietly(testFolder.toFile());
        Files.createDirectories(testFolder);
        storage.setName("local storage");
        storage.setLocation(testFolder.toAbsolutePath().toString());
    }

    @AfterClass
    public static void afterClass() throws IOException {
        Properties props = new Properties();
        props.load(ClassLoader.getSystemResourceAsStream("application.properties"));
        Path testFolder = Paths.get(props.getProperty("test.local.folderpath"));
        FileUtils.deleteQuietly(testFolder.toFile());
    }

    @Test
    @Override
    public void storeFileSuccessTest() throws Exception {
        String id = testName.getMethodName();
        Path path = service.getFolderPath(id).resolve(id);
        service.storeFile(service.getFolderPath(id), id, getSipStream(), SIP_CHECKSUM, new AtomicBoolean(false));
        assertThat(streamToString(new FileInputStream(path.toFile())), is(SIP_CONTENT));
        assertThat(getChecksumValue(path, SIP_CHECKSUM.getType()), is(SIP_CHECKSUM.getValue()));
        assertTrue(isInOneState(path, ObjectState.ARCHIVED));
    }

    @Test
    @Override
    public void storeFileRollbackAware() throws Exception {
        String fileId = testName.getMethodName();

        File file = new File("src/test/resources/8MiB+file");
        AtomicBoolean rollback = new AtomicBoolean(false);

        new Thread(() -> {
            try {
                //this test fails if waiting for too long
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            rollback.set(true);
        }).start();
        Path path = service.getFolderPath(fileId).resolve(fileId);

        try (BufferedInputStream bos = new BufferedInputStream(new FileInputStream(file))) {
            service.storeFile(service.getFolderPath(fileId), fileId, bos, SIP_CHECKSUM, rollback);
        }
        assertTrue(isInOneState(path, ObjectState.PROCESSING));
    }

    @Test
    @Override
    public void storeFileSettingRollback() throws Exception {
        String fileId = testName.getMethodName();

        LocalFsProcessor service = new TestStorageService(storage);

        AtomicBoolean rollback = new AtomicBoolean(false);

        assertThrown(() -> service.storeFile(service.getFolderPath(fileId), fileId, getSipStream(), SIP_CHECKSUM, rollback))
                .isInstanceOf(FileCorruptedAfterStoreException.class);
        assertThat(rollback.get(), is(true));

        rollback.set(false);

        assertThrown(() -> service.storeFile(service.getFolderPath(fileId), fileId, getSipStream(), null, rollback))
                .isInstanceOf(Throwable.class);
        assertThat(rollback.get(), is(true));
    }

    @Test
    @Override
    public void storeAipOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        Path path = service.getFolderPath(sipId);
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        assertTrue(isInOneState(path.resolve(sipId), ObjectState.ARCHIVED));
        assertThat(streamToString(new FileInputStream(path.resolve(sipId).toFile())), is(SIP_CONTENT));
        assertTrue(isInOneState(path.resolve(xmlId), ObjectState.ARCHIVED));
        assertThat(streamToString(new FileInputStream(path.resolve(xmlId).toFile())), is(XML_CONTENT));

        assertThat(rollback.get(), is(false));
    }

    @Test
    @Override
    public void storeAipSetsRollback() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto(sipId, getSipStream(), null, getXmlStream(), null);
        AtomicBoolean rollback = new AtomicBoolean(false);
        assertThrown(() -> service.storeAip(aip, rollback)).isInstanceOf(GeneralException.class);
        assertThat(rollback.get(), is(true));
    }

    @Test
    @Override
    public void storeXmlOk() throws Exception {
        String sipId = testName.getMethodName();
        AtomicBoolean rollback = new AtomicBoolean(false);
        String xmlId = toXmlId(sipId, 99);
        service.storeObject(new ArchivalObjectDto("databaseId", xmlId, getXmlStream(), XML_CHECKSUM), rollback);
        Path path = service.getFolderPath(xmlId);
        assertTrue(isInOneState(path.resolve(xmlId), ObjectState.ARCHIVED));
        assertThat(streamToString(new FileInputStream(path.resolve(xmlId).toFile())), is(XML_CONTENT));
        assertThat(rollback.get(), is(false));
    }

    @Override
    public void removeSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.remove(sipId);
        service.remove(sipId);

        Path path = service.getFolderPath(sipId);
        assertThat(streamToString(new FileInputStream(path.resolve(sipId).toFile())), is(SIP_CONTENT));
        assertTrue(isInOneState(path.resolve(sipId), ObjectState.REMOVED));
    }

    @Override
    public void renewSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.remove(sipId);
        service.renew(sipId);
        service.renew(sipId);

        Path path = service.getFolderPath(sipId);
        assertThat(streamToString(new FileInputStream(path.resolve(sipId).toFile())), is(SIP_CONTENT));
        assertTrue(isInOneState(path.resolve(sipId), ObjectState.ARCHIVED));
    }

    @Override
    public void deleteSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.deleteSip(sipId);
        service.deleteSip(sipId);

        Path path = service.getFolderPath(sipId);
        assertThat(Files.exists(path.resolve(sipId)), is(false));
        assertTrue(isInOneState(path.resolve(sipId), ObjectState.DELETED));

        assertThat(streamToString(new FileInputStream(path.resolve(xmlId).toFile())), is(XML_CONTENT));
        assertTrue(isInOneState(path.resolve(xmlId), ObjectState.ARCHIVED));
    }

    @Override
    public void rollbackProcessingFile() throws Exception {
        String fileId = testName.getMethodName();

//preparation phase copied from rollbackAwareTest
        File file = new File("src/test/resources/8MiB+file");
        AtomicBoolean rollback = new AtomicBoolean(false);

        new Thread(() -> {
            try {
                //this test fails if waiting for too long
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            rollback.set(true);
        }).start();
//actual test
        Path path = service.getFolderPath(fileId);

        try (BufferedInputStream bos = new BufferedInputStream(new FileInputStream(file))) {
            service.storeFile(service.getFolderPath(fileId), fileId, bos, SIP_CHECKSUM, rollback);
        }
        assertTrue(isInOneState(path.resolve(fileId), ObjectState.PROCESSING));

        service.rollbackFile(path, fileId);

        assertThat(Files.exists(path.resolve(fileId)), is(false));
        assertTrue(isInOneState(path.resolve(fileId), ObjectState.ROLLED_BACK));
    }

    @Override
    public void rollbackStoredFileMultipleTimes() throws Exception {
        String fileId = testName.getMethodName();
        Path path = service.getFolderPath(fileId);

        service.storeFile(path, fileId, getSipStream(), SIP_CHECKSUM, new AtomicBoolean(false));
        service.rollbackFile(path, fileId);
        service.rollbackFile(path, fileId);

        assertThat(Files.exists(path.resolve(fileId)), is(false));
        assertTrue(isInOneState(path.resolve(fileId), ObjectState.ROLLED_BACK));
    }

    @Override
    public void rollbackCompletlyMissingFile() throws Exception {
        String fileId = testName.getMethodName();
        Path path = service.getFolderPath(fileId);

        service.rollbackFile(path, fileId);
        assertTrue(isInOneState(path.resolve(fileId), ObjectState.ROLLED_BACK));
    }

    @Override
    public void rollbackAipOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.rollbackAip(sipId);

        Path path = service.getFolderPath(sipId);
        assertThat(Files.exists(path.resolve(sipId)), is(false));
        assertThat(Files.exists(path.resolve(xmlId)), is(false));

        assertTrue(isInOneState(path.resolve(sipId), ObjectState.ROLLED_BACK));
        assertTrue(isInOneState(path.resolve(xmlId), ObjectState.ROLLED_BACK));
    }

    @Override
    public void rollbackXmlOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.rollbackObject(toXmlId(sipId, 1));

        Path path = service.getFolderPath(sipId);

        assertThat(Files.exists(path.resolve(xmlId)), is(false));

        assertThat(streamToString(new FileInputStream(path.resolve(sipId).toFile())), is(SIP_CONTENT));
        assertTrue(isInOneState(path.resolve(sipId), ObjectState.ARCHIVED));
        assertTrue(isInOneState(path.resolve(xmlId), ObjectState.ROLLED_BACK));
    }

    @Test
    @Override
    public void testConnection() {
        Storage badConfig = new Storage();
        badConfig.setName("bad storage");
        badConfig.setLocation("blah");
        LocalFsProcessor badService = new LocalFsProcessor(badConfig);
        assertThat(service.testConnection(), is(true));
        assertThat(badService.testConnection(), is(false));
    }

    private String getChecksumValue(Path fileBasePath, ChecksumType checksumType) throws FileNotFoundException {
        return streamToString(new FileInputStream(fileBasePath.resolveSibling(fileBasePath.getFileName() + "." + checksumType).toFile()));
    }

    private boolean isInOneState(Path fileBasePath, ObjectState state) {
        if (!Files.exists(fileBasePath.resolveSibling(fileBasePath.getFileName() + "." + state)))
            return false;
        return Arrays.stream(ObjectState.values()).filter(o -> o != state).noneMatch(
                o -> Files.exists(fileBasePath.resolveSibling(fileBasePath.getFileName() + "." + o))
        );
    }

    private static final class TestStorageService extends LocalFsProcessor {
        public TestStorageService(Storage storage) {
            super(storage);
        }

        @Override
        public Checksum computeChecksumRollbackAware(InputStream fileStream, ChecksumType checksumType, AtomicBoolean rollback) throws IOException {
            return new Checksum(ChecksumType.MD5, "alwayswrong");
        }
    }
}
