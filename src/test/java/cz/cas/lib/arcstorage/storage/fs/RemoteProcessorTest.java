package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.storage.StorageServiceTest;
import cz.cas.lib.arcstorage.storage.exception.CantParseMetadataFile;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import lombok.Getter;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static helper.ThrowableAssertion.assertThrown;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

public class RemoteProcessorTest extends StorageServiceTest {
    @Getter
    private RemoteFsProcessor service;
    private static Storage storage = new Storage();
    private static String S = "/";
    private static String sshKeyPath;
    private static String sshUser;
    private static SFTPClient sftp;
    private static Properties props;
    private static String dataSpace;

    @BeforeClass
    public static void beforeClass() throws IOException {
        //get values from properties
        props = new Properties();
        props.load(ClassLoader.getSystemResourceAsStream("application.properties"));
        dataSpace = props.getProperty("test.sftp.dataspace");
        S = props.getProperty("test.sftp.separator");
        storage.setName("sftp storage");
        storage.setHost(props.getProperty("test.sftp.host"));
        storage.setPort(Integer.parseInt(props.getProperty("test.sftp.port")));
        String testFolder = props.getProperty("test.sftp.folderpath") + S + dataSpace;
        sshUser = props.getProperty("test.sftp.ssh.user");
        sshKeyPath = props.getProperty("test.sftp.ssh.keyPath");
        //create connection held for the whole time
        //public methods of the services uses own connection, this is used only for private methods
        SSHClient ssh = new SSHClient();
        ssh.addHostKeyVerifier(new PromiscuousVerifier());
        ssh.connect(storage.getHost(), storage.getPort());
        ssh.authPublickey(sshUser, sshKeyPath);
        sftp = ssh.newSFTPClient();
        if (sftp.statExistence(testFolder) == null)
            sftp.mkdir(testFolder);
    }

    @AfterClass
    public static void afterClass() throws IOException {
        IOUtils.closeQuietly(sftp);
    }

    @Before
    public void before() throws IOException {
        service = new RemoteFsProcessor(storage, props.getProperty("test.sftp.folderpath"), sshKeyPath, sshUser, 10000);
    }

    @Override
    public String getDataSpace() {
        return dataSpace;
    }

    @Test
    @Override
    public void storeFileSuccessTest() throws Exception {
        String fileId = testName.getMethodName();
        String folder = getFolderPath(fileId);

        service.storeFile(sftp, folder, fileId, getSipStream(), SIP_CHECKSUM, new AtomicBoolean(false), Instant.now());
        assertThat(getFileContent(folder + S + fileId), is(SIP_CONTENT));
        ObjectMetadata objectMetadata = readObjectMetadata(sftp, folder, fileId);
        assertThat(objectMetadata.getChecksum(), is(SIP_CHECKSUM));
        assertThat(objectMetadata.getState(), is(ObjectState.ARCHIVED));
    }

    @Test
    public void getLargeFileSuccessTest() throws Exception {
        String fileId = testName.getMethodName();
        String folder = getFolderPath(fileId);
        File file = new File(LARGE_SIP_PATH);
        service.storeFile(sftp, folder, fileId, new FileInputStream(file), LARGE_SIP_CHECKSUM, new AtomicBoolean(false), Instant.now());
        assertThat(isInState(folder, fileId, ObjectState.ARCHIVED), is(true));
        ObjectRetrievalResource object = service.getObject(fileId, dataSpace);
        assertThat(object.getInputStream(), not(nullValue()));
    }

    @Test
    @Override
    public void storeFileRollbackAware() throws Exception {
        String fileId = testName.getMethodName();
        String folder = getFolderPath(fileId);

        File file = new File(LARGE_SIP_PATH);
        AtomicBoolean rollback = new AtomicBoolean(false);

        new Thread(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            rollback.set(true);
        }).start();

        try (BufferedInputStream bos = new BufferedInputStream(new FileInputStream(file))) {
            service.storeFile(sftp, folder, fileId, bos, LARGE_SIP_CHECKSUM, rollback, Instant.now());
        }
        assertThat(isInState(folder, fileId, ObjectState.PROCESSING), is(true));
    }

    @Test
    @Override
    public void storeFileSettingRollback() throws Exception {
        String fileId = testName.getMethodName();
        String folder = getFolderPath(fileId);
        RemoteFsProcessor service = new TestServiceSettingRollback(storage);
        AtomicBoolean rollback = new AtomicBoolean(false);

        assertThrown(() -> service.storeFile(sftp, folder, fileId, getSipStream(), SIP_CHECKSUM, rollback, Instant.now()))
                .isInstanceOf(FileCorruptedAfterStoreException.class);
        assertThat(rollback.get(), is(true));

        rollback.set(false);

        assertThrown(() -> service.storeFile(sftp, folder, fileId, getSipStream(), null, rollback, Instant.now()))
                .isInstanceOf(Throwable.class);
        assertThat(rollback.get(), is(true));
    }

    @Test
    @Override
    public void storeAipOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        String path = getFolderPath(sipId);
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, dataSpace);

        assertThat(isInState(path, sipId, ObjectState.ARCHIVED), is(true));
        assertThat(getFileContent(path + S + sipId), is(SIP_CONTENT));
        assertThat(isInState(path, xmlId, ObjectState.ARCHIVED), is(true));
        assertThat(getFileContent(path + S + xmlId), is(XML_CONTENT));

        assertThat(rollback.get(), is(false));
    }

    @Test
    @Override
    public void storeXmlOk() throws Exception {
        String sipId = testName.getMethodName();
        AtomicBoolean rollback = new AtomicBoolean(false);
        String xmlId = toXmlId(sipId, 99);
        service.storeObject(new ArchivalObjectDto(xmlId, "databaseId", XML_CHECKSUM, new User("ownerId"), getXmlStream(), ObjectState.PROCESSING, Instant.now(), ObjectType.XML), rollback, dataSpace);
        String path = getFolderPath(xmlId) + S + xmlId;
        assertThat(isInState(getFolderPath(xmlId), xmlId, ObjectState.ARCHIVED), is(true));
        assertThat(getFileContent(path), is(XML_CONTENT));
        assertThat(rollback.get(), is(false));
    }

    @Test
    @Override
    public void removeSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, dataSpace);

        service.remove(sipId, dataSpace);
        service.remove(sipId, dataSpace);

        String path = getFolderPath(sipId) + S + sipId;
        assertThat(getFileContent(path), is(SIP_CONTENT));
        assertThat(isInState(getFolderPath(sipId), sipId, ObjectState.REMOVED), is(true));
    }

    @Test
    @Override
    public void renewSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, dataSpace);

        service.remove(sipId, dataSpace);
        service.renew(sipId, dataSpace);
        service.renew(sipId, dataSpace);

        String path = getFolderPath(sipId) + S + sipId;
        assertThat(getFileContent(path), is(SIP_CONTENT));
        assertThat(isInState(getFolderPath(sipId), sipId, ObjectState.ARCHIVED), is(true));
    }

    @Test
    @Override
    public void deleteSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, dataSpace);

        service.delete(sipId, dataSpace);
        service.delete(sipId, dataSpace);

        String path = getFolderPath(sipId);

        assertThat(sftp.statExistence(path + S + sipId), nullValue());
        assertThat(isInState(path, sipId, ObjectState.DELETED), is(true));

        assertThat(getFileContent(path + S + xmlId), is(XML_CONTENT));
        assertThat(isInState(path, xmlId, ObjectState.ARCHIVED), is(true));
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
                //this test fails if waiting for too long
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            rollback.set(true);
        }).start();
//actual test
        String path = getFolderPath(fileId);

        try (BufferedInputStream bos = new BufferedInputStream(new FileInputStream(file))) {
            service.storeFile(sftp, getFolderPath(fileId), fileId, bos, LARGE_SIP_CHECKSUM, rollback, Instant.now());
        } catch (Exception e) {}

        ArchivalObjectDto dto = new ArchivalObjectDto(fileId, null, LARGE_SIP_CHECKSUM, null, null, ObjectState.PROCESSING, Instant.now(), ObjectType.SIP);
        service.rollbackFile(sftp, path, dto);

        assertThat(sftp.statExistence(path + S + fileId), nullValue());
        assertThat(isInState(path, fileId, ObjectState.ROLLED_BACK), is(true));
    }

    @Test
    @Override
    public void rollbackStoredFileMultipleTimes() throws Exception {
        String fileId = testName.getMethodName();
        String path = getFolderPath(fileId);

        service.storeFile(sftp, path, fileId, getSipStream(), SIP_CHECKSUM, new AtomicBoolean(false), Instant.now());
        ArchivalObjectDto dto = new ArchivalObjectDto(fileId, null, SIP_CHECKSUM, null, null, ObjectState.ARCHIVED, Instant.now(), ObjectType.SIP);
        service.rollbackFile(sftp, path, dto);
        service.rollbackFile(sftp, path, dto);

        assertThat(sftp.statExistence(path + S + fileId), nullValue());
        assertThat(isInState(path, fileId, ObjectState.ROLLED_BACK), is(true));
    }

    @Test
    @Override
    public void rollbackCompletlyMissingFile() throws Exception {
        String fileId = testName.getMethodName();
        String path = getFolderPath(fileId);

        ArchivalObjectDto dto = new ArchivalObjectDto(fileId, null, XML_CHECKSUM, null, null, ObjectState.ARCHIVED, Instant.now(), ObjectType.XML);
        service.rollbackFile(sftp, path, dto);
        assertThat(isInState(path, fileId, ObjectState.ROLLED_BACK), is(true));
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

        String path = getFolderPath(sipId);
        assertThat(sftp.statExistence(path + S + sipId), nullValue());
        assertThat(sftp.statExistence(path + S + xmlId), nullValue());

        assertThat(isInState(path, sipId, ObjectState.ROLLED_BACK), is(true));
        assertThat(isInState(path, xmlId, ObjectState.ROLLED_BACK), is(true));
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

        String path = getFolderPath(sipId);

        assertThat(sftp.statExistence(path + S + xmlId), nullValue());

        assertThat(getFileContent(path + S + sipId), is(SIP_CONTENT));
        assertThat(isInState(path, sipId, ObjectState.ARCHIVED), is(true));
        assertThat(isInState(path, xmlId, ObjectState.ROLLED_BACK), is(true));
    }

    @Test
    public void testConnection() {
        RemoteFsProcessor badService = new RemoteFsProcessor(storage, "/blah", sshKeyPath, "invaliduser", 10000);
        assertThat(service.testConnection(), is(true));
        assertThat(badService.testConnection(), is(false));
    }

    private boolean isInState(String folder, String fileId, ObjectState state) throws IOException, CantParseMetadataFile, IOStorageException {
        ObjectMetadata metadataAtStorage = readObjectMetadata(sftp, folder, fileId);
        return state == metadataAtStorage.getState();
    }

    private String getFileContent(String pathToFile) throws IOException {
        PipedInputStream in = new PipedInputStream();
        PipedOutputStream out = new PipedOutputStream(in);
        if (sftp.statExistence(pathToFile) == null)
            throw new FileNotFoundException(pathToFile);
        new Thread(() -> {
            try {
                sftp.get(pathToFile, new RemoteFsProcessor.OutputStreamSource(out));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).start();
        return streamToString(in);
    }

    private String getFolderPath(String fileName) {
        return service.getFolderPath(fileName, dataSpace);
    }

    private static final class TestServiceSettingRollback extends RemoteFsProcessor {
        public TestServiceSettingRollback(Storage storage) {
            super(storage, props.getProperty("test.sftp.folderpath"), sshKeyPath, sshUser, 10000);
        }

        @Override
        public Checksum computeChecksumRollbackAware(InputStream fileStream, ChecksumType checksumType, AtomicBoolean rollback) throws IOException {
            return new Checksum(ChecksumType.MD5, "alwayswrong");
        }
    }

    private ObjectMetadata readObjectMetadata(SFTPClient sftp, String folder, String fileId) throws IOStorageException, CantParseMetadataFile {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            sftp.get(folder + getService().getSeparator() + fileId + ".meta", new RemoteFsProcessor.OutputStreamSource(bos));
        } catch (IOException e) {
            throw new IOStorageException(e, storage);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bos.toByteArray())));
        List<String> lines = reader.lines().collect(Collectors.toList());
        return new ObjectMetadata(lines, fileId, storage);
    }
}
