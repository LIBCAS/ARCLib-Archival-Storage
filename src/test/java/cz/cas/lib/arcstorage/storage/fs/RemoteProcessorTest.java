package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.storage.StorageServiceTest;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
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
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static helper.ThrowableAssertion.assertThrown;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class RemoteProcessorTest extends StorageServiceTest {
    @Getter
    private RemoteFsProcessor service;
    private static Storage storage = new Storage();
    private static String S = "/";
    private static final String KEY_PATH = "src/main/resources/arcstorage.ppk";
    private static final String USER = "arcstorage";
    private static SFTPClient sftp;

    @BeforeClass
    public static void beforeClass() throws IOException {
        //get values from properties
        Properties props = new Properties();
        props.load(ClassLoader.getSystemResourceAsStream("application.properties"));
        S = props.getProperty("test.sftp.separator");
        storage.setName("sftp storage");
        storage.setLocation(props.getProperty("test.sftp.folderpath") + S + "test");
        storage.setHost(props.getProperty("test.sftp.host"));
        storage.setPort(Integer.parseInt(props.getProperty("test.sftp.port")));

        //create connection held for the whole time
        //public methods of the services uses own connection, this is used only for private methods
        SSHClient ssh = new SSHClient();
        ssh.addHostKeyVerifier(new PromiscuousVerifier());
        ssh.connect(storage.getHost(), storage.getPort());
        ssh.authPublickey(USER, KEY_PATH);
        sftp = ssh.newSFTPClient();
        if (sftp.statExistence(storage.getLocation()) == null)
            sftp.mkdir(storage.getLocation());
    }

    @AfterClass
    public static void afterClass() throws IOException {
        IOUtils.closeQuietly(sftp);
    }

    @Before
    public void before() throws IOException {
        service = new RemoteFsProcessor(storage, S, KEY_PATH);
    }

    @Test
    @Override
    public void storeFileSuccessTest() throws Exception {
        String fileId = testName.getMethodName();
        String folder = service.getFolderPath(fileId);

        service.storeFile(sftp, folder, fileId, getSipStream(), SIP_CHECKSUM, new AtomicBoolean(false));
        assertThat(getFileContent(folder + S + fileId), is(SIP_CONTENT));
        assertThat(getFileContent(folder + S + fileId + "." + SIP_CHECKSUM.getType()), is(SIP_CHECKSUM.getValue()));
        assertThat(isInState(folder + S + fileId, ObjectState.ARCHIVED), is(true));
    }

    @Test
    @Override
    public void storeFileRollbackAware() throws Exception {
        String fileId = testName.getMethodName();
        String folder = service.getFolderPath(fileId);

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
            service.storeFile(sftp, folder, fileId, bos, LARGE_SIP_CHECKSUM, rollback);
        }
        assertThat(isInState(folder + S + fileId, ObjectState.PROCESSING), is(true));
    }

    @Test
    @Override
    public void storeFileSettingRollback() throws Exception {
        String fileId = testName.getMethodName();
        String folder = service.getFolderPath(fileId);
        RemoteFsProcessor service = new TestServiceSettingRollback(storage);
        AtomicBoolean rollback = new AtomicBoolean(false);

        assertThrown(() -> service.storeFile(sftp, folder, fileId, getSipStream(), SIP_CHECKSUM, rollback))
                .isInstanceOf(FileCorruptedAfterStoreException.class);
        assertThat(rollback.get(), is(true));

        rollback.set(false);

        assertThrown(() -> service.storeFile(sftp, folder, fileId, getSipStream(), null, rollback))
                .isInstanceOf(Throwable.class);
        assertThat(rollback.get(), is(true));
    }

    @Test
    @Override
    public void storeAipOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        String path = service.getFolderPath(sipId);
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        assertThat(isInState(path + S + sipId, ObjectState.ARCHIVED), is(true));
        assertThat(getFileContent(path + S + sipId), is(SIP_CONTENT));
        assertThat(isInState(path + S + xmlId, ObjectState.ARCHIVED), is(true));
        assertThat(getFileContent(path + S + xmlId), is(XML_CONTENT));

        assertThat(rollback.get(), is(false));
    }

    @Test
    @Override
    public void storeXmlOk() throws Exception {
        String sipId = testName.getMethodName();
        AtomicBoolean rollback = new AtomicBoolean(false);
        String xmlId = toXmlId(sipId, 99);
        service.storeObject(new ArchivalObjectDto("databaseId", xmlId, getXmlStream(), XML_CHECKSUM), rollback);
        String path = service.getFolderPath(xmlId) + S + xmlId;
        assertThat(isInState(path, ObjectState.ARCHIVED), is(true));
        assertThat(getFileContent(path), is(XML_CONTENT));
        assertThat(rollback.get(), is(false));
    }

    @Test
    @Override
    public void removeSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.remove(sipId);
        service.remove(sipId);

        String path = service.getFolderPath(sipId) + S + sipId;
        assertThat(getFileContent(path), is(SIP_CONTENT));
        assertThat(isInState(path, ObjectState.REMOVED), is(true));
    }

    @Test
    @Override
    public void renewSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.remove(sipId);
        service.renew(sipId);
        service.renew(sipId);

        String path = service.getFolderPath(sipId) + S + sipId;
        assertThat(getFileContent(path), is(SIP_CONTENT));
        assertThat(isInState(path, ObjectState.ARCHIVED), is(true));
    }

    @Test
    @Override
    public void deleteSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.deleteSip(sipId);
        service.deleteSip(sipId);

        String path = service.getFolderPath(sipId);

        assertThat(sftp.statExistence(path + S + sipId), nullValue());
        assertThat(isInState(path + S + sipId, ObjectState.DELETED), is(true));

        assertThat(getFileContent(path + S + xmlId), is(XML_CONTENT));
        assertThat(isInState(path + S + xmlId, ObjectState.ARCHIVED), is(true));
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
        String path = service.getFolderPath(fileId);

        try (BufferedInputStream bos = new BufferedInputStream(new FileInputStream(file))) {
            service.storeFile(sftp, service.getFolderPath(fileId), fileId, bos, LARGE_SIP_CHECKSUM, rollback);
        } catch (Exception e) {}

        service.rollbackFile(sftp, path, fileId);

        assertThat(sftp.statExistence(path + S + fileId), nullValue());
        assertThat(isInState(path + S + fileId, ObjectState.ROLLED_BACK), is(true));
    }

    @Test
    @Override
    public void rollbackStoredFileMultipleTimes() throws Exception {
        String fileId = testName.getMethodName();
        String path = service.getFolderPath(fileId);

        service.storeFile(sftp, path, fileId, getSipStream(), SIP_CHECKSUM, new AtomicBoolean(false));
        service.rollbackFile(sftp, path, fileId);
        service.rollbackFile(sftp, path, fileId);

        assertThat(sftp.statExistence(path + S + fileId), nullValue());
        assertThat(isInState(path + S + fileId, ObjectState.ROLLED_BACK), is(true));
    }

    @Test
    @Override
    public void rollbackCompletlyMissingFile() throws Exception {
        String fileId = testName.getMethodName();
        String path = service.getFolderPath(fileId);

        service.rollbackFile(sftp, path, fileId);
        assertThat(isInState(path + S + fileId, ObjectState.ROLLED_BACK), is(true));
    }

    @Test
    @Override
    public void rollbackAipOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.rollbackAip(sipId);

        String path = service.getFolderPath(sipId);
        assertThat(sftp.statExistence(path + S + sipId), nullValue());
        assertThat(sftp.statExistence(path + S + xmlId), nullValue());

        assertThat(isInState(path + S + sipId, ObjectState.ROLLED_BACK), is(true));
        assertThat(isInState(path + S + xmlId, ObjectState.ROLLED_BACK), is(true));
    }

    @Test
    @Override
    public void rollbackXmlOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.rollbackObject(toXmlId(sipId, 1));

        String path = service.getFolderPath(sipId);

        assertThat(sftp.statExistence(path + S + xmlId), nullValue());

        assertThat(getFileContent(path + S + sipId), is(SIP_CONTENT));
        assertThat(isInState(path + S + sipId, ObjectState.ARCHIVED), is(true));
        assertThat(isInState(path + S + xmlId, ObjectState.ROLLED_BACK), is(true));
    }

    @Test
    public void testConnection() {
        RemoteFsProcessor badService = new RemoteFsProcessor(storage, "/", "src/main/resources/arcstorage.pub");
        assertThat(service.testConnection(), is(true));
        assertThat(badService.testConnection(), is(false));
    }

    private boolean isInState(String filePath, ObjectState state) throws IOException {
        if (sftp.statExistence(filePath + "." + state) == null)
            return false;
        //when there is a rolled_back file, other files do not not have to be deleted
        if (state == ObjectState.ROLLED_BACK)
            return true;
        if (state == ObjectState.DELETED)
            return !(sftp.statExistence(filePath + "." + ObjectState.ROLLED_BACK) != null ||
                    sftp.statExistence(filePath + "." + ObjectState.PROCESSING) != null);
        return Arrays.stream(ObjectState.values()).filter(o -> o != state).noneMatch(
                o -> {
                    try {
                        return sftp.statExistence(filePath + "." + o) != null;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
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

    private static final class TestServiceSettingRollback extends RemoteFsProcessor {
        public TestServiceSettingRollback(Storage storage) {
            super(storage, S, KEY_PATH);
        }

        @Override
        public Checksum computeChecksumRollbackAware(InputStream fileStream, ChecksumType checksumType, AtomicBoolean rollback) throws IOException {
            return new Checksum(ChecksumType.MD5, "alwayswrong");
        }
    }
}
