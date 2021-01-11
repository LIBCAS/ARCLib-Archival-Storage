package cz.cas.lib.arcstorage.storage.ceph;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3Object;
import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.storage.StorageServiceTest;
import cz.cas.lib.arcstorage.storage.StorageUtils;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import lombok.Getter;
import org.assertj.core.util.Strings;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static helper.ThrowableAssertion.assertThrown;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class CephS3Test extends StorageServiceTest {

    @Getter
    private CephS3StorageService service;
    private static Storage storage = new Storage();
    private static String bucketName;
    private static boolean https;
    private static String secretKey;
    private static String userKey;
    private static String sshServer;
    private static int sshPort;
    private static boolean virtualHost;
    private static Properties props = new Properties();
    private static String sshKeyPath;
    private static String sshUser;
    private static String cluster;
    private static String cephBinHome;

    @BeforeClass
    public static void beforeClass() throws IOException {
        props.load(ClassLoader.getSystemResourceAsStream("application.properties"));
        storage.setHost(props.getProperty("test.ceph.host"));
        storage.setName("ceph s3");
        storage.setPort(Integer.parseInt(props.getProperty("test.ceph.port")));
        storage.setPriority(1);
        storage.setStorageType(StorageType.CEPH);
        storage.setConfig("{\"adapterType\":\"S3\"," +
                "\"userKey\":\"" + props.getProperty("test.ceph.s3.user.key") + "\"," +
                "\"userSecret\":\"" + props.getProperty("test.ceph.s3.user.secret") + "\"}");
        storage.setReachable(true);
        userKey=props.getProperty("test.ceph.s3.user.key");
        String unsanitizedSecretKey = props.getProperty("test.ceph.s3.user.secret");
        secretKey= Strings.isNullOrEmpty(unsanitizedSecretKey)?"ldap":unsanitizedSecretKey;
        https=Boolean.parseBoolean(props.getProperty("test.ceph.https"));
        bucketName = props.getProperty("test.ceph.bucketname");
        virtualHost = Boolean.parseBoolean(props.getProperty("test.ceph.virtualHost"));
        sshPort = Integer.parseInt(props.getProperty("test.ceph.ssh.port"));
        sshUser = props.getProperty("test.ceph.ssh.user");
        sshKeyPath = props.getProperty("test.ceph.ssh.keyPath");
    }

    @Before
    public void before() throws IOException {
        service = new CephS3StorageService(storage, userKey, secretKey, https, null, 10000, sshServer, sshPort, sshKeyPath, sshUser, virtualHost, cluster, cephBinHome);
    }

    @Override
    public String getDataSpace() {
        return bucketName;
    }

    /**
     * tests that file larger than 8MiB (split into two parts) is successfully stored together with metadata
     */
    @Test
    public void storeLargeFileSuccessTest() throws Exception {

//        String fileId = testName.getMethodName();
        String fileId = "abortingmultiparts";

        AmazonS3 s3 = service.connect();

        File file = new File(LARGE_SIP_PATH);
        try (BufferedInputStream bos = new BufferedInputStream(new FileInputStream(file))) {
            service.storeFile(s3, fileId, bos, LARGE_SIP_CHECKSUM, new AtomicBoolean(false), bucketName, Instant.now());
        }

        S3Object object = s3.getObject(bucketName, fileId);
        Checksum checksumOfStoredFile = StorageUtils.computeChecksum(object.getObjectContent(), ChecksumType.MD5);
        assertThat(LARGE_SIP_CHECKSUM, is(checksumOfStoredFile));

        ObjectMetadata objectMetadata = s3.getObjectMetadata(bucketName, service.toMetadataObjectId(fileId));
        Map<String, String> userMetadata = objectMetadata.getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.ARCHIVED.toString()));
        assertThat(userMetadata.get(LARGE_SIP_CHECKSUM.getType().toString()), is(LARGE_SIP_CHECKSUM.getValue()));
        assertThat(userMetadata.get(CephS3StorageService.CREATED_KEY), not(isEmptyOrNullString()));
    }

    /**
     * tests that small file is successfully stored together with metadata
     */
    @Test
    @Override
    public void storeFileSuccessTest() throws Exception {

        String fileId = testName.getMethodName();
        AmazonS3 s3 = service.connect();

        service.storeFile(s3, fileId, getSipStream(), SIP_CHECKSUM, new AtomicBoolean(false), bucketName, Instant.now());

        S3Object object = s3.getObject(bucketName, fileId);
        Checksum checksumOfStoredFile = StorageUtils.computeChecksum(object.getObjectContent(), ChecksumType.MD5);
        assertThat(SIP_CHECKSUM, is(checksumOfStoredFile));

        ObjectMetadata objectMetadata = s3.getObjectMetadata(bucketName, service.toMetadataObjectId(fileId));
        Map<String, String> userMetadata = objectMetadata.getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.ARCHIVED.toString()));
        assertThat(userMetadata.get(SIP_CHECKSUM.getType().toString()), is(SIP_CHECKSUM.getValue()));
        assertThat(userMetadata.get(CephS3StorageService.CREATED_KEY), not(isEmptyOrNullString()));
    }

    /**
     * tests that when rollback flag is set by another thread the method returns and does not finish its job, thus the metadata object still contains {@link ObjectState#PROCESSING} state
     */
    @Test
    @Override
    public void storeFileRollbackAware() throws Exception {

        String fileId = testName.getMethodName();
        AmazonS3 s3 = service.connect();

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
            service.storeFile(s3, fileId, bos, LARGE_SIP_CHECKSUM, rollback, bucketName, Instant.now());
        }

        ObjectMetadata objectMetadata = s3.getObjectMetadata(bucketName, service.toMetadataObjectId(fileId));
        Map<String, String> userMetadata = objectMetadata.getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.PROCESSING.toString()));
    }

    /**
     * tests that rollback is set either when sipStorageChecksum of stored file does not match expectation or if there is another error (NPE for example)
     */
    @Test
    @Override
    public void storeFileSettingRollback() throws Exception {

        String fileId = testName.getMethodName();

        CephS3StorageService service = new TestStorageService(storage, userKey,secretKey,https, null);
        AmazonS3 s3 = service.connect();

        AtomicBoolean rollback = new AtomicBoolean(false);

        assertThrown(() -> service.storeFile(s3, fileId, getSipStream(), SIP_CHECKSUM, rollback, bucketName, Instant.now()))
                .isInstanceOf(FileCorruptedAfterStoreException.class);
        assertThat(rollback.get(), is(true));

        rollback.set(false);

        assertThrown(() -> service.storeFile(s3, fileId, getSipStream(), null, rollback, bucketName, Instant.now()))
                .isInstanceOf(Throwable.class);
        assertThat(rollback.get(), is(true));
    }

    @Test
    @Override
    public void storeAipOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, bucketName);

        AmazonS3 s3 = service.connect();
        S3Object sipObj = s3.getObject(bucketName, sipId);
        S3Object sipObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(sipId));
        S3Object xmlObj = s3.getObject(bucketName, xmlId);
        S3Object xmlObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(xmlId));

        assertThat(streamToString(sipObj.getObjectContent()), is(SIP_CONTENT));
        assertThat(sipObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(ObjectState.ARCHIVED.toString()));
        assertThat(streamToString(xmlObj.getObjectContent()), is(XML_CONTENT));
        assertThat(xmlObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(ObjectState.ARCHIVED.toString()));
        assertThat(rollback.get(), is(false));
    }

    @Test
    @Override
    public void storeXmlOk() throws Exception {
        String sipId = testName.getMethodName();
        AtomicBoolean rollback = new AtomicBoolean(false);
        String xmlId = toXmlId(sipId, 99);
        service.storeObject(new ArchivalObjectDto(xmlId, "databaseId", SIP_CHECKSUM, new User("ownerId"), getXmlStream(), ObjectState.PROCESSING, Instant.now(), ObjectType.XML), rollback, bucketName);

        AmazonS3 s3 = service.connect();
        S3Object xmlObj = s3.getObject(bucketName, xmlId);
        S3Object xmlObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(xmlId));

        assertThat(streamToString(xmlObj.getObjectContent()), is(XML_CONTENT));
        assertThat(xmlObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(ObjectState.ARCHIVED.toString()));
        assertThat(rollback.get(), is(false));
    }

    @Test
    @Override
    public void deleteSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, bucketName);

        service.delete(sipId, bucketName);
        service.delete(sipId, bucketName);

        AmazonS3 s3 = service.connect();
        assertThrown(() -> s3.getObject(bucketName, sipId)).isInstanceOf(AmazonS3Exception.class).messageContains("NoSuchKey");
        S3Object sipMeta = s3.getObject(bucketName, service.toMetadataObjectId(sipId));
        assertThat(sipMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(ObjectState.DELETED.toString()));

        S3Object xmlObj = s3.getObject(bucketName, xmlId);
        S3Object xmlObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(xmlId));

        assertThat(streamToString(xmlObj.getObjectContent()), is(XML_CONTENT));
        assertThat(xmlObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(ObjectState.ARCHIVED.toString()));
    }

    @Test
    @Override
    public void removeSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, bucketName);

        service.remove(sipId, bucketName);
        service.remove(sipId, bucketName);

        AmazonS3 s3 = service.connect();
        S3Object sipObj = s3.getObject(bucketName, sipId);
        S3Object sipObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(sipId));
        assertThat(streamToString(sipObj.getObjectContent()), is(SIP_CONTENT));
        assertThat(sipObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(ObjectState.REMOVED.toString()));
    }

    @Test
    @Override
    public void renewSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, bucketName);

        service.remove(sipId, bucketName);
        service.renew(sipId, bucketName);
        service.renew(sipId, bucketName);

        AmazonS3 s3 = service.connect();
        S3Object sipObj = s3.getObject(bucketName, sipId);
        S3Object sipObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(sipId));
        assertThat(streamToString(sipObj.getObjectContent()), is(SIP_CONTENT));
        assertThat(sipObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(ObjectState.ARCHIVED.toString()));
    }

    @Test
    @Override
    public void rollbackProcessingFile() throws Exception {
        String fileId = testName.getMethodName()+ UUID.randomUUID().toString();
        AmazonS3 s3 = service.connect();
//preparation phase copied from rollbackAwareTest
        File file = new File(LARGE_SIP_PATH);
        AtomicBoolean rollback = new AtomicBoolean(false);
        Instant creation = Instant.now();
        new Thread(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            rollback.set(true);
        }).start();

        try (BufferedInputStream bos = new BufferedInputStream(new FileInputStream(file))) {
            service.storeFile(s3, fileId, bos, LARGE_SIP_CHECKSUM, rollback, bucketName, creation);
        }

        ObjectMetadata objectMetadata = s3.getObjectMetadata(bucketName, service.toMetadataObjectId(fileId));
        Map<String, String> userMetadata = objectMetadata.getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.PROCESSING.toString()));
        //actual test
        ArchivalObjectDto dto = new ArchivalObjectDto();
        dto.setStorageId(fileId);
        dto.setChecksum(LARGE_SIP_CHECKSUM);
        dto.setCreated(creation);
        service.rollbackFile(s3, dto, bucketName);

        assertThrown(() -> s3.getObject(bucketName, fileId)).isInstanceOf(AmazonS3Exception.class).messageContains("NoSuchKey");
        userMetadata = s3.getObjectMetadata(bucketName, service.toMetadataObjectId(fileId)).getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.ROLLED_BACK.toString()));
    }

    @Test
    @Override
    public void rollbackStoredFileMultipleTimes() throws Exception {
        String fileId = testName.getMethodName();
        AmazonS3 s3 = service.connect();

        Instant creation = Instant.now();
        service.storeFile(s3, fileId, getSipStream(), SIP_CHECKSUM, new AtomicBoolean(false), bucketName, creation);
        ArchivalObjectDto dto = new ArchivalObjectDto();
        dto.setStorageId(fileId);
        dto.setChecksum(SIP_CHECKSUM);
        dto.setCreated(creation);
        service.rollbackFile(s3, dto, bucketName);
        service.rollbackFile(s3, dto, bucketName);

        assertThrown(() -> s3.getObject(bucketName, fileId)).isInstanceOf(AmazonS3Exception.class).messageContains("NoSuchKey");
        Map<String, String> userMetadata = s3.getObjectMetadata(bucketName, service.toMetadataObjectId(fileId)).getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.ROLLED_BACK.toString()));
    }

    @Test
    @Override
    public void rollbackCompletlyMissingFile() throws Exception {
        String fileId = testName.getMethodName();
        AmazonS3 s3 = service.connect();
        ArchivalObjectDto dto = new ArchivalObjectDto();
        dto.setStorageId(fileId);
        dto.setChecksum(new Checksum(ChecksumType.MD5, "hash"));
        dto.setCreated(Instant.now());
        service.rollbackFile(s3, dto, bucketName);
        Map<String, String> userMetadata = s3.getObjectMetadata(bucketName, service.toMetadataObjectId(fileId)).getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.ROLLED_BACK.toString()));
    }

    @Test
    @Override
    public void rollbackAipOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, bucketName);
        service.rollbackAip(aip, bucketName);

        AmazonS3 s3 = service.connect();
        assertThrown(() -> s3.getObject(bucketName, sipId)).isInstanceOf(AmazonS3Exception.class).messageContains("NoSuchKey");
        S3Object sipObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(sipId));

        assertThrown(() -> s3.getObject(bucketName, xmlId)).isInstanceOf(AmazonS3Exception.class).messageContains("NoSuchKey");
        S3Object xmlObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(xmlId));

        assertThat(sipObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(ObjectState.ROLLED_BACK.toString()));
        assertThat(xmlObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(ObjectState.ROLLED_BACK.toString()));
    }

    @Test
    @Override
    public void rollbackXmlOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback, bucketName);
        ArchivalObjectDto dto = new ArchivalObjectDto();
        dto.setStorageId(xmlId);
        service.rollbackObject(aip.getXml(), bucketName);

        AmazonS3 s3 = service.connect();
        S3Object sipObj = s3.getObject(bucketName, sipId);
        S3Object sipObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(sipId));

        assertThrown(() -> s3.getObject(bucketName, xmlId)).isInstanceOf(AmazonS3Exception.class).messageContains("NoSuchKey");
        S3Object xmlObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(xmlId));

        assertThat(streamToString(sipObj.getObjectContent()), is(SIP_CONTENT));
        assertThat(sipObjMeta.getObjectMetadata().getUserMetadata().get(CephS3StorageService.STATE_KEY), is(ObjectState.ARCHIVED.toString()));
        assertThat(xmlObjMeta.getObjectMetadata().getUserMetadata().get(CephS3StorageService.STATE_KEY), is(ObjectState.ROLLED_BACK.toString()));
    }

    /**
     * tests that connection to test cluster can be established and test user exists
     */
    @Test
    @Override
    public void testConnection() throws StorageException {
        CephS3StorageService badService = new TestStorageService(storage, "blah", "blah", https,"blah");
        assertThat(badService.testConnection(), is(false));
        assertThat(service.testConnection(), is(true));
        AmazonS3 s3 = service.connect();
        Owner s3AccountOwner = s3.getS3AccountOwner();
        service.getStorageState();
        assertThat(s3AccountOwner.getId(), is("arclib"));
    }


    @Test
    @Ignore
    public void createBucket() {
        AmazonS3 s3 = service.connect();
        s3.createBucket(bucketName);
    }

    @Test
    public void getStorageStateTest() throws Exception {
        StorageStateDto storageState = service.getStorageState();
        assertThat(storageState.getStorageStateData().keySet(), hasSize(8));
        assertThat((Collection<String>) storageState.getStorageStateData().get("cmd: " + service.CMD_STATUS), hasSize(greaterThan(0)));
        assertThat((Collection<String>) storageState.getStorageStateData().get("cmd: " + service.CMD_DF), hasSize(greaterThan(0)));
        assertThat((Collection<CephS3StorageService.BucketInfoDto>) storageState.getStorageStateData().get("buckets"), hasSize(greaterThan(0)));
        Map<String, List<String>> poolDetailMap = (Map<String, List<String>>) storageState.getStorageStateData().get("cmd: ceph pg ls-by-pool");
        assertThat(poolDetailMap.get("default"), not(empty()));
    }

    private static final class TestStorageService extends CephS3StorageService {
        public TestStorageService(Storage storage, String userAccessKey, String userSecretKey, boolean https, String region) {
            super(storage, userAccessKey, userSecretKey, https, region, 10000, sshServer, sshPort, null, null, virtualHost, cluster, cephBinHome);
        }

        @Override
        public Checksum computeChecksumRollbackAware(InputStream fileStream, ChecksumType checksumType, AtomicBoolean rollback) throws IOException {
            return new Checksum(ChecksumType.MD5, "alwayswrong");
        }
    }
}
