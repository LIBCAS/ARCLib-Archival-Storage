package cz.cas.lib.arcstorage.storage.ceph;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3Object;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.storage.StorageServiceTest;
import cz.cas.lib.arcstorage.storage.StorageUtils;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import lombok.Getter;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static helper.ThrowableAssertion.assertThrown;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class CephS3Test extends StorageServiceTest {

    @Getter
    private CephS3StorageService service = new CephS3StorageService(storage, "BLZBGL9ZDD23WD0GL8V8", "pPYbINKQxEBLdxhzbycUI00UmTD4uaHjDel1IPui", null);
    private static Storage storage = new Storage();
    private static String bucketName;

    @BeforeClass
    public static void beforeClass() throws IOException {
        Properties props = new Properties();
        props.load(ClassLoader.getSystemResourceAsStream("application.properties"));
        storage.setHost(props.getProperty("test.ceph.host"));
        storage.setName("ceph s3");
        storage.setPort(Integer.parseInt(props.getProperty("test.ceph.port")));
        storage.setPriority(1);
        storage.setStorageType(StorageType.CEPH);
        storage.setLocation(props.getProperty("test.ceph.bucketname"));
        storage.setConfig("{\"adapterType\":\"S3\"," +
                "\"userKey\":\"" + props.getProperty("test.ceph.s3.user.key") + "\"," +
                "\"userSecret\":\"" + props.getProperty("test.ceph.s3.user.secret") + "\"}");
        storage.setReachable(true);
        bucketName = storage.getLocation();
    }

    /**
     * tests that file larger than 8MiB (split into two parts) is successfully stored together with metadata
     */
    @Test
    public void storeLargeFileSuccessTest() throws Exception {

        String fileId = testName.getMethodName();
        Checksum checksum = new Checksum(ChecksumType.MD5, "A95E65A3DE9704CB0C5B5C68AE41AE6F");
        AmazonS3 s3 = service.connect();

        File file = new File("src/test/resources/8MiB+file");
        try (BufferedInputStream bos = new BufferedInputStream(new FileInputStream(file))) {
            service.storeFile(s3, fileId, bos, checksum, new AtomicBoolean(false));
        }

        S3Object object = s3.getObject(service.getStorage().getLocation(), fileId);
        Checksum checksumOfStoredFile = StorageUtils.computeChecksum(object.getObjectContent(), ChecksumType.MD5);
        assertThat(checksum, is(checksumOfStoredFile));

        ObjectMetadata objectMetadata = s3.getObjectMetadata(service.getStorage().getLocation(), service.toMetadataObjectId(fileId));
        Map<String, String> userMetadata = objectMetadata.getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.ARCHIVED.toString()));
        assertThat(userMetadata.get(checksum.getType().toString()), is(checksum.getValue()));
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

        service.storeFile(s3, fileId, getSipStream(), SIP_CHECKSUM, new AtomicBoolean(false));

        S3Object object = s3.getObject(service.getStorage().getLocation(), fileId);
        Checksum checksumOfStoredFile = StorageUtils.computeChecksum(object.getObjectContent(), ChecksumType.MD5);
        assertThat(SIP_CHECKSUM, is(checksumOfStoredFile));

        ObjectMetadata objectMetadata = s3.getObjectMetadata(service.getStorage().getLocation(), service.toMetadataObjectId(fileId));
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

        File file = new File("src/test/resources/8MiB+file");
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
            service.storeFile(s3, fileId, bos, SIP_CHECKSUM, rollback);
        }

        ObjectMetadata objectMetadata = s3.getObjectMetadata(service.getStorage().getLocation(), service.toMetadataObjectId(fileId));
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

        CephS3StorageService service = new TestStorageService(storage, "BLZBGL9ZDD23WD0GL8V8", "pPYbINKQxEBLdxhzbycUI00UmTD4uaHjDel1IPui", null);
        AmazonS3 s3 = service.connect();

        AtomicBoolean rollback = new AtomicBoolean(false);

        assertThrown(() -> service.storeFile(s3, fileId, getSipStream(), SIP_CHECKSUM, rollback))
                .isInstanceOf(FileCorruptedAfterStoreException.class);
        assertThat(rollback.get(), is(true));

        rollback.set(false);

        assertThrown(() -> service.storeFile(s3, fileId, getSipStream(), null, rollback))
                .isInstanceOf(Throwable.class);
        assertThat(rollback.get(), is(true));
    }

    @Test
    @Override
    public void storeAipOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

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
        service.storeObject(new ArchivalObjectDto("databaseId", xmlId, getXmlStream(), SIP_CHECKSUM), rollback);

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
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.deleteSip(sipId);
        service.deleteSip(sipId);

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
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.remove(sipId);
        service.remove(sipId);

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
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.remove(sipId);
        service.renew(sipId);
        service.renew(sipId);

        AmazonS3 s3 = service.connect();
        S3Object sipObj = s3.getObject(bucketName, sipId);
        S3Object sipObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(sipId));
        assertThat(streamToString(sipObj.getObjectContent()), is(SIP_CONTENT));
        assertThat(sipObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(ObjectState.ARCHIVED.toString()));
    }

    @Test
    @Override
    public void rollbackProcessingFile() throws Exception {
        String fileId = testName.getMethodName();
        AmazonS3 s3 = service.connect();
//preparation phase copied from rollbackAwareTest
        File file = new File("src/test/resources/8MiB+file");
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
            service.storeFile(s3, fileId, bos, SIP_CHECKSUM, rollback);
        }

        ObjectMetadata objectMetadata = s3.getObjectMetadata(service.getStorage().getLocation(), service.toMetadataObjectId(fileId));
        Map<String, String> userMetadata = objectMetadata.getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.PROCESSING.toString()));
//actual test
        service.rollbackFile(s3, fileId);

        assertThrown(() -> s3.getObject(bucketName, fileId)).isInstanceOf(AmazonS3Exception.class).messageContains("NoSuchKey");
        userMetadata = s3.getObjectMetadata(bucketName, service.toMetadataObjectId(fileId)).getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.ROLLED_BACK.toString()));
    }

    @Test
    @Override
    public void rollbackStoredFileMultipleTimes() throws Exception {
        String fileId = testName.getMethodName();
        AmazonS3 s3 = service.connect();

        service.storeFile(s3, fileId, getSipStream(), SIP_CHECKSUM, new AtomicBoolean(false));
        service.rollbackFile(s3, fileId);
        service.rollbackFile(s3, fileId);

        assertThrown(() -> s3.getObject(bucketName, fileId)).isInstanceOf(AmazonS3Exception.class).messageContains("NoSuchKey");
        Map<String, String> userMetadata = s3.getObjectMetadata(bucketName, service.toMetadataObjectId(fileId)).getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.ROLLED_BACK.toString()));
    }

    @Test
    @Override
    public void rollbackCompletlyMissingFile() throws Exception {
        String fileId = testName.getMethodName();
        AmazonS3 s3 = service.connect();
        service.rollbackFile(s3, fileId);
        Map<String, String> userMetadata = s3.getObjectMetadata(bucketName, service.toMetadataObjectId(fileId)).getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.ROLLED_BACK.toString()));
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
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.rollbackObject(toXmlId(sipId, 1));

        AmazonS3 s3 = service.connect();
        S3Object sipObj = s3.getObject(bucketName, sipId);
        S3Object sipObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(sipId));

        assertThrown(() -> s3.getObject(bucketName, xmlId)).isInstanceOf(AmazonS3Exception.class).messageContains("NoSuchKey");
        S3Object xmlObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(xmlId));

        assertThat(streamToString(sipObj.getObjectContent()), is(SIP_CONTENT));
        assertThat(sipObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(ObjectState.ARCHIVED.toString()));
        assertThat(xmlObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(ObjectState.ROLLED_BACK.toString()));
    }

    /**
     * tests that connection to test cluster can be established and test user exists
     */
    @Test
    @Override
    public void testConnection() {
        CephS3StorageService badService = new TestStorageService(storage, "blah", "blah", "blah");
        assertThat(badService.testConnection(), is(false));
        assertThat(service.testConnection(), is(true));
        AmazonS3 s3 = service.connect();
        Owner s3AccountOwner = s3.getS3AccountOwner();
        assertThat(s3AccountOwner.getId(), is("arcephUser"));
    }


// use this to easily create test bucket if it does not exist yet
//    @Test @Override
//    public void createBucket(){
//        AmazonS3 s3 = service.connect();
//        s3.createBucket(storage.getLocation());
//    }

    private static final class TestStorageService extends CephS3StorageService {
        public TestStorageService(Storage storage, String userAccessKey, String userSecretKey, String region) {
            super(storage, userAccessKey, userSecretKey, region);
        }

        @Override
        public Checksum computeChecksumRollbackAware(InputStream fileStream, ChecksumType checksumType, AtomicBoolean rollback) throws IOException {
            return new Checksum(ChecksumType.MD5, "alwayswrong");
        }
    }
}
