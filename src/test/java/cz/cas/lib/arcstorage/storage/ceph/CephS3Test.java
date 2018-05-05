package cz.cas.lib.arcstorage.storage.ceph;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3Object;
import cz.cas.lib.arcstorage.domain.ChecksumType;
import cz.cas.lib.arcstorage.domain.ObjectState;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.storage.StorageServiceTest;
import cz.cas.lib.arcstorage.storage.StorageUtils;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import cz.cas.lib.arcstorage.storage.exception.FileDoesNotExistException;
import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static helper.ThrowableAssertion.assertThrown;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class CephS3Test implements StorageServiceTest {

    private CephS3StorageService service = new CephS3StorageService(config, "BLZBGL9ZDD23WD0GL8V8", "pPYbINKQxEBLdxhzbycUI00UmTD4uaHjDel1IPui", null);
    private static StorageConfig config = new StorageConfig();
    private static String bucketName;
    private static final String SIP_CONTENT = "blah";
    private static final Checksum SIP_CHECKSUM = new Checksum(ChecksumType.MD5, "6F1ED002AB5595859014EBF0951522D9");
    private static final String XML_CONTENT = "blik";
    private static final Checksum XML_CHECKSUM = new Checksum(ChecksumType.SHA512, "7EE090163B74E20DFEA30A7DD3CA969F75B1CCD713844F6B6ECD08F101AD04711C0D931BF372C32284BBF656CAC459AFC217C1F290808D0EB35AFFD569FF899C");

    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void beforeClass() {
        config.setHost("192.168.10.60");
        config.setName("ceph s3");
        config.setPort(7480);
        config.setPriority(1);
        config.setStorageType(StorageType.CEPH);
        config.setLocation("arclib.bucket1");
        config.setConfig("{\"adapterType\":\"S3\",\"userKey\":\"BLZBGL9ZDD23WD0GL8V8\",\"userSecret\":\"pPYbINKQxEBLdxhzbycUI00UmTD4uaHjDel1IPui\"}");
        config.setReachable(true);
        bucketName = config.getLocation();
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

        S3Object object = s3.getObject(service.getStorageConfig().getLocation(), fileId);
        Checksum checksumOfStoredFile = StorageUtils.computeChecksum(object.getObjectContent(), ChecksumType.MD5);
        assertThat(checksum, is(checksumOfStoredFile));

        ObjectMetadata objectMetadata = s3.getObjectMetadata(service.getStorageConfig().getLocation(), service.toMetadataObjectId(fileId));
        Map<String, String> userMetadata = objectMetadata.getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.ARCHIVED.toString()));
        assertThat(userMetadata.get(checksum.getType().toString()), is(checksum.getHash()));
        assertThat(userMetadata.get(CephS3StorageService.CREATED_KEY), not(isEmptyOrNullString()));
    }

    /**
     * tests that small file is successfully stored together with metadata
     */
    @Test
    public void storeSmallFileSuccessTest() throws Exception {

        String fileId = testName.getMethodName();
        AmazonS3 s3 = service.connect();

        service.storeFile(s3, fileId, getSipStream(), SIP_CHECKSUM, new AtomicBoolean(false));

        S3Object object = s3.getObject(service.getStorageConfig().getLocation(), fileId);
        Checksum checksumOfStoredFile = StorageUtils.computeChecksum(object.getObjectContent(), ChecksumType.MD5);
        assertThat(SIP_CHECKSUM, is(checksumOfStoredFile));

        ObjectMetadata objectMetadata = s3.getObjectMetadata(service.getStorageConfig().getLocation(), service.toMetadataObjectId(fileId));
        Map<String, String> userMetadata = objectMetadata.getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.ARCHIVED.toString()));
        assertThat(userMetadata.get(SIP_CHECKSUM.getType().toString()), is(SIP_CHECKSUM.getHash()));
        assertThat(userMetadata.get(CephS3StorageService.CREATED_KEY), not(isEmptyOrNullString()));
    }

    /**
     * tests that when rollback flag is set by another thread the method returns and does not finish its job, thus the metadata object still contains {@link ObjectState#PROCESSING} state
     */
    @Test
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

        ObjectMetadata objectMetadata = s3.getObjectMetadata(service.getStorageConfig().getLocation(), service.toMetadataObjectId(fileId));
        Map<String, String> userMetadata = objectMetadata.getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.PROCESSING.toString()));
    }

    /**
     * tests that rollback is set either when storageChecksum of stored file does not match expectation or if there is another error (NPE for example)
     */
    @Test
    public void storeFileSettingRollback() throws Exception {

        String fileId = testName.getMethodName();

        CephS3StorageService service = new TestStorageService(config, "BLZBGL9ZDD23WD0GL8V8", "pPYbINKQxEBLdxhzbycUI00UmTD4uaHjDel1IPui", null);
        AmazonS3 s3 = service.connect();

        AtomicBoolean rollback = new AtomicBoolean(false);

        assertThrown(() -> service.storeFile(s3, fileId, getSipStream(), SIP_CHECKSUM, rollback))
                .isInstanceOf(FileCorruptedAfterStoreException.class);
        assertThat(rollback.get(), is(true));

        rollback.set(false);

        assertThrown(() -> service.storeFile(s3, fileId, getSipStream(), null, rollback))
                .isInstanceOf(GeneralException.class);
        assertThat(rollback.get(), is(true));
    }

    @Test
    public void storeAipOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, xmlId, getXmlStream(), XML_CHECKSUM);
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
    public void storeAipSetsRollback() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto(sipId, getSipStream(), null, toXmlId(sipId, 1), getXmlStream(), null);
        AtomicBoolean rollback = new AtomicBoolean(false);
        assertThrown(() -> service.storeAip(aip, rollback)).isInstanceOf(GeneralException.class);
        assertThat(rollback.get(), is(true));
    }

    @Test
    public void storeXmlOk() throws Exception {
        String sipId = testName.getMethodName();
        AtomicBoolean rollback = new AtomicBoolean(false);
        String xmlId = toXmlId(sipId, 99);
        service.storeObject(new ArchivalObjectDto(xmlId, getXmlStream(), SIP_CHECKSUM), rollback);

        AmazonS3 s3 = service.connect();
        S3Object xmlObj = s3.getObject(bucketName, xmlId);
        S3Object xmlObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(xmlId));

        assertThat(streamToString(xmlObj.getObjectContent()), is(XML_CONTENT));
        assertThat(xmlObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(ObjectState.ARCHIVED.toString()));
        assertThat(rollback.get(), is(false));
    }

    @Test
    public void getAipWithMoreXmlsOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, toXmlId(sipId, 1), getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);
        service.storeObject(new ArchivalObjectDto(toXmlId(sipId, 2), new ByteArrayInputStream("blob".getBytes()), SIP_CHECKSUM), rollback);

        AipRetrievalResource aip1 = service.getAip(sipId, 2, 1);
        assertThat(streamToString(aip1.getSip()), is(SIP_CONTENT));
        assertThat(streamToString(aip1.getXmls().get(2)), is("blob"));
        assertThat(streamToString(aip1.getXmls().get(1)), is(XML_CONTENT));
    }

    @Test
    public void getAipWithSpecificXmlOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, toXmlId(sipId, 1), getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);
        service.storeObject(new ArchivalObjectDto(toXmlId(sipId, 99), new ByteArrayInputStream("blob".getBytes()), SIP_CHECKSUM), rollback);

        AipRetrievalResource aip1 = service.getAip(sipId, 99);
        assertThat(streamToString(aip1.getSip()), is(SIP_CONTENT));
        assertThat(streamToString(aip1.getXmls().get(99)), is("blob"));
    }

    @Test
    public void getAipMissing() throws Exception {
        String sipId = testName.getMethodName();
        assertThrown(() -> service.getAip(sipId)).isInstanceOf(FileDoesNotExistException.class);
    }

    @Test
    public void getAipMissingXml() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, toXmlId(sipId, 1), getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);
        assertThrown(() -> service.getAip(sipId, 2)).isInstanceOf(FileDoesNotExistException.class);
    }

    @Test
    public void getXmlOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, toXmlId(sipId, 1), getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        ObjectRetrievalResource object = service.getObject(toXmlId(sipId, 1));
        assertThat(streamToString(object.getInputStream()), is(XML_CONTENT));
    }

    @Test
    public void getXmlMissing() throws Exception {
        String sipId = testName.getMethodName();
        assertThrown(() -> service.getObject(toXmlId(sipId, 1))).isInstanceOf(FileDoesNotExistException.class);
    }

    @Test
    public void deleteSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, xmlId, getXmlStream(), XML_CHECKSUM);
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
    public void deleteSipMissingMetadata() throws Exception {
        String sipId = testName.getMethodName();
        assertThrown(() -> service.deleteSip(sipId)).isInstanceOf(FileDoesNotExistException.class);
    }

    @Test
    public void removeSipMultipleTimesOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, toXmlId(sipId, 1), getXmlStream(), XML_CHECKSUM);
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
    public void removeSipMissingMetadata() throws Exception {
        String sipId = testName.getMethodName();
        assertThrown(() -> service.remove(sipId)).isInstanceOf(FileDoesNotExistException.class);
    }

    @Test
    public void getAipInfoOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, toXmlId(sipId, 1), getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);
        service.storeObject(new ArchivalObjectDto(toXmlId(sipId, 2), new ByteArrayInputStream("blob".getBytes()), new Checksum(ChecksumType.MD5, "ee26908bf9629eeb4b37dac350f4754a")), rollback);

        Map<Integer, Checksum> map = new HashMap<>();
        map.put(1, XML_CHECKSUM);
        map.put(2, SIP_CHECKSUM);
        AipStateInfoDto aipInfo = service.getAipInfo(sipId, SIP_CHECKSUM, ObjectState.ARCHIVED, map);

        assertThat(aipInfo.getObjectState(), is(ObjectState.ARCHIVED));
        assertThat(aipInfo.getStorageChecksum(), is(SIP_CHECKSUM));
        assertThat(aipInfo.getDatabaseChecksum(), is(SIP_CHECKSUM));
        assertThat(aipInfo.getStorageType(), is(StorageType.CEPH));
        assertThat(aipInfo.getStorageName(), is(config.getName()));
        assertThat(aipInfo.isConsistent(), is(true));

        List<XmlStateInfoDto> xmlsStates = aipInfo.getXmlsState();
        assertThat(xmlsStates, hasSize(2));

        XmlStateInfoDto xmlInfo = xmlsStates.get(0);
        assertThat(xmlInfo.getStorageChecksum(), is(XML_CHECKSUM));
        assertThat(xmlInfo.getDatabaseChecksum(), is(XML_CHECKSUM));
        assertThat(xmlInfo.getVersion(), is(1));
        assertThat(xmlInfo.isConsistent(), is(true));

        XmlStateInfoDto xmlInfo2 = xmlsStates.get(1);
        assertThat(xmlInfo2.getStorageChecksum(), is(new Checksum(ChecksumType.MD5, "ee26908bf9629eeb4b37dac350f4754a")));
        assertThat(xmlInfo2.getDatabaseChecksum(), is(SIP_CHECKSUM));
        assertThat(xmlInfo2.getVersion(), is(2));
        assertThat(xmlInfo2.isConsistent(), is(false));
    }

    @Test
    public void getAipInfoMissingSip() throws Exception {
        String sipId = testName.getMethodName();
        assertThrown(() -> service.getAipInfo(sipId, SIP_CHECKSUM, ObjectState.ARCHIVED, new HashMap<>())).isInstanceOf(FileDoesNotExistException.class);
    }

    @Test
    public void getAipInfoMissingXml() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, toXmlId(sipId, 1), getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        Map<Integer, Checksum> map = new HashMap<>();
        map.put(99, XML_CHECKSUM);
        assertThrown(() -> service.getAipInfo(sipId, SIP_CHECKSUM, ObjectState.ARCHIVED, map)).isInstanceOf(FileDoesNotExistException.class);
    }

    @Test
    public void getAipInfoDeletedSip() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, toXmlId(sipId, 1), getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);
        service.deleteSip(sipId);

        Map<Integer, Checksum> map = new HashMap<>();
        map.put(1, XML_CHECKSUM);
        AipStateInfoDto aipInfo = service.getAipInfo(sipId, SIP_CHECKSUM, ObjectState.DELETED, map);

        assertThat(aipInfo.getObjectState(), is(ObjectState.DELETED));
        assertThat(aipInfo.getStorageChecksum(), nullValue());
        assertThat(aipInfo.getDatabaseChecksum(), is(SIP_CHECKSUM));
        assertThat(aipInfo.isConsistent(), is(false));

        List<XmlStateInfoDto> xmlsStates = aipInfo.getXmlsState();
        assertThat(xmlsStates, hasSize(1));

        XmlStateInfoDto xmlInfo = xmlsStates.get(0);
        assertThat(xmlInfo.getStorageChecksum(), is(XML_CHECKSUM));
        assertThat(xmlInfo.getDatabaseChecksum(), is(XML_CHECKSUM));
        assertThat(xmlInfo.getVersion(), is(1));
        assertThat(xmlInfo.isConsistent(), is(true));
    }

    @Test
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

        ObjectMetadata objectMetadata = s3.getObjectMetadata(service.getStorageConfig().getLocation(), service.toMetadataObjectId(fileId));
        Map<String, String> userMetadata = objectMetadata.getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.PROCESSING.toString()));
//actual test
        service.rollbackFile(s3, fileId);

        assertThrown(() -> s3.getObject(bucketName, fileId)).isInstanceOf(AmazonS3Exception.class).messageContains("NoSuchKey");
        userMetadata = s3.getObjectMetadata(bucketName, service.toMetadataObjectId(fileId)).getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.ROLLED_BACK.toString()));
    }

    @Test
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
    public void rollbackCompletlyMissingFile() throws Exception {
        String fileId = testName.getMethodName();
        AmazonS3 s3 = service.connect();
        service.rollbackFile(s3, fileId);
        Map<String, String> userMetadata = s3.getObjectMetadata(bucketName, service.toMetadataObjectId(fileId)).getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(ObjectState.ROLLED_BACK.toString()));
    }

    @Test
    public void rollbackAipOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, xmlId, getXmlStream(), XML_CHECKSUM);
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
    public void rollbackXmlOk() throws Exception {
        String sipId = testName.getMethodName();
        String xmlId = toXmlId(sipId, 1);
        AipDto aip = new AipDto(sipId, getSipStream(), SIP_CHECKSUM, xmlId, getXmlStream(), XML_CHECKSUM);
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
    public void testConnection() {
        CephS3StorageService badService = new TestStorageService(config, "blah", "blah", "blah");
        assertThat(badService.testConnection(), is(false));
        assertThat(service.testConnection(), is(true));
        AmazonS3 s3 = service.connect();
        Owner s3AccountOwner = s3.getS3AccountOwner();
        assertThat(s3AccountOwner.getId(), is("arcephUser"));
    }


// use this to easily create test bucket if it does not exist yet
//    @Test
//    public void createBucket(){
//        AmazonS3 s3 = service.connect();
//        s3.createBucket(config.getLocation());
//    }

    private static final class TestStorageService extends CephS3StorageService {
        public TestStorageService(StorageConfig storageConfig, String userAccessKey, String userSecretKey, String region) {
            super(storageConfig, userAccessKey, userSecretKey, region);
        }

        @Override
        public Checksum computeChecksumRollbackAware(InputStream fileStream, ChecksumType checksumType, AtomicBoolean rollback) throws IOException {
            return new Checksum(ChecksumType.MD5, "alwayswrong");
        }
    }

    private InputStream getSipStream() {
        return new ByteArrayInputStream(SIP_CONTENT.getBytes());
    }

    private InputStream getXmlStream() {
        return new ByteArrayInputStream(XML_CONTENT.getBytes());
    }

    private String streamToString(InputStream is) {
        try {
            return IOUtils.toString(is, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
