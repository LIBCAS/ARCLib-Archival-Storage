package cz.cas.lib.arcstorage.storage.ceph;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3Object;
import cz.cas.lib.arcstorage.domain.AipState;
import cz.cas.lib.arcstorage.domain.ChecksumType;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.domain.XmlState;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.storage.StorageUtils;
import cz.cas.lib.arcstorage.storage.exception.FileCorruptedAfterStoreException;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static helper.ThrowableAssertion.assertThrown;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class CephS3Test {

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
        bucketName = config.getLocation();
    }

    /**
     * tests that file larger than 8MiB (split into two parts) is successfully stored together with metadata
     */
    @Test
    public void storeLargeFileSuccessTest() throws IOStorageException, IOException, FileCorruptedAfterStoreException {

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
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(AipState.ARCHIVED.toString()));
        assertThat(userMetadata.get(checksum.getType().toString()), is(checksum.getHash()));
        assertThat(userMetadata.get(CephS3StorageService.CREATED_KEY), not(isEmptyOrNullString()));
        assertThat(userMetadata.get(service.UPLOAD_ID), not(isEmptyOrNullString()));

        //part listing does not work
        //PartListing partListing = s3.listParts(new ListPartsRequest(config.getLocation(),fileId,userMetadata.get(service.UPLOAD_ID)));
        //assertThat(partListing.getParts(), hasSize(1));
    }

    /**
     * tests that small file is successfully stored together with metadata
     */
    @Test
    public void storeSmallFileSuccessTest() throws IOStorageException, IOException, FileCorruptedAfterStoreException {

        String fileId = testName.getMethodName();
        AmazonS3 s3 = service.connect();

        service.storeFile(s3, fileId, getSipStream(), SIP_CHECKSUM, new AtomicBoolean(false));

        S3Object object = s3.getObject(service.getStorageConfig().getLocation(), fileId);
        Checksum checksumOfStoredFile = StorageUtils.computeChecksum(object.getObjectContent(), ChecksumType.MD5);
        assertThat(SIP_CHECKSUM, is(checksumOfStoredFile));

        ObjectMetadata objectMetadata = s3.getObjectMetadata(service.getStorageConfig().getLocation(), service.toMetadataObjectId(fileId));
        Map<String, String> userMetadata = objectMetadata.getUserMetadata();
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(AipState.ARCHIVED.toString()));
        assertThat(userMetadata.get(SIP_CHECKSUM.getType().toString()), is(SIP_CHECKSUM.getHash()));
        assertThat(userMetadata.get(CephS3StorageService.CREATED_KEY), not(isEmptyOrNullString()));
        assertThat(userMetadata.get(service.UPLOAD_ID), not(isEmptyOrNullString()));
    }

    /**
     * tests that when rollback flag is set by another thread the method returns and does not finish its job, thus the metadata object still contains {@link AipState#PROCESSING} state
     */
    @Test
    public void storeFileRollbackAware() throws IOStorageException, IOException, FileCorruptedAfterStoreException {

        String fileId = testName.getMethodName();
        AmazonS3 s3 = service.connect();

        File file = new File("src/test/resources/8MiB+file");
        AtomicBoolean rollback = new AtomicBoolean(false);

        new Thread(() -> {
            try {
                Thread.sleep(3000);
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
        assertThat(userMetadata.get(CephS3StorageService.STATE_KEY), is(AipState.PROCESSING.toString()));
        assertThat(userMetadata.get(service.UPLOAD_ID), not(isEmptyOrNullString()));
    }

    /**
     * tests that rollback is set either when checksum of stored file does not match expectation or if there is another error (NPE for example)
     */
    @Test
    public void storeFileSettingRollback() throws IOStorageException, IOException, FileCorruptedAfterStoreException {

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
    public void storeAipOk() throws StorageException {
        String sipId = testName.getMethodName();
        String xmlId = service.toXmlId(sipId, 1);
        AipRef aip = new AipRef(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        AmazonS3 s3 = service.connect();
        S3Object sipObj = s3.getObject(bucketName, sipId);
        S3Object sipObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(sipId));
        S3Object xmlObj = s3.getObject(bucketName, xmlId);
        S3Object xmlObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(xmlId));

        assertThat(streamToString(sipObj.getObjectContent()), is(SIP_CONTENT));
        assertThat(sipObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(AipState.ARCHIVED.toString()));
        assertThat(streamToString(xmlObj.getObjectContent()), is(XML_CONTENT));
        assertThat(xmlObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(XmlState.ARCHIVED.toString()));
        assertThat(rollback.get(), is(false));

    }

    @Test
    public void storeAipSetsRollback() throws StorageException {
        String sipId = testName.getMethodName();
        AipRef aip = new AipRef(sipId, getSipStream(), null, getXmlStream(), null);
        AtomicBoolean rollback = new AtomicBoolean(false);
        assertThrown(() -> service.storeAip(aip, rollback)).isInstanceOf(GeneralException.class);
        assertThat(rollback.get(), is(true));
    }

    @Test
    public void storeXmlOk() throws StorageException {
        String sipId = testName.getMethodName();
        AtomicBoolean rollback = new AtomicBoolean(false);
        String xmlId = service.toXmlId(sipId, 99);
        service.storeXml(sipId, new XmlRef(new FileRef(getXmlStream()), SIP_CHECKSUM, 99), rollback);

        AmazonS3 s3 = service.connect();
        S3Object xmlObj = s3.getObject(bucketName, xmlId);
        S3Object xmlObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(xmlId));

        assertThat(streamToString(xmlObj.getObjectContent()), is(XML_CONTENT));
        assertThat(xmlObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(XmlState.ARCHIVED.toString()));
        assertThat(rollback.get(), is(false));
    }

    @Test
    public void getAipWithMoreXmlsOk() throws StorageException {
        String sipId = testName.getMethodName();
        AipRef aip = new AipRef(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);
        service.storeXml(sipId, new XmlRef(new FileRef(new ByteArrayInputStream("blob".getBytes())), SIP_CHECKSUM, 2), rollback);

        List<FileRef> files = service.getAip(sipId, 2, 1);
        assertThat(streamToString(files.get(0).getInputStream()), is(SIP_CONTENT));
        assertThat(streamToString(files.get(1).getInputStream()), is("blob"));
        assertThat(streamToString(files.get(2).getInputStream()), is(XML_CONTENT));
    }

    @Test
    public void getAipWithSpecificXmlOk() throws StorageException {
        String sipId = testName.getMethodName();
        AipRef aip = new AipRef(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);
        service.storeXml(sipId, new XmlRef(new FileRef(new ByteArrayInputStream("blob".getBytes())), SIP_CHECKSUM, 99), rollback);

        List<FileRef> files = service.getAip(sipId, 99);
        assertThat(streamToString(files.get(0).getInputStream()), is(SIP_CONTENT));
        assertThat(streamToString(files.get(1).getInputStream()), is("blob"));
    }

    @Test
    public void getXmlOk() throws StorageException {
        String sipId = testName.getMethodName();
        AipRef aip = new AipRef(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        FileRef file = service.getXml(sipId, 1);
        assertThat(streamToString(file.getInputStream()), is(XML_CONTENT));
    }

    @Test
    public void deleteSipOk() throws StorageException {
        String sipId = testName.getMethodName();
        String xmlId = service.toXmlId(sipId, 1);
        AipRef aip = new AipRef(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.deleteSip(sipId);

        AmazonS3 s3 = service.connect();
        assertThrown(() -> s3.getObject(bucketName, sipId)).isInstanceOf(AmazonS3Exception.class).messageContains("NoSuchKey");
        assertThrown(() -> s3.getObject(bucketName, service.toMetadataObjectId(sipId))).isInstanceOf(AmazonS3Exception.class).messageContains("NoSuchKey");

        S3Object xmlObj = s3.getObject(bucketName, xmlId);
        S3Object xmlObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(xmlId));

        assertThat(streamToString(xmlObj.getObjectContent()), is(XML_CONTENT));
        assertThat(xmlObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(XmlState.ARCHIVED.toString()));
    }

    /**
     * tests that the method does not fail if sip is already deleted
     *
     * @throws StorageException
     */
    @Test
    public void deleteSipMissing() throws StorageException {
        String sipId = testName.getMethodName();
        service.deleteSip(sipId);
    }

    @Test
    public void removeSipOk() throws StorageException {
        String sipId = testName.getMethodName();
        AipRef aip = new AipRef(sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        service.storeAip(aip, rollback);

        service.remove(sipId);

        AmazonS3 s3 = service.connect();
        S3Object sipObj = s3.getObject(bucketName, sipId);
        S3Object sipObjMeta = s3.getObject(bucketName, service.toMetadataObjectId(sipId));
        assertThat(streamToString(sipObj.getObjectContent()), is(SIP_CONTENT));
        assertThat(sipObjMeta.getObjectMetadata().getUserMetadata().get(service.STATE_KEY), is(AipState.REMOVED.toString()));
    }

    /**
     * tests that connection to test cluster can be established and test user exists
     */
    @Test
    public void connectTest() {
        AmazonS3 s3 = service.connect();
        Owner s3AccountOwner = s3.getS3AccountOwner();
        assertThat(s3AccountOwner.getId(), is("arcephUser"));
    }

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
