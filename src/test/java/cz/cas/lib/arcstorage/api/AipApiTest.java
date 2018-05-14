package cz.cas.lib.arcstorage.api;

import com.querydsl.jpa.impl.JPAQueryFactory;
import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.store.AipSipStore;
import cz.cas.lib.arcstorage.domain.store.AipXmlStore;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.service.StorageProvider;
import cz.cas.lib.arcstorage.storage.ceph.CephS3StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.storage.fs.FsStorageService;
import cz.cas.lib.arcstorage.storage.fs.ZfsStorageService;
import helper.ApiTest;
import helper.DbTest;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import javax.inject.Inject;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static cz.cas.lib.arcstorage.util.Utils.asList;
import static cz.cas.lib.arcstorage.util.Utils.asMap;
import static java.lang.String.valueOf;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * If null pointer exception is thrown inside test the cause can be that async method invocation of previous test has
 * not finished during that test. {@link Thread#sleep(long)} is used to overcome this.
 * Most of POST and DELETE requests run asynchronously.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class AipApiTest extends DbTest implements ApiTest {

    private static Path tmpFolder;

    @Inject
    private AipApi api;

    @Inject
    private AipSipStore sipStore;
    @Inject
    private AipXmlStore xmlStore;
    @Inject
    private StorageStore storageStore;

    @MockBean
    private FsStorageService fsStorageService;

    @MockBean
    private ZfsStorageService zfsStorageService;

    @MockBean
    private CephS3StorageService cephS3StorageService;

    @MockBean
    private StorageProvider storageProvider;

    private static final String SIP_ID = "8f719ff7-8756-4101-9e87-42391ced37f1";
    private static final String SIP_HASH = "bc196bfdd827cf371a2ccca02be989ce";

    private static final String XML1_ID = "XML1testID";
    private static final String XML2_ID = "XML2testID";

    private static final String XML1_HASH = "F09E5F27526A0ED7EC5B2D9D5C0B53CF";
    private static final String XML2_HASH = "D5B6402517014CF00C223D6A785A4230";

    private static final String BASE = "/api/storage";
    private static final Path SIP_SOURCE_PATH = Paths.get("src/test/resources", "KPW01169310.ZIP");

    private static AipSip sip;
    private static AipXml aipXml1;
    private static AipXml aipXml2;
    private static Storage s1;
    private static Storage s2;
    private static Storage s3;

    @BeforeClass
    public static void setup() throws IOException {

        s1 = new Storage();
        s1.setHost("localhost");
        s1.setName("local fs");
        s1.setPriority(1);
        s1.setStorageType(StorageType.FS);
        s1.setLocation("localFsFolder");
        s1.setReachable(true);

        s2 = new Storage();
        s2.setHost("192.168.0.60");
        s2.setName("remote zfs");
        s2.setPort(22);
        s2.setPriority(1);
        s2.setStorageType(StorageType.ZFS);
        s2.setLocation("/arcpool/arcfs");
        s2.setReachable(true);
        s2.setConfig("{\"pool\":\"\", \"dataset\":\"\"}");

        s3 = new Storage();
        s3.setHost("192.168.10.60");
        s3.setName("ceph s3");
        s3.setPort(7480);
        s3.setPriority(1);
        s3.setStorageType(StorageType.CEPH);
        s3.setLocation("arclib.bucket1");
        s3.setConfig("{\"adapterType\":\"S3\",\"userKey\":\"BLZBGL9ZDD23WD0GL8V8\",\"userSecret\":\"pPYbINKQxEBLdxhzbycUI00UmTD4uaHjDel1IPui\"}");
        s3.setReachable(true);
    }

    @AfterClass
    public static void tearDown() throws IOException {
        FileUtils.cleanDirectory(tmpFolder.toFile());
    }

    @Before
    public void before() throws StorageException, FileNotFoundException {

        FileInputStream sipContent = new FileInputStream(SIP_SOURCE_PATH.toFile());
        FileInputStream xml1InputStream = new FileInputStream(Paths.get("./src/test/resources/aip/xml1.xml").toFile());
        FileInputStream xml2InputStream = new FileInputStream(Paths.get("./src/test/resources/aip/xml2.xml").toFile());

        sip = new AipSip(SIP_ID, new Checksum(ChecksumType.MD5, SIP_HASH), ObjectState.ARCHIVED);

        aipXml1 = new AipXml(XML1_ID, new Checksum(ChecksumType.MD5, XML1_HASH), sip, 1, ObjectState.ARCHIVED);

        aipXml2 = new AipXml(XML2_ID, new Checksum(ChecksumType.MD5, XML2_HASH), sip, 2, ObjectState.ARCHIVED);

        xmlStore.setEntityManager(getEm());
        xmlStore.setQueryFactory(new JPAQueryFactory(getEm()));

        sipStore.setEntityManager(getEm());
        sipStore.setQueryFactory(new JPAQueryFactory(getEm()));

        sipStore.save(sip);

        xmlStore.save(aipXml1);
        xmlStore.save(aipXml2);

        storageStore.save(s2);
        storageStore.save(s1);
        storageStore.save(s3);

        when(fsStorageService.testConnection()).thenReturn(true);
        when(zfsStorageService.testConnection()).thenReturn(true);
        when(cephS3StorageService.testConnection()).thenReturn(true);

        when(fsStorageService.getStorage()).thenReturn(s1);
        when(zfsStorageService.getStorage()).thenReturn(s2);
        when(cephS3StorageService.getStorage()).thenReturn(s3);

        StorageStateDto storageStateDto = new StorageStateDto(null, asMap("used", "1234",
                "available", "2345"));
        when(fsStorageService.getStorageState()).thenReturn(storageStateDto);
        when(zfsStorageService.getStorageState()).thenReturn(storageStateDto);
        when(cephS3StorageService.getStorageState()).thenReturn(storageStateDto);

        AipStateInfoDto aipStateInfoFsDto = new AipStateInfoDto(fsStorageService.getStorage().getName(),
                StorageType.FS, sip.getState(), sip.getChecksum());
        when(fsStorageService.getAipInfo(anyString(), anyObject(), anyObject(), anyObject()))
                .thenReturn(aipStateInfoFsDto);

        AipStateInfoDto aipStateInfoZfsDto = new AipStateInfoDto(fsStorageService.getStorage().getName(),
                StorageType.ZFS, sip.getState(), sip.getChecksum());
        when(zfsStorageService.getAipInfo(anyString(), anyObject(), anyObject(), anyObject()))
                .thenReturn(aipStateInfoZfsDto);

        AipStateInfoDto aipStateInfoCephDto = new AipStateInfoDto(fsStorageService.getStorage().getName(),
                StorageType.CEPH, sip.getState(), sip.getChecksum());
        when(cephS3StorageService.getAipInfo(anyString(), anyObject(), anyObject(), anyObject()))
                .thenReturn(aipStateInfoCephDto);

        when(storageProvider.createAllAdapters()).thenReturn(asList(fsStorageService, zfsStorageService, cephS3StorageService));

        AipRetrievalResource aip1 = new AipRetrievalResource(sipContent.getChannel());
        aip1.setSip(sipContent);
        aip1.addXml(1, xml1InputStream);
        aip1.addXml(2, xml2InputStream);
        when(fsStorageService.getAip(SIP_ID, 1, 2)).thenReturn(aip1);
        when(zfsStorageService.getAip(SIP_ID, 1, 2)).thenReturn(aip1);
        when(cephS3StorageService.getAip(SIP_ID, 1, 2)).thenReturn(aip1);

        AipRetrievalResource aip2 = new AipRetrievalResource(sipContent.getChannel());
        aip2.setSip(sipContent);
        aip2.addXml(2, xml2InputStream);
        when(fsStorageService.getAip(SIP_ID, 2)).thenReturn(aip2);
        when(zfsStorageService.getAip(SIP_ID, 2)).thenReturn(aip2);
        when(cephS3StorageService.getAip(SIP_ID, 2)).thenReturn(aip2);

        String xml1Id = toXmlId(SIP_ID, 1);
        ObjectRetrievalResource xml1res = new ObjectRetrievalResource(xml1InputStream, xml1InputStream.getChannel());
        String xmll2Id = toXmlId(SIP_ID, 2);
        ObjectRetrievalResource xml2res = new ObjectRetrievalResource(xml2InputStream, xml2InputStream.getChannel());

        when(fsStorageService.getObject(xml1Id)).thenReturn(xml1res);
        when(zfsStorageService.getObject(xml1Id)).thenReturn(xml1res);
        when(cephS3StorageService.getObject(xml1Id)).thenReturn(xml1res);

        when(fsStorageService.getObject(xmll2Id)).thenReturn(xml2res);
        when(zfsStorageService.getObject(xmll2Id)).thenReturn(xml2res);
        when(cephS3StorageService.getObject(xmll2Id)).thenReturn(xml2res);
    }

    /**
     * Send request for AIP data and verifies that ZIP file containing one ZIP (SIP) and latest AIP XML is retrieved.
     *
     * @throws Exception
     */
    @Test
    public void getSipAndLatestXml() throws Exception {
        byte[] zip = mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/{sipId}", SIP_ID))
                .andExpect(header().string("Content-Type", "application/zip"))
                .andExpect(header().string("Content-Disposition",
                        "attachment; filename=aip_" + SIP_ID))
                .andExpect(status().isOk())
                .andReturn().getResponse()
                .getContentAsByteArray();
        List<String> packedFiles = new ArrayList<>();
        try (ZipInputStream zipStream = new ZipInputStream(new BufferedInputStream(new ByteArrayInputStream(zip)))) {
            ZipEntry entry;
            while ((entry = zipStream.getNextEntry()) != null) {
                packedFiles.add(entry.getName());
            }
        }
        assertThat(packedFiles, containsInAnyOrder(SIP_ID, toXmlId(SIP_ID, 2)));
    }

    /**
     * Send request for AIP data and verifies that ZIP file containing one ZIP (SIP) and all AIP XMLs are retrieved.
     *
     * @throws Exception
     */
    @Test
    public void getSipAndAllXmls() throws Exception {
        int tmpFilesBeforeRetrieval = tmpFolder.toFile().listFiles().length;
        byte[] zip = mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/{sipId}", SIP_ID).param("all", "true"))
                .andExpect(header().string("Content-Type", "application/zip"))
                .andExpect(header().string("Content-Disposition",
                        "attachment; filename=aip_" + SIP_ID))
                .andExpect(status().isOk())
                .andReturn().getResponse()
                .getContentAsByteArray();
        List<String> packedFiles = new ArrayList<>();
        try (ZipInputStream zipStream = new ZipInputStream(new BufferedInputStream(new ByteArrayInputStream(zip)))) {
            ZipEntry entry;
            while ((entry = zipStream.getNextEntry()) != null) {
                packedFiles.add(entry.getName());
            }
        }
        int tmpFilesAfterRetrieval = tmpFolder.toFile().listFiles().length;
        assertThat(tmpFilesAfterRetrieval, is(tmpFilesBeforeRetrieval));
        assertThat(packedFiles, containsInAnyOrder(SIP_ID, toXmlId(SIP_ID, 1), toXmlId(SIP_ID, 2)));
    }

    /**
     * Send request for latest xml and verifies data in response.
     *
     * @throws Exception
     */
    @Test
    public void getLatestXml() throws Exception {
        byte[] xmlContent = mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/xml/{sipId}", SIP_ID))
                .andExpect(status().isOk())
                .andReturn().getResponse()
                .getContentAsByteArray();
        assertThat(xmlContent, equalTo(XML2_ID.getBytes()));
    }

    /**
     * Send AIP creation request with AIP data (sip & xml) and verifies that response contains ID of newly created AIP (SIP).
     * Then send state request and verifies that SIP exists.
     *
     * @throws Exception
     */
    @Test
    public void saveIdProvided() throws Exception {
        String sipId = "testSipId";
        String xmlId = "testXmlId";

        MockMultipartFile sipFile = new MockMultipartFile(
                "sip", "sip", "text/plain", Files.readAllBytes(SIP_SOURCE_PATH));
        MockMultipartFile xmlFile = new MockMultipartFile(
                "aipXml", "xml", "text/plain", xmlId.getBytes());

        String xmlHash = "af5e897c3cc424f31b84af579b274626";
        int tmpFilesBeforeRetrieval = tmpFolder.toFile().listFiles().length;
        mvc(api)
                .perform(MockMvcRequestBuilders
                        .fileUpload(BASE + "/save").file(sipFile).file(xmlFile)
                        .param("sipChecksumValue", SIP_HASH)
                        .param("sipChecksumType", valueOf(ChecksumType.MD5))
                        .param("aipXmlChecksumValue", xmlHash)
                        .param("aipXmlChecksumType", valueOf(ChecksumType.MD5))
                        .param("UUID", sipId))
                .andExpect(status().isOk());
        assertThat(sipId, not(isEmptyOrNullString()));
        Thread.sleep(5000);

        AipSip aipSip = sipStore.find(sipId);
        assertThat(aipSip.getState(), is(ObjectState.ARCHIVED));
        assertThat(aipSip.getXmls().size(), is(1));
        assertThat(aipSip.getXml(0).getState(), is(ObjectState.ARCHIVED));
        int tmpFilesAfterRetrieval = tmpFolder.toFile().listFiles().length;
        assertThat(tmpFilesAfterRetrieval, is(tmpFilesBeforeRetrieval));
    }

    /**
     * Send AIP creation request with AIP data where MD5 param does not match MD5 of a file.
     * Then send request for AIP state and verifies that both XML and SIP are ROLLED BACK.
     * Then send request for AIP data and verifies 500 response code.
     *
     * @throws Exception
     */
    @Test
    @Ignore
    public void saveMD5Changed() throws Exception {
        MockMultipartFile sipFile = new MockMultipartFile(
                "sip", "sip", "text/plain", Files.readAllBytes(SIP_SOURCE_PATH));
        MockMultipartFile xmlFile = new MockMultipartFile(
                "aipXml", "xml", "text/plain", XML1_ID.getBytes());

        String sipId = mvc(api)
                .perform(MockMvcRequestBuilders
                        .fileUpload(BASE + "/save").file(sipFile).file(xmlFile)
                        .param("sipChecksumValue", XML1_HASH)
                        .param("sipChecksumType", valueOf(ChecksumType.MD5))
                        .param("aipXmlChecksumValue", XML1_HASH)
                        .param("aipXmlChecksumType", valueOf(ChecksumType.MD5)))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertThat(sipId, not(isEmptyOrNullString()));
        Thread.sleep(6000);

//        AipSip aipSip = sipStore.find(sipId);
//        assertThat(aipSip.getState(), is(ObjectState.ROLLED_BACK));
//        assertThat(aipSip.isConsistent(), is(true));
//
//        assertThat(aipSip.getXml(0).getState(), is(ObjectState.ROLLED_BACK));
//        assertThat(aipSip.getXml(0).isConsistent(), is(true));
//
//        mvc(api)
//                .perform(MockMvcRequestBuilders.get(BASE + "/{sipId}", sipId))
//                .andExpect(status().is(500));
    }

    /**
     * Send request for xml specified by version and verifies data in response.
     *
     * @throws Exception
     */
    @Test
    public void getXmlByVersion() throws Exception {
        byte[] xmlContent = mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/xml/{sipId}", SIP_ID).param("v", "1"))
                .andExpect(status().isOk())
                .andReturn().getResponse()
                .getContentAsByteArray();
        assertThat(xmlContent, equalTo(XML1_ID.getBytes()));
    }

    /**
     * Send request for XML update.
     * Then sends request for AIP state and verifies new XML record is there.
     *
     * @throws Exception
     */
    @Test
    public void updateXml() throws Exception {
        AipSip aipSip = sipStore.find(SIP_ID);
        int countOfXmlVersions = aipSip.getXmls().size();

        MockMultipartFile xmlFile = new MockMultipartFile(
                "xml", "xml", "text/plain", XML2_ID.getBytes());

        mvc(api)
                .perform(MockMvcRequestBuilders.fileUpload(BASE + "/{sipId}/update", SIP_ID).file(xmlFile)
                        .param("checksumType", ChecksumType.MD5.toString())
                        .param("checksumValue", XML2_HASH)
                        .contentType(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk());
        Thread.sleep(2000);

        aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getXmls().size(), is(countOfXmlVersions + 1));
        assertThat(aipSip.getLatestXml().getState(), is(ObjectState.ARCHIVED));
    }

    /**
     * Send request for XML update where MD5 param does not match MD5 of a file.
     * Then send request for AIP state and verifies that only last XML is ROLLED_BACK.
     * Then send request for XML data and verifies 500 response code.
     *
     * @throws Exception
     */
    @Test
    @Ignore
    public void updateXmlMD5Changed() throws Exception {
        MockMultipartFile xmlFile = new MockMultipartFile(
                "xml", "xml", "text/plain", XML2_ID.getBytes());
        mvc(api)
                .perform(MockMvcRequestBuilders.fileUpload(BASE + "/{sipId}/update", SIP_ID).file(xmlFile)
                        .param("checksumType", ChecksumType.MD5.toString())
                        .param("checksumValue", SIP_HASH)
                        .contentType(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk());
        Thread.sleep(4000);

        AipSip aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getXml(0).getState(), is(ObjectState.ARCHIVED));
        assertThat(aipSip.getXml(2).getState(), is(ObjectState.ROLLED_BACK));

        mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/xml/{sipId}", SIP_ID))
                .andExpect(status().is(500));
    }

    @Test
    public void getDuringXmlUpdateWhichFailsInTheEnd() throws Exception {
        doAnswer(invocation -> {
            Thread.sleep(500);
            throw new IllegalStateException("whatever exception");
        }).when(cephS3StorageService).storeObject(anyObject(), anyObject());
        when(storageProvider.createReachableAdapters()).thenReturn(asList(cephS3StorageService));

        AipSip aipSip = sipStore.find(SIP_ID);
        int countOfXmlVersions = aipSip.getXmls().size();

        MockMultipartFile xmlFile = new MockMultipartFile(
                "xml", "xml", "text/plain", XML2_ID.getBytes());
        mvc(api)
                .perform(MockMvcRequestBuilders.fileUpload(BASE + "/{sipId}/update", SIP_ID).file(xmlFile)
                        .param("checksumType", ChecksumType.MD5.toString())
                        .param("checksumValue", XML2_HASH)
                        .contentType(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk());
        aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getXmls().size(), is(countOfXmlVersions + 1));
        assertThat(aipSip.getLatestXml().getState(), is(ObjectState.PROCESSING));
        Thread.sleep(1000);
        aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getLatestXml().getState(), is(ObjectState.ROLLED_BACK));
    }

    /**
     * Sends AIP removeAip (soft deleteAip) request then sends AIP state request and verifies state change.
     *
     * @throws Exception
     */
    @Test
    public void remove() throws Exception {
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{sipId}", SIP_ID))
                .andExpect(status().isOk());
        Thread.sleep(2000);
        AipSip aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getState(), is(ObjectState.REMOVED));
    }

    @Test
    public void delete() throws Exception {
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{sipId}/hard", SIP_ID))
                .andExpect(status().isOk());
        Thread.sleep(2000);
        AipSip aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getState(), is(ObjectState.DELETED));
    }

    /**
     * Send hard deleteAip request on created AIP verifies its status code.
     * Also verifies that two XMLs of SIP does not have the same version number.
     * At the end send request for AIP data and verifies that 404 error status is retrieved.
     *
     * @throws Exception
     */
    @Test
    public void getAfterDelete() throws Exception {
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{sipId}/hard", SIP_ID))
                .andExpect(status().isOk());
        Thread.sleep(2000);

        AipSip aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getState(), is(ObjectState.DELETED));

        mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/{sipId}", SIP_ID)).andExpect(status().is(403));
    }

    @Test
    public void aipState() throws Exception {
        mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/{sipId}/state", SIP_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0].objectState").value("ARCHIVED"))
                .andExpect(jsonPath("$..storageType", containsInAnyOrder("FS", "ZFS", "CEPH")));
    }

    /**
     * Send request for storage state and verifies response contains number of used and available bytes.
     *
     * @throws Exception
     */
    @Test
    public void getStorageState() throws Exception {
        mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/state"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0].storageStateData.available").value("2345"))
                .andExpect(jsonPath("$.[0].storageStateData.used").value("1234"));
    }

    /**
     * Send request with invalid MD5 and verifies that response contains BAD_REQUEST error status.
     *
     * @throws Exception
     */
    @Test
    public void badFormatMD5() throws Exception {
        MockMultipartFile xmlFile = new MockMultipartFile(
                "xml", "xml", "text/plain", XML2_ID.getBytes());
        mvc(api)
                .perform(MockMvcRequestBuilders.fileUpload(BASE + "/{sipId}/update", SIP_ID).file(xmlFile)
                        .param("checksumType", ChecksumType.MD5.toString())
                        .param("checksumValue", "invalidhash")
                        .contentType(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().is(400));
    }

    /**
     * Send request with invalid UUID and verifies that response contains BAD_REQUEST error status.
     *
     * @throws Exception
     */
    @Test
    public void badFormatID() throws Exception {
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{sipId}/hard", "invalidid"))
                .andExpect(status().is(400));
    }

    private static String toXmlId(String sipId, int version) {
        return String.format("%s_xml_%d", sipId, version);
    }

    @Inject
    public void setTmpFolder(@Value("${arcstorage.tmp-folder}") String path) {
        this.tmpFolder = Paths.get(path);
    }
}
