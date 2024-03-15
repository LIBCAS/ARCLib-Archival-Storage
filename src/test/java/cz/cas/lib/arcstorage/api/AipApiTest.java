package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.Initializer;
import cz.cas.lib.arcstorage.domain.entity.*;
import cz.cas.lib.arcstorage.domain.store.AipSipStore;
import cz.cas.lib.arcstorage.domain.store.AipXmlStore;
import cz.cas.lib.arcstorage.domain.store.SystemStateStore;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.security.Role;
import cz.cas.lib.arcstorage.security.user.UserStore;
import cz.cas.lib.arcstorage.service.ArchivalDbService;
import cz.cas.lib.arcstorage.service.StorageProvider;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.ceph.CephS3StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.storage.fs.FsStorageService;
import cz.cas.lib.arcstorage.storage.fs.ZfsStorageService;
import cz.cas.lib.arcstorage.storagesync.AuditedOperation;
import cz.cas.lib.arcstorage.storagesync.ObjectAudit;
import cz.cas.lib.arcstorage.storagesync.ObjectAuditStore;
import helper.ApiTest;
import helper.auth.WithMockCustomUser;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.transaction.support.TransactionTemplate;

import javax.inject.Inject;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static cz.cas.lib.arcstorage.util.Utils.*;
import static java.lang.String.valueOf;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * If null pointer exception is thrown inside test the cause can be that async method invocation of previous test has
 * not finished during that test. {@link Thread#sleep(long)} is used to overcome this.
 * Most of POST and DELETE requests run asynchronously.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Initializer.class)
@WithMockCustomUser(id = "1fa68a7e-eb66-44fd-b492-d4d55e1a95d5")
@Transactional
public class AipApiTest implements ApiTest {

    private Path tmpFolder;

    @Inject
    private ObjectReadApi api;

    @Inject
    private AipSipStore sipStore;
    @Inject
    private AipXmlStore xmlStore;

    @MockBean
    private FsStorageService fsStorageService;

    @MockBean
    private ZfsStorageService zfsStorageService;

    @MockBean
    private CephS3StorageService cephS3StorageService;

    @MockBean
    private StorageProvider storageProvider;

    @Inject
    private SystemStateStore systemStateStore;
    @Inject
    private ObjectAuditStore objectAuditStore;
    @Inject
    private ArchivalDbService archivalDbService;
    @Inject
    private TransactionTemplate transactionTemplate;

    @Inject
    private UserStore userStore;

    private static final String SIP_ID = "8f719ff7-8756-4101-9e87-42391ced37f1";
    private static final String SIP_HASH = "bc196bfdd827cf371a2ccca02be989ce";

    private static final String XML1_ID = "d16d05b6-ff4f-44e7-b029-572e2a03fe84";
    private static final String XML1_CONTENT = "XML1testID";
    private static final String XML2_ID = "27df45cc-4f93-4ff7-9898-43b51bd50fd5";
    private static final String XML2_CONTENT = "XML2testID";

    private static final String XML1_HASH = "F09E5F27526A0ED7EC5B2D9D5C0B53CF";
    private static final String XML2_HASH = "D5B6402517014CF00C223D6A785A4230";

    private static final String BASE = "/api/storage";
    private static final Path SIP_SOURCE_PATH = Paths.get("src/test/resources", "KPW01169310.ZIP");
    private static final String USER_ID = "1fa68a7e-eb66-44fd-b492-d4d55e1a95d5";
    private static final String DATA_SPACE = "dataspace";
    private static AipSip sip;
    private static AipXml aipXml1;
    private static AipXml aipXml2;
    private static Storage s1;
    private static Storage s2;
    private static Storage s3;

    @BeforeClass
    public static void setup() {
        s1 = new Storage();
        s1.setHost("localhost");
        s1.setName("local fs");
        s1.setPriority(1);
        s1.setStorageType(StorageType.FS);
        s1.setReachable(true);
        s1.setConfig("{\"rootDirPath\":\"localFsFolder\"}");


        s2 = new Storage();
        s2.setHost("192.168.0.60");
        s2.setName("remote zfs");
        s2.setPort(22);
        s2.setPriority(1);
        s2.setStorageType(StorageType.ZFS);
        s2.setReachable(true);
        s2.setConfig("{\"rootDirPath\":\"/arcpool/arcfs\"}");

        s3 = new Storage();
        s3.setHost("192.168.10.60");
        s3.setName("ceph s3");
        s3.setPort(7480);
        s3.setPriority(1);
        s3.setStorageType(StorageType.CEPH);
        s3.setConfig("{\"adapterType\":\"S3\",\"userKey\":\"somekey\",\"userSecret\":\"somesecret\"}");
        s3.setReachable(true);
    }


    @After
    public void after() throws IOException, SQLException {
        FileUtils.cleanDirectory(tmpFolder.toFile());
    }

    @Before
    public void before() throws StorageException, IOException, NoLogicalStorageAttachedException, NoLogicalStorageReachableException {
        Files.createDirectories(tmpFolder);
        archivalDbService.setTransactionTemplateTimeout(5);
        transactionTemplate.execute(s -> {
            for (AipXml aipXml : xmlStore.findAll()) {
                xmlStore.delete(aipXml);
            }
            return null;
        });
        transactionTemplate.execute(s -> {
            for (AipSip aipSip : sipStore.findAll()) {
                sipStore.delete(aipSip);
            }
            return null;
        });
        transactionTemplate.execute(s -> {
            User user = new User(USER_ID, "user", "pwd", DATA_SPACE, Role.ROLE_READ_WRITE, null);
            userStore.save(user);

            sip = new AipSip(SIP_ID, new Checksum(ChecksumType.MD5, SIP_HASH), user, ObjectState.ARCHIVED);
            aipXml1 = new AipXml(XML1_ID, new Checksum(ChecksumType.MD5, XML1_HASH), user, sip, 1, ObjectState.ARCHIVED);
            aipXml2 = new AipXml(XML2_ID, new Checksum(ChecksumType.MD5, XML2_HASH), user, sip, 2, ObjectState.ARCHIVED);

            sipStore.save(sip);

            xmlStore.save(aipXml1);
            xmlStore.save(aipXml2);
            return null;
        });
        FileInputStream sipContent = new FileInputStream(SIP_SOURCE_PATH.toFile());
        FileInputStream xml1InputStream = new FileInputStream(Paths.get("./src/test/resources/aip/xml1.xml").toFile());
        FileInputStream xml2InputStream = new FileInputStream(Paths.get("./src/test/resources/aip/xml2.xml").toFile());

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

        ObjectConsistencyVerificationResultDto aipState = new ObjectConsistencyVerificationResultDto();
        aipState.setState(ObjectState.ARCHIVED);
        aipState.setDatabaseChecksum(sip.getChecksum());

        AipConsistencyVerificationResultDto aipStateInfoFsDto = new AipConsistencyVerificationResultDto(fsStorageService.getStorage().getName(),
                StorageType.FS, true);
        aipStateInfoFsDto.setAipState(aipState);
        when(fsStorageService.getAipInfo(any(), any(), any()))
                .thenReturn(aipStateInfoFsDto);

        AipConsistencyVerificationResultDto aipStateInfoZfsDto = new AipConsistencyVerificationResultDto(zfsStorageService.getStorage().getName(),
                StorageType.ZFS, true);
        aipStateInfoZfsDto.setAipState(aipState);
        when(zfsStorageService.getAipInfo(any(), any(), any()))
                .thenReturn(aipStateInfoZfsDto);

        AipConsistencyVerificationResultDto aipStateInfoCephDto = new AipConsistencyVerificationResultDto(cephS3StorageService.getStorage().getName(),
                StorageType.CEPH, true);
        aipStateInfoCephDto.setAipState(aipState);
        when(cephS3StorageService.getAipInfo(any(), any(), any()))
                .thenReturn(aipStateInfoCephDto);

        List<StorageService> serviceList = asList(fsStorageService, zfsStorageService, cephS3StorageService);
        when(storageProvider.createAdaptersForRead()).thenReturn(serviceList);

        when(storageProvider.createAdapter(s1.getId())).thenReturn(fsStorageService);

        AipRetrievalResource aip1 = new AipRetrievalResource(sipContent.getChannel());
        aip1.setSip(sipContent);
        aip1.addXml(1, xml1InputStream);
        aip1.addXml(2, xml2InputStream);
        when(fsStorageService.getAip(SIP_ID, DATA_SPACE, 1, 2)).thenReturn(aip1);
        when(zfsStorageService.getAip(SIP_ID, DATA_SPACE, 1, 2)).thenReturn(aip1);
        when(cephS3StorageService.getAip(SIP_ID, DATA_SPACE, 1, 2)).thenReturn(aip1);

        AipRetrievalResource aip2 = new AipRetrievalResource(sipContent.getChannel());
        aip2.setSip(sipContent);
        aip2.addXml(2, xml2InputStream);
        when(fsStorageService.getAip(SIP_ID, DATA_SPACE, 2)).thenReturn(aip2);
        when(zfsStorageService.getAip(SIP_ID, DATA_SPACE, 2)).thenReturn(aip2);
        when(cephS3StorageService.getAip(SIP_ID, DATA_SPACE, 2)).thenReturn(aip2);

        String xml1Id = toXmlId(SIP_ID, 1);
        ObjectRetrievalResource xml1res = new ObjectRetrievalResource(xml1InputStream, xml1InputStream.getChannel());
        String xmll2Id = toXmlId(SIP_ID, 2);
        ObjectRetrievalResource xml2res = new ObjectRetrievalResource(xml2InputStream, xml2InputStream.getChannel());

        when(fsStorageService.getObject(xml1Id, DATA_SPACE)).thenReturn(xml1res);
        when(zfsStorageService.getObject(xml1Id, DATA_SPACE)).thenReturn(xml1res);
        when(cephS3StorageService.getObject(xml1Id, DATA_SPACE)).thenReturn(xml1res);

        when(fsStorageService.getObject(xmll2Id, DATA_SPACE)).thenReturn(xml2res);
        when(zfsStorageService.getObject(xmll2Id, DATA_SPACE)).thenReturn(xml2res);
        when(cephS3StorageService.getObject(xmll2Id, DATA_SPACE)).thenReturn(xml2res);
    }

    /**
     * Send request for AIP data and verifies that ZIP file containing one ZIP (SIP) and latest AIP XML is retrieved.
     *
     * @throws Exception
     */
    @Test
    public void getSipAndLatestXml() throws Exception {
        byte[] zip = mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/{aipId}", SIP_ID))
                .andExpect(header().string("Content-Type", "application/zip"))
                .andExpect(header().string("Content-Disposition",
                        "attachment; filename=aip_" + SIP_ID + ".zip"))
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
        assertThat(packedFiles, containsInAnyOrder(SIP_ID + ".zip", toXmlId(SIP_ID, 2) + ".xml"));
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
                .perform(MockMvcRequestBuilders.get(BASE + "/{aipId}", SIP_ID).param("all", "true"))
                .andExpect(header().string("Content-Type", "application/zip"))
                .andExpect(header().string("Content-Disposition",
                        "attachment; filename=aip_" + SIP_ID + ".zip"))
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
        assertThat(packedFiles, containsInAnyOrder(SIP_ID + ".zip", toXmlId(SIP_ID, 1) + ".xml", toXmlId(SIP_ID, 2) + ".xml"));
    }

    /**
     * Send request for SIP data for a nonexistent SIP and verifies a Bad Request error (400) is returned.
     *
     * @throws Exception
     */
    @Test
    public void getSipNonExistentSip() throws Exception {
        mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/{aipId}", "nonExistentaipId"))
                .andExpect(status().isBadRequest());
    }

    /**
     * Send request for latest xml and verifies data in response.
     *
     * @throws Exception
     */
    @Test
    public void getLatestXml() throws Exception {
        String xmlContent = mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/{aipId}/xml", SIP_ID))
                .andExpect(status().isOk())
                .andReturn().getResponse()
                .getContentAsString();
        assertThat(xmlContent, equalTo(XML2_CONTENT));
    }

    /**
     * Send request for latest xml for and non existent sip and verifies a Bad Request error (400) is returned.
     *
     * @throws Exception
     */
    @Test
    public void getLatestXmlNonExistentSip() throws Exception {
        mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/{aipId}/xml", "nonExistentaipId"))
                .andExpect(status().isBadRequest())
                .andReturn().getResponse()
                .getContentAsByteArray();
    }

    /**
     * Send request for xml specified by version and verifies data in response.
     *
     * @throws Exception
     */
    @Test
    public void getXmlByVersion() throws Exception {
        String xmlContent = mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/{aipId}/xml", SIP_ID).param("v", "1"))
                .andExpect(status().isOk())
                .andReturn().getResponse()
                .getContentAsString();
        assertThat(xmlContent, equalTo(XML1_CONTENT));
    }

    /**
     * Send request for xml specified by version and verifies data in response.
     *
     * @throws Exception
     */
    @Test
    public void getXmlByVersionNonExistentVersion() throws Exception {
        mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/xml/{aipId}", SIP_ID).param("v", "3"))
                .andExpect(status().isNotFound());
    }

    /**
     * Send AIP creation request with AIP data (sip & xml) and verifies that response contains ID of newly created AIP (SIP).
     * Then verifies that SIP exists in the database.
     *
     * @throws Exception
     */
    @Test
    public void saveIdProvided() throws Exception {
        String aipId = UUID.randomUUID().toString();
        String xmlId = "testXmlId";

        MockMultipartFile sipFile = new MockMultipartFile(
                "sip", "sip", "text/plain", Files.readAllBytes(SIP_SOURCE_PATH));
        MockMultipartFile xmlFile = new MockMultipartFile(
                "aipXml", "xml", "text/plain", xmlId.getBytes());

        String xmlHash = "af5e897c3cc424f31b84af579b274626";
        String aipIdReturned = mvc(api)
                .perform(MockMvcRequestBuilders
                        .fileUpload(BASE + "/save").file(sipFile).file(xmlFile)
                        .param("sipChecksumValue", SIP_HASH)
                        .param("sipChecksumType", valueOf(ChecksumType.MD5))
                        .param("aipXmlChecksumValue", xmlHash)
                        .param("aipXmlChecksumType", valueOf(ChecksumType.MD5))
                        .param("UUID", aipId))
                .andExpect(status().isOk())
                .andReturn().getResponse()
                .getContentAsString();

        assertThat(aipIdReturned, not(isEmptyOrNullString()));
        assertThat(aipId, equalTo(aipIdReturned));
        Thread.sleep(5000);

        AipSip aipSip = sipStore.find(aipId);
        assertThat(aipSip.getState(), is(ObjectState.ARCHIVED));
        assertThat(aipSip.getXmls().size(), is(1));
        assertThat(aipSip.getXml(0).getState(), is(ObjectState.ARCHIVED));
    }

    /**
     * Send AIP creation request with AIP data (sip & xml) and verifies that response contains ID of newly created AIP (SIP).
     * Then verifies that SIP exists in the database.
     *
     * @throws Exception
     */
    @Test
    public void saveIdNotProvided() throws Exception {
        String xmlId = "testXmlId";

        MockMultipartFile sipFile = new MockMultipartFile(
                "sip", "sip", "text/plain", Files.readAllBytes(SIP_SOURCE_PATH));
        MockMultipartFile xmlFile = new MockMultipartFile(
                "aipXml", "xml", "text/plain", xmlId.getBytes());

        String xmlHash = "af5e897c3cc424f31b84af579b274626";
        String aipId = mvc(api)
                .perform(MockMvcRequestBuilders
                        .fileUpload(BASE + "/save").file(sipFile).file(xmlFile)
                        .param("sipChecksumValue", SIP_HASH)
                        .param("sipChecksumType", valueOf(ChecksumType.MD5))
                        .param("aipXmlChecksumValue", xmlHash)
                        .param("aipXmlChecksumType", valueOf(ChecksumType.MD5)))
                .andExpect(status().isOk())
                .andReturn().getResponse()
                .getContentAsString();
        Thread.sleep(5000);

        AipSip aipSip = sipStore.find(aipId);
        assertThat(aipSip.getState(), is(ObjectState.ARCHIVED));
        assertThat(aipSip.getXmls().size(), is(1));
        assertThat(aipSip.getXml(0).getState(), is(ObjectState.ARCHIVED));
    }

    /**
     * Send AIP creation request with AIP data where MD5 param does not match MD5 of a file.
     *
     * @throws Exception
     */
    @Test
    public void saveNonMatchingHashOfSip() throws Exception {
        MockMultipartFile sipFile = new MockMultipartFile(
                "sip", "sip", "text/plain", Files.readAllBytes(SIP_SOURCE_PATH));
        MockMultipartFile xmlFile = new MockMultipartFile(
                "aipXml", "xml", "text/plain", XML1_CONTENT.getBytes());

        mvc(api)
                .perform(MockMvcRequestBuilders
                        .fileUpload(BASE + "/save").file(sipFile).file(xmlFile)
                        .param("sipChecksumValue", XML1_HASH)
                        .param("sipChecksumType", valueOf(ChecksumType.MD5))
                        .param("aipXmlChecksumValue", XML1_HASH)
                        .param("aipXmlChecksumType", valueOf(ChecksumType.MD5)))
                .andExpect(status().isUnprocessableEntity());
    }

    /**
     * Send AIP creation request with AIP data where MD5 param does not match MD5 of a file.
     *
     * @throws Exception
     */
    @Test
    public void saveNonMatchingHashOfXml() throws Exception {
        MockMultipartFile sipFile = new MockMultipartFile(
                "sip", "sip", "text/plain", Files.readAllBytes(SIP_SOURCE_PATH));
        MockMultipartFile xmlFile = new MockMultipartFile(
                "aipXml", "xml", "text/plain", XML1_CONTENT.getBytes());

        mvc(api)
                .perform(MockMvcRequestBuilders
                        .fileUpload(BASE + "/save").file(sipFile).file(xmlFile)
                        .param("sipChecksumValue", SIP_HASH)
                        .param("sipChecksumType", valueOf(ChecksumType.MD5))
                        .param("aipXmlChecksumValue", SIP_HASH)
                        .param("aipXmlChecksumType", valueOf(ChecksumType.MD5)))
                .andExpect(status().isUnprocessableEntity());
    }

    /**
     * Send request for XML update.
     * Then checks AIP state in DB and verifies new XML record is there.
     *
     * @throws Exception
     */
    @Test
    public void updateXml() throws Exception {
        AipSip aipSip = sipStore.find(SIP_ID);
        int countOfXmlVersions = aipSip.getXmls().size();

        MockMultipartFile xmlFile = new MockMultipartFile(
                "xml", "xml", "text/plain", XML2_CONTENT.getBytes());

        mvc(api)
                .perform(MockMvcRequestBuilders.fileUpload(BASE + "/{aipId}/update", SIP_ID).file(xmlFile)
                        .param("checksumType", ChecksumType.MD5.toString())
                        .param("checksumValue", XML2_HASH)
                        .contentType(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk());
        Thread.sleep(3000);

        aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getXmls().size(), is(countOfXmlVersions + 1));
        assertThat(aipSip.getLatestXml().getState(), is(ObjectState.ARCHIVED));
    }

    /**
     * Send request for XML update where MD5 param does not match MD5 of a file.
     * Then checks AIP in DB and verifies that no new XML was saved.
     *
     * @throws Exception
     */
    @Test
    public void updateXmlNonMatchingHash() throws Exception {
        AipSip aipSip = sipStore.find(SIP_ID);
        List<AipXml> xmls = aipSip.getXmls();

        MockMultipartFile xmlFile = new MockMultipartFile(
                "xml", "xml", "text/plain", XML2_CONTENT.getBytes());
        mvc(api)
                .perform(MockMvcRequestBuilders.fileUpload(BASE + "/{aipId}/update", SIP_ID).file(xmlFile)
                        .param("checksumType", ChecksumType.MD5.toString())
                        .param("checksumValue", SIP_HASH)
                        .contentType(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isUnprocessableEntity());

        assertThat(asSet(aipSip.getXmls()), is(asSet(xmls)));
    }

    /**
     * Sends request for XML update that fails during the saving process to one of the storage services.
     * Then checks that the new XML is first in the state PROCESSING and later it is switched to the state ROLLED_BACK.
     *
     * @throws Exception
     */
    @Test
    public void getDuringXmlUpdateWhichFailsInTheEnd() throws Exception {
        doAnswer(invocation -> {
            Thread.sleep(500);
            throw new IllegalStateException("whatever exception");
        }).when(cephS3StorageService).storeObject(any(), any(), any());
        when(storageProvider.createAdaptersForWriteOperation()).thenReturn(asList(cephS3StorageService));

        AipSip aipSip = sipStore.find(SIP_ID);
        int countOfXmlVersions = aipSip.getXmls().size();

        MockMultipartFile xmlFile = new MockMultipartFile(
                "xml", "xml", "text/plain", XML2_CONTENT.getBytes());
        mvc(api)
                .perform(MockMvcRequestBuilders.fileUpload(BASE + "/{aipId}/update", SIP_ID).file(xmlFile)
                        .param("checksumType", ChecksumType.MD5.toString())
                        .param("checksumValue", XML2_HASH)
                        .contentType(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk());
        aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getXmls().size(), is(countOfXmlVersions + 1));
        assertThat(aipSip.getLatestXml().getState(), is(ObjectState.PROCESSING));
        Thread.sleep(3000);
        aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getLatestXml().getState(), is(ObjectState.ROLLED_BACK));
    }

    /**
     * Sends AIP removeObject (soft delete) request then sends AIP state request and verifies state change.
     *
     * @throws Exception
     */
    @Test
    public void remove() throws Exception {
        mvc(api)
                .perform(MockMvcRequestBuilders.put(BASE + "/{aipId}/remove", SIP_ID))
                .andExpect(status().isOk());
        AipSip aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getState(), is(ObjectState.REMOVED));
        List<ObjectAudit> operationsOfObject = objectAuditStore.findOperationsOfObject(SIP_ID);
        ObjectAudit latestAudit = operationsOfObject.get(operationsOfObject.size() - 1);
        assertThat(latestAudit.getOperation(), is(AuditedOperation.REMOVAL));
        assertThat(latestAudit.getUser(), is(new User(USER_ID)));
        assertThat(latestAudit.getIdInDatabase(), is(SIP_ID));
    }

    /**
     * Send renew request on created AIP verify its status code.
     * At the end checks state of AIP in DB and verify its state is ARCHIVED.
     *
     * @throws Exception
     */
    @Test
    public void renew() throws Exception {
        AipSip aipSip = sipStore.find(SIP_ID);
        aipSip.setState(ObjectState.REMOVED);
        transactionTemplate.execute(s -> sipStore.save(aipSip));

        mvc(api)
                .perform(MockMvcRequestBuilders.put(BASE + "/{aipId}/renew", SIP_ID))
                .andExpect(status().isOk());
        AipSip sipAfterRenew = sipStore.find(SIP_ID);
        assertThat(sipAfterRenew.getState(), is(ObjectState.ARCHIVED));
    }

    /**
     * Send delete request on created AIP verify its status code.
     * At the end send request for AIP data and verify that 404 error status is retrieved.
     *
     * @throws Exception
     */
    @Test
    public void delete() throws Exception {
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}", SIP_ID))
                .andExpect(status().isOk());
        AipSip aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getState(), is(ObjectState.DELETED));

        mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/{aipId}", SIP_ID)).andExpect(status().is(403));
    }

    /**
     * Send request for AIP state and verify that the AIP is in the state ARCHIVED and and it is stored at the storage
     * services of types: FS, ZFS and CEPH.
     *
     * @throws Exception
     */
    @Test
    public void getAipInfo() throws Exception {
        mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/{aipId}/info", SIP_ID).param("storageId",
                        s1.getId()))
                .andExpect(status().isOk())
                .andDo(print())
                .andExpect(jsonPath("$.aipState.state").value("ARCHIVED"))
                .andExpect(jsonPath("$.storageType", equalTo("FS")));
    }

//    /**
//     * Send request for storage state and verify response contains number of used and available bytes.
//     *
//     * @throws Exception
//     */
//    @Test
//    @Ignore
//    public void getStorageState() throws Exception {
//        mvc(api)
//                .perform(MockMvcRequestBuilders.get(BASE + "/state"))
//                .andExpect(status().isOk())
//                .andExpect(jsonPath("$.[0].storageStateData.available").value("2345"))
//                .andExpect(jsonPath("$.[0].storageStateData.used").value("1234"));
//    }

    /**
     * Send request with invalid MD5 and verify that response contains BAD_REQUEST error status.
     *
     * @throws Exception
     */
    @Test
    public void badFormatMD5() throws Exception {
        MockMultipartFile xmlFile = new MockMultipartFile(
                "xml", "xml", "text/plain", XML2_CONTENT.getBytes());
        mvc(api)
                .perform(MockMvcRequestBuilders.fileUpload(BASE + "/{aipId}/update", SIP_ID).file(xmlFile)
                        .param("checksumType", ChecksumType.MD5.toString())
                        .param("checksumValue", "invalidhash")
                        .contentType(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().is(400));
    }

    @Test
    public void checkAipSaved() throws Exception {
        String stateRetrieved = mvc(api)
                .perform(MockMvcRequestBuilders.get(BASE + "/{aipId}/state", SIP_ID))
                .andExpect(status().is(200))
                .andReturn().getResponse()
                .getContentAsString();

        assertThat(stateRetrieved.replace("\"", ""), is(ObjectState.ARCHIVED.toString()));
    }

    /**
     * Send request with invalid UUID and verify that response contains BAD_REQUEST error status.
     *
     * @throws Exception
     */
    @Test
    public void badFormatID() throws Exception {
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}", "invalidid"))
                .andExpect(status().is(400));
    }

    /**
     * in order to work, the methods handled with {@link org.springframework.transaction.support.TransactionTemplate}
     * should not be nested in any method with {@link cz.cas.lib.arcstorage.domain.store.Transactional} annotation
     */
    @Test
    public void transactionTimeoutsTest() throws Exception {
        archivalDbService.setTransactionTemplateTimeout(0);

        //updatexml
        MockMultipartFile xmlFile = new MockMultipartFile(
                "xml", "xml", "text/plain", XML2_CONTENT.getBytes());
        String contentAsString = mvc(api)
                .perform(MockMvcRequestBuilders.fileUpload(BASE + "/{aipId}/update", SIP_ID).file(xmlFile)
                        .param("checksumType", ChecksumType.MD5.toString())
                        .param("checksumValue", XML2_HASH)
                        .contentType(MediaType.APPLICATION_JSON)
                )
                .andReturn().getResponse().getContentAsString();
        assertThat(contentAsString, containsString("TransactionTimedOutException"));

        //delete
        contentAsString = mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}", SIP_ID))
                .andReturn().getResponse().getContentAsString();
        assertThat(contentAsString, containsString("TransactionTimedOutException"));

        //remove
        contentAsString = mvc(api)
                .perform(MockMvcRequestBuilders.put(BASE + "/{aipId}/remove", SIP_ID))
                .andReturn().getResponse().getContentAsString();
        assertThat(contentAsString, containsString("TransactionTimedOutException"));

        //renew
        contentAsString = mvc(api)
                .perform(MockMvcRequestBuilders.put(BASE + "/{aipId}/renew", SIP_ID))
                .andReturn().getResponse().getContentAsString();
        assertThat(contentAsString, containsString("TransactionTimedOutException"));


        String aipId = UUID.randomUUID().toString();
        String xmlId = "testXmlId";

        MockMultipartFile sipFile = new MockMultipartFile(
                "sip", "sip", "text/plain", Files.readAllBytes(SIP_SOURCE_PATH));
        xmlFile = new MockMultipartFile(
                "aipXml", "xml", "text/plain", xmlId.getBytes());

        //save
        String xmlHash = "af5e897c3cc424f31b84af579b274626";
        contentAsString = mvc(api)
                .perform(MockMvcRequestBuilders
                        .fileUpload(BASE + "/save").file(sipFile).file(xmlFile)
                        .param("sipChecksumValue", SIP_HASH)
                        .param("sipChecksumType", valueOf(ChecksumType.MD5))
                        .param("aipXmlChecksumValue", xmlHash)
                        .param("aipXmlChecksumType", valueOf(ChecksumType.MD5))
                        .param("UUID", UUID.randomUUID().toString()))
                .andReturn().getResponse()
                .getContentAsString();
        assertThat(contentAsString, containsString("TransactionTimedOutException"));
    }

    @Test
    public void rollbackEndpoint() throws Exception {
        //can't rollback XML through rollback AIP endpoint
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollback", XML2_ID))
                .andExpect(status().is(403));
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollback", XML1_ID))
                .andExpect(status().is(403));
        //can't rollback AIP which has more than one AIP XMLs
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollback", SIP_ID))
                .andExpect(status().is(403));
        //rollback AIP
        transactionTemplate.execute(t -> {
            xmlStore.delete(aipXml2);
            return null;
        });
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollback", SIP_ID))
                .andExpect(status().is(200));
        assertThat(archivalDbService.getObject(SIP_ID).getState(), is(ObjectState.ROLLED_BACK));
        assertThat(archivalDbService.getObject(XML1_ID).getState(), is(ObjectState.ROLLED_BACK));
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollback", SIP_ID))
                .andExpect(status().is(200));
        assertThat(archivalDbService.getObject(SIP_ID).getState(), is(ObjectState.ROLLED_BACK));
        assertThat(archivalDbService.getObject(XML1_ID).getState(), is(ObjectState.ROLLED_BACK));

        //rollback objects in various non-processing states
        ArchivalObject obj = new ArchivalObject();
        obj.setChecksum(new Checksum(ChecksumType.MD5, "blah"));
        obj.setState(ObjectState.ARCHIVAL_FAILURE);
        archivalDbService.saveObject(obj);
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollback", obj.getId()))
                .andExpect(status().is(200));
        assertThat(archivalDbService.getObject(obj.getId()).getState(), is(ObjectState.ROLLED_BACK));

        obj.setState(ObjectState.ROLLBACK_FAILURE);
        archivalDbService.saveObject(obj);
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollback", obj.getId()))
                .andExpect(status().is(200));
        assertThat(archivalDbService.getObject(obj.getId()).getState(), is(ObjectState.ROLLED_BACK));

        obj.setState(ObjectState.DELETED);
        archivalDbService.saveObject(obj);
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollback", obj.getId()))
                .andExpect(status().is(200));
        assertThat(archivalDbService.getObject(obj.getId()).getState(), is(ObjectState.ROLLED_BACK));

        obj.setState(ObjectState.REMOVED);
        archivalDbService.saveObject(obj);
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollback", obj.getId()))
                .andExpect(status().is(200));
        assertThat(archivalDbService.getObject(obj.getId()).getState(), is(ObjectState.ROLLED_BACK));

        obj.setState(ObjectState.DELETION_FAILURE);
        archivalDbService.saveObject(obj);
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollback", obj.getId()))
                .andExpect(status().is(200));
        assertThat(archivalDbService.getObject(obj.getId()).getState(), is(ObjectState.ROLLED_BACK));
    }

    @Test
    public void rollbackXmlEndpoint() throws Exception {
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollbackXml/2", SIP_ID))
                .andExpect(status().is(200));
        assertThat(archivalDbService.getObject(SIP_ID).getState(), is(ObjectState.ARCHIVED));
        assertThat(archivalDbService.getObject(XML1_ID).getState(), is(ObjectState.ARCHIVED));
        assertThat(archivalDbService.getObject(XML2_ID).getState(), is(ObjectState.ROLLED_BACK));

        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollbackXml/2", SIP_ID))
                .andExpect(status().is(200));
        assertThat(archivalDbService.getObject(SIP_ID).getState(), is(ObjectState.ARCHIVED));
        assertThat(archivalDbService.getObject(XML1_ID).getState(), is(ObjectState.ARCHIVED));
        assertThat(archivalDbService.getObject(XML2_ID).getState(), is(ObjectState.ROLLED_BACK));

        transactionTemplate.execute(status -> {
            xmlStore.delete(aipXml2);
            return null;
        });

        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollbackXml/1", SIP_ID))
                .andExpect(status().is(403));
        assertThat(archivalDbService.getObject(SIP_ID).getState(), is(ObjectState.ARCHIVED));
        assertThat(archivalDbService.getObject(XML1_ID).getState(), is(ObjectState.ARCHIVED));
    }

    @Test
    public void aipRollbackArchiveRollback() throws Exception {
        ArchivalObject sipAfterRollback = archivalDbService.getObject(SIP_ID);
        sipAfterRollback.setState(ObjectState.ROLLED_BACK);
        AipXml xmlAfterRollback = xmlStore.find(XML1_ID);
        xmlAfterRollback.setState(ObjectState.ROLLED_BACK);
        transactionTemplate.execute(t -> {
            archivalDbService.saveObject(sipAfterRollback);
            xmlStore.save(xmlAfterRollback);
            return null;
        });
        //try invalid archival retry.. archival retry is only allowed when there is exactly one XML
        MockMultipartFile sipFile = new MockMultipartFile(
                "sip", "sip", "text/plain", Files.readAllBytes(SIP_SOURCE_PATH));
        MockMultipartFile xmlFile = new MockMultipartFile(
                "aipXml", "xml", "text/plain", XML1_CONTENT.getBytes());

        mvc(api)
                .perform(MockMvcRequestBuilders
                        .fileUpload(BASE + "/save").file(sipFile).file(xmlFile)
                        .param("sipChecksumValue", SIP_HASH)
                        .param("sipChecksumType", valueOf(ChecksumType.MD5))
                        .param("aipXmlChecksumValue", XML1_HASH)
                        .param("aipXmlChecksumType", valueOf(ChecksumType.MD5))
                        .param("UUID", SIP_ID))
                .andExpect(status().is5xxServerError());
        transactionTemplate.execute(t -> {
            xmlStore.delete(aipXml2);
            return null;
        });
        //valid archival retry
        mvc(api)
                .perform(MockMvcRequestBuilders
                        .fileUpload(BASE + "/save").file(sipFile).file(xmlFile)
                        .param("sipChecksumValue", SIP_HASH)
                        .param("sipChecksumType", valueOf(ChecksumType.MD5))
                        .param("aipXmlChecksumValue", XML1_HASH)
                        .param("aipXmlChecksumType", valueOf(ChecksumType.MD5))
                        .param("UUID", SIP_ID))
                .andExpect(status().isOk());

        Thread.sleep(5000);
        AipSip aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getState(), is(ObjectState.ARCHIVED));
        assertThat(aipSip.getXmls().size(), is(1));
        assertThat(aipSip.getXml(0).getState(), is(ObjectState.ARCHIVED));
        //except of state are metadata equals
        sipAfterRollback.setState(ObjectState.ARCHIVED);
        xmlAfterRollback.setState(ObjectState.ARCHIVED);
        assertThat(aipSip.toDto().metadataEquals(sipAfterRollback.toDto()), is(true));
        assertThat(aipSip.getLatestXml().toDto().metadataEquals(xmlAfterRollback.toDto()), is(true));
        sipAfterRollback.setState(ObjectState.ROLLED_BACK);
        xmlAfterRollback.setState(ObjectState.ROLLED_BACK);
        //rollback AIP
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollback", SIP_ID))
                .andExpect(status().is(200));
        ArchivalObject sipAfterSndRollback = archivalDbService.getObject(SIP_ID);
        ArchivalObject xmlAfterSndRollback = archivalDbService.getObject(XML1_ID);
        assertThat(sipAfterSndRollback.getState(), is(ObjectState.ROLLED_BACK));
        assertThat(xmlAfterSndRollback.getState(), is(ObjectState.ROLLED_BACK));

        assertThat(sipAfterRollback.toDto().metadataEquals(sipAfterSndRollback.toDto()), is(true));
        assertThat(xmlAfterRollback.toDto().metadataEquals(xmlAfterSndRollback.toDto()), is(true));
    }

    @Test
    public void xmlRollbackArchiveRollback() throws Exception {
        ArchivalObject sndXmlBeforeRollback = archivalDbService.getObject(XML2_ID);
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollbackXml/2", SIP_ID))
                .andExpect(status().is(200));
        ArchivalObject sndXmlAfterFirstRollback = archivalDbService.getObject(XML2_ID);
        assertThat(archivalDbService.getObject(SIP_ID).getState(), is(ObjectState.ARCHIVED));
        assertThat(archivalDbService.getObject(XML1_ID).getState(), is(ObjectState.ARCHIVED));
        assertThat(sndXmlAfterFirstRollback.getState(), is(ObjectState.ROLLED_BACK));
        sndXmlBeforeRollback.setState(ObjectState.ROLLED_BACK);
        assertThat(sndXmlAfterFirstRollback.toDto().metadataEquals(sndXmlBeforeRollback.toDto()), is(true));
        sndXmlBeforeRollback.setState(ObjectState.ARCHIVED);

        MockMultipartFile xmlFile = new MockMultipartFile(
                "xml", "xml", "text/plain", XML2_CONTENT.getBytes());
        mvc(api)
                .perform(MockMvcRequestBuilders.fileUpload(BASE + "/{aipId}/update", SIP_ID).file(xmlFile)
                        .param("checksumType", ChecksumType.MD5.toString())
                        .param("checksumValue", XML2_HASH)
                        .contentType(MediaType.APPLICATION_JSON)
                )
                .andExpect(status().isOk());
        Thread.sleep(3000);

        AipSip aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getXmls().size(), is(2));
        AipXml sndXmlAfterArchivalRetry = aipSip.getLatestXml();
        assertThat(sndXmlAfterArchivalRetry.getState(), is(ObjectState.ARCHIVED));
        assertThat(sndXmlBeforeRollback.toDto().metadataEquals(sndXmlAfterArchivalRetry.toDto()), is(true));

        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE + "/{aipId}/rollbackXml/2", SIP_ID))
                .andExpect(status().is(200));
        assertThat(archivalDbService.getObject(SIP_ID).getState(), is(ObjectState.ARCHIVED));
        assertThat(archivalDbService.getObject(XML1_ID).getState(), is(ObjectState.ARCHIVED));
        ArchivalObject sndXmlAfterSndRollback = archivalDbService.getObject(XML2_ID);
        assertThat(sndXmlAfterSndRollback.toDto().metadataEquals(sndXmlAfterFirstRollback.toDto()), is(true));
    }

    private static String toXmlId(String aipId, int version) {
        return String.format("%s_xml_%d", aipId, version);
    }

    @Inject
    public void setTmpFolder(@Value("${arcstorage.tmpFolder}") String path) {
        this.tmpFolder = Paths.get(path);
    }

    @Inject
    public void setObjectAuditStore(ObjectAuditStore objectAuditStore) {
        this.objectAuditStore = objectAuditStore;
    }
}
