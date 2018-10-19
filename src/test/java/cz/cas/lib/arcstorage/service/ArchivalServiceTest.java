package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.*;
import cz.cas.lib.arcstorage.domain.store.*;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.security.Role;
import cz.cas.lib.arcstorage.security.user.UserDelegate;
import cz.cas.lib.arcstorage.security.user.UserStore;
import cz.cas.lib.arcstorage.service.exception.state.DeletedStateException;
import cz.cas.lib.arcstorage.service.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.service.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.storagesync.ObjectAuditStore;
import cz.cas.lib.arcstorage.util.Utils;
import helper.DbTest;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.orm.jpa.JpaTransactionManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static cz.cas.lib.arcstorage.util.Utils.asList;
import static helper.ThrowableAssertion.assertThrown;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ArchivalServiceTest extends DbTest {

    private static final ArchivalService archivalService = new ArchivalService();
    private static final AipSipStore aipSipStore = new AipSipStore();
    private static final AipXmlStore aipXmlStore = new AipXmlStore();
    private static final StorageStore storageStore = new StorageStore();
    private static final ArchivalDbService archivalDbService = new ArchivalDbService();
    private static final ConfigurationStore configurationStore = new ConfigurationStore();
    private static final UserStore userStore = new UserStore();
    private static final ArchivalObjectStore objectStore = new ArchivalObjectStore();

    private static final String USER_ID = "dd23923a-923b-43b1-8a8e-3eebc7598432";
    private static final String DATA_SPACE = "dataSpace";
    private static final String SIP_ID = "SIPtestID";
    private static final String SIP_MD5 = "101b295a91f771d96e1987ff501b034c";
    private static final Checksum SIP_CHECKSUM = new Checksum(ChecksumType.MD5, SIP_MD5);

    private static final String SIP2_ID = "testSipId";

    private static final String XML1_ID = toXmlId(SIP_ID, 1);
    private static final String XML2_ID = toXmlId(SIP_ID, 2);

    private static final String XML1_MD5 = "5e95c70e5ca025d836f3bbe04fab0968";
    private static final Checksum XML1_CHECKSUM = new Checksum(ChecksumType.MD5, XML1_MD5);

    private static final String XML2_MD5 = "2aef000621927f2091b88f32d5a3ff00";
    private static final Checksum XML2_CHECKSUM = new Checksum(ChecksumType.MD5, XML2_MD5);

    private static final String STORAGE_CONFIG = "{\"adapterType\": \"S3\", \"userKey\": \"key\", \"userSecret\": \"secret\", \"region\": \"region\"}";

    private static Path tmpFolder;

    private static InputStream sipStream() {
        return new ByteArrayInputStream(SIP_ID.getBytes());
    }

    private static InputStream xml1Stream() {
        return new ByteArrayInputStream(XML1_ID.getBytes());
    }

    private static InputStream xml2Stream() {
        return new ByteArrayInputStream(XML2_ID.getBytes());
    }

    @Mock
    private StorageProvider storageProvider;

    @Mock
    private StorageService storageService;

    @Mock
    private ArchivalAsyncService async;

    @Mock
    private ObjectAuditStore objectAuditStore;

    private Storage storage;

    private AipSip SIP;
    private AipXml XML1;
    private AipXml XML2;

    @BeforeClass
    public static void beforeClass() throws IOException {
        Properties props = new Properties();
        props.load(ClassLoader.getSystemResourceAsStream("application.properties"));
        tmpFolder = Paths.get(props.getProperty("arcstorage.tmp-folder"));
        Files.createDirectories(tmpFolder);
    }

    @Before
    public void setup() throws Exception {
        clearDatabase();
        MockitoAnnotations.initMocks(this);
        archivalDbService.setTransactionTemplate(new JpaTransactionManager(getFactory()), 5);


        SIP = new AipSip(SIP_ID, SIP_CHECKSUM, new User(USER_ID), ObjectState.ARCHIVED);
        XML1 = new AipXml(XML1_ID, XML1_CHECKSUM, new User(USER_ID), null, 1, ObjectState.ARCHIVED);
        XML2 = new AipXml(XML2_ID, XML2_CHECKSUM, new User(USER_ID), null, 2, ObjectState.ARCHIVED);

        initializeStores(aipSipStore, aipXmlStore, storageStore, configurationStore, userStore, objectStore);
        userStore.save(new User(USER_ID, "username", "password", DATA_SPACE, Role.ROLE_READ_WRITE, null));
        configurationStore.save(new Configuration(2, false));
        archivalDbService.setAipSipStore(aipSipStore);
        archivalDbService.setAipXmlStore(aipXmlStore);
        archivalDbService.setConfigurationStore(configurationStore);
        archivalDbService.setObjectAuditStore(objectAuditStore);
        archivalDbService.setArchivalObjectStore(objectStore);
        archivalDbService.setUserDetails(new UserDelegate(new User(USER_ID)));


        async.setArchivalDbService(archivalDbService);

        archivalService.setArchivalDbService(archivalDbService);
        archivalService.setAsyncService(async);
        archivalService.setStorageProvider(storageProvider);
        archivalService.setTmpFolder(tmpFolder.toString());

        aipSipStore.save(SIP);
        XML1.setSip(SIP);
        XML2.setSip(SIP);
        aipXmlStore.save(XML1);
        aipXmlStore.save(XML2);

        storage = new Storage();
        storage.setPriority(1);
        storage.setName("test ceph storage");
        storage.setStorageType(StorageType.CEPH);
        storage.setConfig(STORAGE_CONFIG);
        storage.setReachable(true);
        storageStore.save(storage);
        when(storageService.getStorage()).thenReturn(storage);

        AipRetrievalResource aip1 = new AipRetrievalResource(null);
        aip1.setSip(sipStream());
        aip1.addXml(1, xml1Stream());

        AipRetrievalResource aip2 = new AipRetrievalResource(null);
        aip2.setSip(sipStream());
        aip2.addXml(2, xml2Stream());

        AipRetrievalResource aip3 = new AipRetrievalResource(null);
        aip3.setSip(sipStream());
        aip3.addXml(1, xml1Stream());
        aip3.addXml(2, xml2Stream());

        when(storageService.getAip(SIP_ID, DATA_SPACE, 1)).thenReturn(aip1);
        when(storageService.getAip(SIP_ID, DATA_SPACE, 2)).thenReturn(aip2);
        when(storageService.getAip(SIP_ID, DATA_SPACE, 1, 2)).thenReturn(aip3);

        when(storageProvider.createAllAdapters()).thenReturn(asList(storageService));
        when(storageProvider.createAdaptersForWriteOperation()).thenReturn(asList(storageService));

        List<StorageService> serviceList = asList(storageService, storageService, storageService);
        when(storageProvider.getReachableStorageServicesByPriorities()).thenReturn(serviceList);

        ObjectRetrievalResource xml1 = new ObjectRetrievalResource(xml1Stream(), null);
        ObjectRetrievalResource xml2 = new ObjectRetrievalResource(xml2Stream(), null);

        when(storageService.getObject(XML1_ID, DATA_SPACE)).thenReturn(xml1);
        when(storageService.getObject(XML2_ID, DATA_SPACE)).thenReturn(xml2);
    }

    @AfterClass
    public static void tearDown() throws IOException {
        FileUtils.cleanDirectory(tmpFolder.toFile());
    }

    @Test
    public void getAll() throws Exception {
        AipRetrievalResource aip = archivalService.getAip(SIP_ID, true);

        try (InputStream ios = aip.getSip(); InputStream sipStream = sipStream()) {
            assertTrue(IOUtils.contentEquals(ios, sipStream));
        }

        Map<Integer, InputStream> xmls = aip.getXmls();
        assertThat(xmls.values(), hasSize(2));

        try (InputStream inputStream1 = aip.getXmls().get(1); InputStream xml1Stream = xml1Stream()) {
            assertThat(inputStream1, notNullValue());
            assertTrue(IOUtils.contentEquals(inputStream1, xml1Stream));
        }

        try (InputStream inputStream2 = aip.getXmls().get(2); InputStream xml2Stream = xml2Stream()) {
            assertThat(inputStream2, notNullValue());
            assertTrue(IOUtils.contentEquals(inputStream2, xml2Stream));
        }
    }

    @Test
    public void getLatest() throws Exception {
        AipRetrievalResource aip = archivalService.getAip(SIP_ID, false);

        try (InputStream ios = aip.getSip(); InputStream sipStream = sipStream()) {
            assertTrue(IOUtils.contentEquals(ios, sipStream));
        }

        Map<Integer, InputStream> xmls = aip.getXmls();
        assertThat(xmls.values(), hasSize(1));

        try (InputStream inputStream = aip.getXmls().get(2); InputStream xml2Stream = xml2Stream()) {
            assertThat(inputStream, notNullValue());
            assertTrue(IOUtils.contentEquals(inputStream, xml2Stream));
        }
    }

    @Test
    public void getXml() throws Exception {
        Utils.Pair<Integer, ObjectRetrievalResource> xml = archivalService.getXml(SIP_ID, null);
        assertThat(xml.getL(), is(2));
        try (InputStream inputStream = xml.getR().getInputStream(); InputStream xml2Stream = xml2Stream()) {
            assertThat(inputStream, notNullValue());
            assertTrue(IOUtils.contentEquals(inputStream, xml2Stream));
        }
    }

    @Test
    public void getXmlVersionSpecified() throws Exception {
        Utils.Pair<Integer, ObjectRetrievalResource> xml = archivalService.getXml(SIP_ID, 1);
        assertThat(xml.getL(), is(1));
        try (InputStream inputStream = xml.getR().getInputStream(); InputStream xml1Stream = xml1Stream()) {
            assertThat(inputStream, notNullValue());
            assertTrue(IOUtils.contentEquals(inputStream, xml1Stream));
        }

        xml = archivalService.getXml(SIP_ID, 2);
        assertThat(xml.getL(), is(2));
        try (InputStream inputStream = xml.getR().getInputStream(); InputStream xml2Stream = xml2Stream()) {
            assertThat(inputStream, notNullValue());
            assertTrue(IOUtils.contentEquals(inputStream, xml2Stream));
        }
    }

    @Test
    public void getXmlNonExistentVersionSpecified() {
        assertThrown(() -> archivalService.getXml(SIP_ID, 3)).isInstanceOf(MissingObject.class);
    }

    @Test
    public void getXmlIllegalStates() {
        XML2.setState(ObjectState.ROLLED_BACK);
        aipXmlStore.save(XML2);
        assertThrown(() -> archivalService.getXml(SIP_ID, 2)).isInstanceOf(RollbackStateException.class);

        XML2.setState(ObjectState.PROCESSING);
        aipXmlStore.save(XML2);
        assertThrown(() -> archivalService.getXml(SIP_ID, 2)).isInstanceOf(StillProcessingStateException.class);
    }

    @Test
    public void store() throws Exception {
        AipDto aipDto = new AipDto(USER_ID, SIP2_ID, sipStream(), SIP_CHECKSUM, xml1Stream(), XML1_CHECKSUM);
        archivalService.saveAip(aipDto);

        ArgumentCaptor<byte[]> xmlBytesCaptor = ArgumentCaptor.forClass(byte[].class);

        AipSip aipSip = archivalDbService.getAip(SIP2_ID);
        assertThat(aipSip, notNullValue());
        verify(async).saveAip(eq(aipDto), anyObject(), xmlBytesCaptor.capture(), anyList(), anyString());
        String byteString = IOUtils.toString(xmlBytesCaptor.getValue());
        assertThat(byteString, containsString(IOUtils.toString(xml1Stream())));
    }

    @Test
    public void updateXml() throws Exception {
        Collection allXmls = aipXmlStore.findAll();
        assertThat(allXmls.size(), is(2));

        archivalService.saveXmlAsynchronously(SIP_ID, xml1Stream(), XML1_CHECKSUM, null);

        ArgumentCaptor<TmpSourceHolder> resourceHolderCaptor = ArgumentCaptor.forClass(TmpSourceHolder.class);

        allXmls = aipXmlStore.findAll();
        assertThat(allXmls.size(), is(3));
        AipXml newXml = aipXmlStore.findBySipAndVersion(SIP_ID, 3).stream().findFirst().get();
        ArchivalObjectDto xmlRef = newXml.toDto();
        verify(async).saveObject(eq(xmlRef), resourceHolderCaptor.capture(), anyList());
        assertThat(resourceHolderCaptor.getValue(), instanceOf(ByteArrayHolder.class));
        assertTrue(IOUtils.contentEquals(resourceHolderCaptor.getValue().createInputStream(), xml1Stream()));
    }

//    @Test
//    public void getAipStatesInfo() throws Exception {
//        when(storageService.getAipInfo(anyObject(), anyObject(), anyObject(), anyObject())).thenReturn(
//                new AipStateInfoDto("", StorageType.CEPH, ObjectState.ARCHIVED, null));
//
//        SIP.setState(ObjectState.ARCHIVED);
//        aipSipStore.saveAip(SIP);
//
//        flushCache();
//
//        List<AipStateInfoDto> aipStateInfoDtos = archivalService.getAipStates(SIP_ID);
//
//        assertThat(aipStateInfoDtos.size(), is(1));
//
//        AipStateInfoDto aipStateInfoDto = aipStateInfoDtos.getAip(0);
//        assertThat(aipStateInfoDto.getStorageType(), is(StorageType.CEPH));
//        assertThat(aipStateInfoDto.getObjectState(), is(ObjectState.ARCHIVED));
//    }

    @Test
    public void getIllegalStateSipTest() {
        SIP.setState(ObjectState.DELETED);
        aipSipStore.save(SIP);
        assertThrown(() -> archivalService.getAip(SIP_ID, true)).isInstanceOf(DeletedStateException.class);

        SIP.setState(ObjectState.ROLLED_BACK);
        aipSipStore.save(SIP);
        assertThrown(() -> archivalService.getAip(SIP_ID, true)).isInstanceOf(RollbackStateException.class);

        SIP.setState(ObjectState.PROCESSING);
        aipSipStore.save(SIP);
        assertThrown(() -> archivalService.getAip(SIP_ID, true)).isInstanceOf(StillProcessingStateException.class);

        XML1.setState(ObjectState.PROCESSING);
        aipXmlStore.save(XML1);
        assertThrown(() -> archivalService.getAip(SIP_ID, true)).isInstanceOf(StillProcessingStateException.class);

        XML1.setState(ObjectState.ROLLED_BACK);
        SIP.setState(ObjectState.ARCHIVED);
        aipSipStore.save(SIP);
        aipXmlStore.save(XML1);
        aipXmlStore.delete(XML2);
        //findall does the trick so that service sees up-to-date records
        aipXmlStore.findAll();
        assertThrown(() -> archivalService.getAip(SIP_ID, true)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void delete() throws Exception {
        archivalService.delete(SIP_ID);

        AipSip sip = archivalDbService.getAip(SIP_ID);
        assertThat(sip.getState(), is(ObjectState.DELETED));

        ArgumentCaptor<ArchivalObjectDto> objectCaptor = ArgumentCaptor.forClass(ArchivalObjectDto.class);

        verify(async).deleteObject(objectCaptor.capture(), anyList());
        assertThat(objectCaptor.getValue().getDatabaseId(), is(sip.getId()));
    }

    @Test
    public void remove() throws Exception {
        archivalService.removeObject(SIP_ID);

        AipSip sip = archivalDbService.getAip(SIP_ID);
        assertThat(sip.getState(), is(ObjectState.REMOVED));
        verify(async).removeObject(eq(sip.getId()), anyList(), anyString());
    }

    @Test
    public void getAipState() {
        ObjectState aipState = archivalService.getAipState(SIP_ID);
        assertThat(aipState, is(ObjectState.ARCHIVED));

        SIP.setState(ObjectState.DELETED);
        aipSipStore.save(SIP);

        aipState = archivalService.getAipState(SIP_ID);
        assertThat(aipState, is(ObjectState.DELETED));
    }

    @Test
    public void cleanUp() throws Exception{
        ArchivalObject o1 = new ArchivalObject(null, null, ObjectState.DELETED);
        ArchivalObject o2 = new ArchivalObject(null, null, ObjectState.DELETION_FAILURE);
        AipSip s1 = new AipSip(UUID.randomUUID().toString(), null, null, ObjectState.PROCESSING);
        AipSip s2 = new AipSip(UUID.randomUUID().toString(), null, null, ObjectState.ARCHIVED);
        AipXml x1 = new AipXml(UUID.randomUUID().toString(), null, null, s1, 1, ObjectState.ARCHIVAL_FAILURE);
        AipXml x2 = new AipXml(UUID.randomUUID().toString(), null, null, s1, 2, ObjectState.PRE_PROCESSING);
        objectStore.save(asList(o1,o2,s1,s2,x1,x2));
        List<ArchivalObject> cleanup = archivalService.cleanup(true);
        assertThat(cleanup,containsInAnyOrder(o2,s1,x1,x2));
        verify(async).cleanUp(cleanup,storageProvider.createAdaptersForWriteOperation());
        cleanup = archivalService.cleanup(false);
        assertThat(cleanup,containsInAnyOrder(o2,x1));
        verify(async).cleanUp(cleanup,storageProvider.createAdaptersForWriteOperation());
    }
}
