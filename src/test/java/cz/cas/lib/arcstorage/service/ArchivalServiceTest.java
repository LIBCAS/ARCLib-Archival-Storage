package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.*;
import cz.cas.lib.arcstorage.domain.store.*;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.security.Role;
import cz.cas.lib.arcstorage.security.user.UserDelegate;
import cz.cas.lib.arcstorage.security.user.UserStore;
import cz.cas.lib.arcstorage.service.exception.state.DeletedStateException;
import cz.cas.lib.arcstorage.service.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.service.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storage.fs.FsAdapter;
import cz.cas.lib.arcstorage.storage.fs.LocalFsProcessor;
import cz.cas.lib.arcstorage.storagesync.ObjectAuditStore;
import cz.cas.lib.arcstorage.storagesync.newstorage.StorageSyncStatusStore;
import helper.DbTest;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.orm.jpa.JpaTransactionManager;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static cz.cas.lib.arcstorage.util.Utils.asList;
import static helper.ThrowableAssertion.assertThrown;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

public class ArchivalServiceTest extends DbTest {

    private static final AipService aipService = new AipService();
    private static final ArchivalService archivalService = new ArchivalService();
    private static final AipSipStore aipSipStore = new AipSipStore();
    private static final AipXmlStore aipXmlStore = new AipXmlStore();
    private static final StorageStore storageStore = new StorageStore();
    private static final ArchivalDbService archivalDbService = new ArchivalDbService();
    private static final SystemStateStore SYSTEM_STATE_STORE = new SystemStateStore();
    private static final UserStore userStore = new UserStore();
    private static final ArchivalObjectStore objectStore = new ArchivalObjectStore();
    private static final SystemAdministrationService systemAdministrationService = new SystemAdministrationService();
    private static final ExecutorService executorService = Executors.newFixedThreadPool(2);

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

    private static final String SIP_ZIP = "KPW01169310.ZIP";
    private static final Path SIP_SOURCE_PATH = Paths.get("src/test/resources", SIP_ZIP);

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
    private ArcstorageMailCenter mailCenter;

    @Mock
    private StorageProvider storageProvider;

    @Mock
    private StorageService storageService;

    @Mock
    private ArchivalAsyncService async;

    @Mock
    private ObjectAuditStore objectAuditStore;

    @Mock
    private StorageSyncStatusStore storageSyncStatusStore;

    private Storage storage;
    private User user;

    private AipSip SIP;
    private AipXml XML1;
    private AipXml XML2;

    @BeforeClass
    public static void beforeClass() throws IOException {
        Properties props = new Properties();
        props.load(ClassLoader.getSystemResourceAsStream("application.properties"));
        tmpFolder = Paths.get(props.getProperty("arcstorage.tmpFolder"));
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

        initializeStores(aipSipStore, aipXmlStore, storageStore, SYSTEM_STATE_STORE, userStore, objectStore);

        SystemStateService systemStateService = new SystemStateService();
        systemStateService.setSystemStateStore(SYSTEM_STATE_STORE);

        user = userStore.save(new User(USER_ID, "username", "password", DATA_SPACE, Role.ROLE_READ_WRITE, null));
        SYSTEM_STATE_STORE.save(new SystemState(2, false));
        archivalDbService.setAipSipStore(aipSipStore);
        archivalDbService.setAipXmlStore(aipXmlStore);
        archivalDbService.setSystemStateService(systemStateService);
        archivalDbService.setObjectAuditStore(objectAuditStore);
        archivalDbService.setArchivalObjectStore(objectStore);
        archivalDbService.setUserDetails(new UserDelegate(new User(USER_ID)));
        archivalDbService.setUserStore(userStore);


        async.setArchivalDbService(archivalDbService);

        aipService.setArchivalDbService(archivalDbService);
        aipService.setAsyncService(async);
        aipService.setStorageProvider(storageProvider);
        aipService.setTmpFolder(tmpFolder.toString());
        aipService.setArcstorageMailCenter(mailCenter);
        aipService.setExecutorService(executorService);
        aipService.setArchivalService(archivalService);

        archivalService.setArchivalDbService(archivalDbService);
        archivalService.setStorageProvider(storageProvider);
        archivalService.setTmpFolder(tmpFolder.toString());
        archivalService.setArcstorageMailCenter(mailCenter);
        archivalService.setAsync(async);

        systemAdministrationService.setStorageSyncStatusStore(storageSyncStatusStore);
        systemAdministrationService.setStorageProvider(storageProvider);
        systemAdministrationService.setArchivalDbService(archivalDbService);
        systemAdministrationService.setAsync(async);
        systemAdministrationService.setTmpFolder(tmpFolder.toString());

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

        when(storageProvider.createAdaptersForWriteOperation()).thenReturn(asList(storageService));
        when(storageProvider.createAdaptersForWriteOperation(false)).thenReturn(asList(storageService));
        when(storageProvider.createAdapter(storage.getId())).thenReturn(storageService);

        List<StorageService> serviceList = asList(storageService, storageService, storageService);
        when(storageProvider.createAdaptersForRead()).thenReturn(serviceList);

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
        AipRetrievalResource aip = aipService.getAip(SIP_ID, true);

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
    public void getAipWithListedFiles() throws Exception {
        Storage zfsStorage = new Storage();
        zfsStorage.setPriority(1);
        zfsStorage.setName("test ZFS");
        zfsStorage.setStorageType(StorageType.ZFS);
        zfsStorage.setConfig(STORAGE_CONFIG);
        zfsStorage.setReachable(true);
        zfsStorage.setHost("localhost");
        storageStore.save(zfsStorage);

        FsAdapter fsAdapter = mock(FsAdapter.class);
        when(fsAdapter.getStorage()).thenReturn(zfsStorage);
        when(storageProvider.createAdaptersForRead()).thenReturn(List.of(fsAdapter));
        LocalFsProcessor localFsProcessorMock = mock(LocalFsProcessor.class);
        when(localFsProcessorMock.getStorage()).thenReturn(zfsStorage);
        when(localFsProcessorMock.getAipDataFilePath(anyString(), any())).thenReturn(SIP_SOURCE_PATH);
        when(fsAdapter.getFsProcessor()).thenReturn(localFsProcessorMock);

        HashSet<String> wantedFiles = new HashSet<>();
        wantedFiles.add("KPW01169310/ALTO/ALTO_KPW01169310_0001.XML");
        wantedFiles.add("KPW01169310/ALTO/desktop.ini");
        wantedFiles.add("KPW01169310/TXT/TXT_KPW01169310_0002.TXT");
        wantedFiles.add("KPW01169310/userCopy/UC_KPW01169310_0002.JP2");

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(0);
        aipService.streamAipReducedByFileListFromLocalStorage(SIP.getId(), byteArrayOutputStream, wantedFiles);

        Set<String> paths = new LinkedHashSet<>();
        HashMap<String, LinkedList<String>> files = new HashMap<>();

        try (ZipInputStream zipInputStream = new ZipInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()))) {
            ZipEntry zipEntry = zipInputStream.getNextEntry();

            while (zipEntry != null) {
                paths.add(StringUtils.substringBeforeLast(zipEntry.getName(), "/"));
                String fileName = StringUtils.substringAfterLast(zipEntry.getName(), "/");

                if (files.containsKey(fileName)) {
                    LinkedList<String> strings = files.get(fileName);
                    strings.add(fileName);

                    files.put(fileName, strings);
                } else {
                    LinkedList<String> linkedList = new LinkedList<>();
                    linkedList.add(fileName);

                    files.put(fileName, linkedList);
                }

                zipInputStream.closeEntry();
                zipEntry = zipInputStream.getNextEntry();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        assertEquals(3, paths.size());

        assertTrue(paths.contains(SIP.getId() + "/KPW01169310/ALTO"));
        assertTrue(paths.contains(SIP.getId() + "/KPW01169310/TXT"));
        assertTrue(paths.contains(SIP.getId() + "/KPW01169310/userCopy"));

        assertEquals(4, files.size());

        assertEquals(1, files.get("desktop.ini").size());
        assertEquals(1, files.get("ALTO_KPW01169310_0001.XML").size());
        assertEquals(1, files.get("TXT_KPW01169310_0002.TXT").size());
        assertEquals(1, files.get("UC_KPW01169310_0002.JP2").size());
    }

    @Test
    public void getAipWithFilesReducedByRegex() throws Exception {
        Storage zfsStorage = new Storage();
        zfsStorage.setPriority(1);
        zfsStorage.setName("test ZFS");
        zfsStorage.setStorageType(StorageType.ZFS);
        zfsStorage.setConfig(STORAGE_CONFIG);
        zfsStorage.setReachable(true);
        zfsStorage.setHost("localhost");
        storageStore.save(zfsStorage);

        FsAdapter fsAdapter = mock(FsAdapter.class);
        when(fsAdapter.getStorage()).thenReturn(zfsStorage);
        when(storageProvider.createAdaptersForRead()).thenReturn(List.of(fsAdapter));
        LocalFsProcessor localFsProcessorMock = mock(LocalFsProcessor.class);
        when(localFsProcessorMock.getStorage()).thenReturn(zfsStorage);
        when(localFsProcessorMock.getAipDataFilePath(anyString(), any())).thenReturn(SIP_SOURCE_PATH);
        when(fsAdapter.getFsProcessor()).thenReturn(localFsProcessorMock);

        List<String> regexesForInclMode = new ArrayList<>();
        regexesForInclMode.add("KPW01169310/\\w+Sec.*");
        regexesForInclMode.add("KPW01169310/ALTO/.+\\.ini");
        regexesForInclMode.add("KPW01169310/info\\.xml");

        List<String> regexesForExclMode = new ArrayList<>();
        regexesForExclMode.add("KPW01169310/ALTO/.+\\.XML");
        regexesForExclMode.add("KPW01169310/masterCopy.*");
        regexesForExclMode.add("KPW01169310/TXT.*");
        regexesForExclMode.add("KPW01169310/userCopy.*");
        regexesForExclMode.add("KPW01169310/desktop.ini");
        regexesForExclMode.add("KPW01169310/KPW01169310.md5");
        regexesForExclMode.add("KPW01169310/METS_KPW01169310.xml");

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(0);
        DataReduction inclReduction = new DataReduction(regexesForInclMode, DataReductionMode.INCLUDE);
        aipService.streamAipReducedByRegexesFromLocalStorage(SIP.getId(), byteArrayOutputStream, inclReduction);
        assertGetAipWithFilesReducedByRegexOutput(byteArrayOutputStream);

        byteArrayOutputStream = new ByteArrayOutputStream(0);
        DataReduction exclReduction = new DataReduction(regexesForExclMode, DataReductionMode.EXCLUDE);
        aipService.streamAipReducedByRegexesFromLocalStorage(SIP.getId(), byteArrayOutputStream, exclReduction);
        assertGetAipWithFilesReducedByRegexOutput(byteArrayOutputStream);
    }

    private void assertGetAipWithFilesReducedByRegexOutput(ByteArrayOutputStream byteArrayOutputStream) {
        Set<String> paths = new LinkedHashSet<>();
        HashMap<String, LinkedList<String>> files = new HashMap<>();

        try (ZipInputStream zipInputStream = new ZipInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()))) {
            ZipEntry zipEntry = zipInputStream.getNextEntry();

            while (zipEntry != null) {
                paths.add(StringUtils.substringBeforeLast(zipEntry.getName(), "/"));
                String fileName = StringUtils.substringAfterLast(zipEntry.getName(), "/");

                if (files.containsKey(fileName)) {
                    LinkedList<String> strings = files.get(fileName);
                    strings.add(fileName);

                    files.put(fileName, strings);
                } else {
                    LinkedList<String> linkedList = new LinkedList<>();
                    linkedList.add(fileName);

                    files.put(fileName, linkedList);
                }

                zipInputStream.closeEntry();
                zipEntry = zipInputStream.getNextEntry();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        assertEquals(3, paths.size());

        assertTrue(paths.contains(SIP.getId() + "/KPW01169310/amdSec"));
        assertTrue(paths.contains(SIP.getId() + "/KPW01169310/ALTO"));
        assertTrue(paths.contains(SIP.getId() + "/KPW01169310"));

        assertEquals(10, files.size());

        assertEquals(1, files.get("info.xml").size());
        assertEquals(2, files.get("desktop.ini").size());
        assertEquals(1, files.get("AMD_METS_KPW01169310_0001.xml").size());
        assertEquals(1, files.get("AMD_METS_KPW01169310_0002.xml").size());
        assertEquals(1, files.get("AMD_METS_KPW01169310_0003.xml").size());
        assertEquals(1, files.get("AMD_METS_KPW01169310_0004.xml").size());
        assertEquals(1, files.get("AMD_METS_KPW01169310_0005.xml").size());
        assertEquals(1, files.get("AMD_METS_KPW01169310_0006.xml").size());
        assertEquals(1, files.get("AMD_METS_KPW01169310_0007.xml").size());
        assertEquals(1, files.get("AMD_METS_KPW01169310_0008.xml").size());
    }

    @Test
    public void getLatest() throws Exception {
        AipRetrievalResource aip = aipService.getAip(SIP_ID, false);

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
        Pair<Integer, ObjectRetrievalResource> xml = aipService.getXml(SIP_ID, null);
        assertThat(xml.getLeft(), is(2));
        try (InputStream inputStream = xml.getRight().getInputStream(); InputStream xml2Stream = xml2Stream()) {
            assertThat(inputStream, notNullValue());
            assertTrue(IOUtils.contentEquals(inputStream, xml2Stream));
        }
    }

    @Test
    public void getXmlVersionSpecified() throws Exception {
        Pair<Integer, ObjectRetrievalResource> xml = aipService.getXml(SIP_ID, 1);
        assertThat(xml.getLeft(), is(1));
        try (InputStream inputStream = xml.getRight().getInputStream(); InputStream xml1Stream = xml1Stream()) {
            assertThat(inputStream, notNullValue());
            assertTrue(IOUtils.contentEquals(inputStream, xml1Stream));
        }

        xml = aipService.getXml(SIP_ID, 2);
        assertThat(xml.getLeft(), is(2));
        try (InputStream inputStream = xml.getRight().getInputStream(); InputStream xml2Stream = xml2Stream()) {
            assertThat(inputStream, notNullValue());
            assertTrue(IOUtils.contentEquals(inputStream, xml2Stream));
        }
    }

    @Test
    public void getXmlNonExistentVersionSpecified() {
        assertThrown(() -> aipService.getXml(SIP_ID, 3)).isInstanceOf(MissingObject.class);
    }

    @Test
    public void getXmlIllegalStates() {
        XML2.setState(ObjectState.ROLLED_BACK);
        aipXmlStore.save(XML2);
        assertThrown(() -> aipService.getXml(SIP_ID, 2)).isInstanceOf(RollbackStateException.class);

        XML2.setState(ObjectState.PROCESSING);
        aipXmlStore.save(XML2);
        assertThrown(() -> aipService.getXml(SIP_ID, 2)).isInstanceOf(StillProcessingStateException.class);
    }

    @Test
    public void store() throws Exception {
        AipDto aipDto = new AipDto(USER_ID, SIP2_ID, sipStream(), SIP_CHECKSUM, xml1Stream(), XML1_CHECKSUM);
        aipService.saveAip(aipDto);

        ArgumentCaptor<byte[]> xmlBytesCaptor = ArgumentCaptor.forClass(byte[].class);

        ArgumentCaptor<TmpSourceHolder> sipHolderCaptor = ArgumentCaptor.forClass(TmpSourceHolder.class);
        ArgumentCaptor<TmpSourceHolder> xmlHolderCaptor = ArgumentCaptor.forClass(TmpSourceHolder.class);

        AipSip aipSip = archivalDbService.getAip(SIP2_ID);
        assertThat(aipSip, notNullValue());
        verify(async).saveAip(eq(aipDto), sipHolderCaptor.capture(), xmlHolderCaptor.capture(), anyList(), anyString());
        try (InputStream sipStream = sipHolderCaptor.getValue().createInputStream();
             InputStream xmlStream = xmlHolderCaptor.getValue().createInputStream()) {
            assertTrue(IOUtils.contentEquals(sipStream, sipStream()));
            assertTrue(IOUtils.contentEquals(xmlStream, xml1Stream()));
        }
    }

    @Test
    public void updateXml() throws Exception {
        Collection allXmls = aipXmlStore.findAll();
        assertThat(allXmls.size(), is(2));

        aipService.saveXml(SIP_ID, xml1Stream(), XML1_CHECKSUM, null, false);

        ArgumentCaptor<TmpSourceHolder> resourceHolderCaptor = ArgumentCaptor.forClass(TmpSourceHolder.class);

        allXmls = aipXmlStore.findAll();
        assertThat(allXmls.size(), is(3));
        AipXml newXml = aipXmlStore.findBySipAndVersion(SIP_ID, 3);
        ArchivalObjectDto xmlRef = newXml.toDto();
        verify(async).saveObject(eq(xmlRef), resourceHolderCaptor.capture(), anyList(), eq(false));
        try (
                InputStream xmlStream = resourceHolderCaptor.getValue().createInputStream()) {
            assertTrue(IOUtils.contentEquals(xmlStream, xml1Stream()));
        }
    }

    @Test
    public void getAipStateInfoAndRecoveryTest() throws Exception {
        AipConsistencyVerificationResultDto dto = new AipConsistencyVerificationResultDto(storage.getName(), storage.getStorageType(), storage.isReachable());
        ObjectConsistencyVerificationResultDto aipState = new ObjectConsistencyVerificationResultDto("aip", "aip", ObjectState.REMOVED, true, false, null, null, Instant.now());
        XmlConsistencyVerificationResultDto xml1state = new XmlConsistencyVerificationResultDto("xml1", "xml1", ObjectState.ARCHIVED, false, true, null, null, Instant.now(), 1);
        XmlConsistencyVerificationResultDto xml2state = new XmlConsistencyVerificationResultDto("xml2", "xml2", ObjectState.ROLLED_BACK, false, true, null, null, Instant.now(), 2);
        dto.setAipState(aipState);
        List<XmlConsistencyVerificationResultDto> xmlStates = new ArrayList<>();
        xmlStates.add(xml1state);
        xmlStates.add(xml2state);
        dto.setXmlStates(xmlStates);
        when(storageService.getAipInfo(any(), any(), any())).thenReturn(dto);
        doThrow(new IOStorageException(storage)).when(storageService).storeObject(any(), any(), any());

        AipSip aip = new AipSip("aip", null, user, ObjectState.REMOVED);
        AipXml xml1 = new AipXml("xml1", null, user, aip, 1, ObjectState.ARCHIVED);
        AipXml xml2 = new AipXml("xml2", null, user, aip, 2, ObjectState.ROLLED_BACK);
        AipXml xml3 = new AipXml("xml3", null, user, aip, 3, ObjectState.ARCHIVAL_FAILURE);
        aipSipStore.save(aip);
        aipXmlStore.save(asList(xml1, xml2, xml3));

        aipService.verifyAipsAtStorage(asList(aip), storage.getId());

        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        verify(storageService).getAipInfo(eq(aip.toDto()), captor.capture(), any());
        Map<Integer, ArchivalObjectDto> requestedXmls = captor.getValue();
        assertThat(requestedXmls.keySet(), containsInAnyOrder(1, 2));

        ArgumentCaptor<Map> recResCaptor = ArgumentCaptor.forClass(Map.class);
        Thread.sleep(500);
        verify(mailCenter).sendAipsVerificationError(recResCaptor.capture());
        Map<String, RecoveryResultDto> recRes = (Map<String, RecoveryResultDto>) recResCaptor.getValue();
        RecoveryResultDto storageRecRes = recRes.get(storage.getId());

        assertThat(storageRecRes.getContentInconsistencyObjectsIds().iterator().next(), is(xml1.getId()));
        assertThat(storageRecRes.getContentRecoveredObjectsIds(), hasSize(0));
        assertThat(storageRecRes.getMetadataInconsistencyObjectsIds().iterator().next(), is(aip.getId()));
        assertThat(storageRecRes.getMetadataRecoveredObjectsIds().iterator().next(), is(aip.getId()));
    }

    @Test
    public void getIllegalStateSipTest() {
        SIP.setState(ObjectState.DELETED);
        aipSipStore.save(SIP);
        assertThrown(() -> aipService.getAip(SIP_ID, true)).isInstanceOf(DeletedStateException.class);

        SIP.setState(ObjectState.ROLLED_BACK);
        aipSipStore.save(SIP);
        assertThrown(() -> aipService.getAip(SIP_ID, true)).isInstanceOf(RollbackStateException.class);

        SIP.setState(ObjectState.PROCESSING);
        aipSipStore.save(SIP);
        assertThrown(() -> aipService.getAip(SIP_ID, true)).isInstanceOf(StillProcessingStateException.class);

        XML1.setState(ObjectState.PROCESSING);
        aipXmlStore.save(XML1);
        assertThrown(() -> aipService.getAip(SIP_ID, true)).isInstanceOf(StillProcessingStateException.class);

        XML1.setState(ObjectState.ROLLED_BACK);
        SIP.setState(ObjectState.ARCHIVED);
        aipSipStore.save(SIP);
        aipXmlStore.save(XML1);
        aipXmlStore.delete(XML2);
        //findall does the trick so that service sees up-to-date records
        aipXmlStore.findAll();
        assertThrown(() -> aipService.getAip(SIP_ID, true)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void delete() throws Exception {
        archivalService.deleteObject(SIP_ID);

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
        verify(async).removeObject(eq(sip.toDto()), anyList(), anyString());
    }

    @Test
    public void getAipState() {
        ObjectState aipState = aipService.getAipState(SIP_ID);
        assertThat(aipState, is(ObjectState.ARCHIVED));

        SIP.setState(ObjectState.DELETED);
        aipSipStore.save(SIP);

        aipState = aipService.getAipState(SIP_ID);
        assertThat(aipState, is(ObjectState.DELETED));
    }

    @Test
    public void cleanUp() throws Exception {
        when(storageSyncStatusStore.anySynchronizing()).thenReturn(null);

        ArchivalObject o1 = new ArchivalObject(null, null, ObjectState.DELETED);
        ArchivalObject o2 = new ArchivalObject(null, null, ObjectState.DELETION_FAILURE);
        ArchivalObject o3 = new ArchivalObject(null, null, ObjectState.ROLLBACK_FAILURE);
        AipSip s1 = new AipSip(UUID.randomUUID().toString(), null, null, ObjectState.PROCESSING);
        AipSip s2 = new AipSip(UUID.randomUUID().toString(), null, null, ObjectState.ARCHIVED);
        AipXml x1 = new AipXml(UUID.randomUUID().toString(), null, null, s1, 1, ObjectState.ARCHIVAL_FAILURE);
        AipXml x2 = new AipXml(UUID.randomUUID().toString(), null, null, s1, 2, ObjectState.PRE_PROCESSING);
        objectStore.save(asList(o1, o2, o3, s1, s2, x1, x2));
        List<ArchivalObject> cleanup = systemAdministrationService.cleanup(true);
        assertThat(cleanup, containsInAnyOrder(o2, o3, s1, x1, x2));
        verify(async).cleanUp(cleanup, storageProvider.createAdaptersForWriteOperation());
        cleanup = systemAdministrationService.cleanup(false);
        assertThat(cleanup, containsInAnyOrder(o2, o3, x1));
        verify(async).cleanUp(cleanup, storageProvider.createAdaptersForWriteOperation());
    }
}