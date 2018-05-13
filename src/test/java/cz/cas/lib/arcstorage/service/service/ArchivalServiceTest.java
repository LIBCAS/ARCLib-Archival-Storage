package cz.cas.lib.arcstorage.service.service;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.domain.entity.StorageConfig;
import cz.cas.lib.arcstorage.domain.store.AipSipStore;
import cz.cas.lib.arcstorage.domain.store.AipXmlStore;
import cz.cas.lib.arcstorage.domain.store.StorageConfigStore;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.service.ArchivalAsyncService;
import cz.cas.lib.arcstorage.service.ArchivalDbService;
import cz.cas.lib.arcstorage.service.ArchivalService;
import cz.cas.lib.arcstorage.service.StorageProvider;
import cz.cas.lib.arcstorage.service.exception.InvalidChecksumException;
import cz.cas.lib.arcstorage.service.exception.StorageNotReachableException;
import cz.cas.lib.arcstorage.service.exception.state.DeletedStateException;
import cz.cas.lib.arcstorage.service.exception.state.FailedStateException;
import cz.cas.lib.arcstorage.service.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.service.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import helper.DbTest;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static cz.cas.lib.arcstorage.storage.StorageUtils.extractXmlVersion;
import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
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
    private static final StorageConfigStore storageConfigStore = new StorageConfigStore();
    private static final ArchivalDbService archivalDbService = new ArchivalDbService();

    private static final String SIP_ID = "SIPtestID";
    private static final String SIP_MD5 = "101b295a91f771d96e1987ff501b034c";
    private static final Checksum SIP_CHECKSUM = new Checksum(ChecksumType.MD5, SIP_MD5);
    private static final AipSip SIP = new AipSip(SIP_ID, SIP_CHECKSUM, ObjectState.ARCHIVED);

    private static final String SIP2_ID = "testSipId";

    private static final String XML1_ID = "XML1testID";
    private static final String XML1_MD5 = "f09e5f27526a0ed7ec5b2d9d5c0b53cf";
    private static final Checksum XML1_CHECKSUM = new Checksum(ChecksumType.MD5, XML1_MD5);
    private static final AipXml XML1 = new AipXml(XML1_ID, XML1_CHECKSUM, null, 1, ObjectState.ARCHIVED);

    private static final String XML2_ID = "XML2testID";
    private static final String XML2_MD5 = "XML2hash";
    private static final Checksum XML2_CHECKSUM = new Checksum(ChecksumType.MD5, XML2_MD5);
    private static final AipXml XML2 = new AipXml(XML2_ID, XML2_CHECKSUM, null, 2, ObjectState.ARCHIVED);

    private static final String STORAGE_CONFIG = "{\"adapterType\": \"S3\", \"userKey\": \"key\", \"userSecret\": \"secret\", \"region\": \"region\"}";

    private static final Path tmpFolder = Paths.get("tmp");

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

    private StorageConfig storageConfig;

    @Before
    public void setup() throws StorageException, SQLException {
        clearDatabase();
        MockitoAnnotations.initMocks(this);

        initializeStores(aipSipStore, aipXmlStore, storageConfigStore);

        archivalDbService.setAipSipStore(aipSipStore);
        archivalDbService.setAipXmlStore(aipXmlStore);

        async.setArchivalDbService(archivalDbService);

        archivalService.setArchivalDbService(archivalDbService);
        archivalService.setAsyncService(async);
        archivalService.setStorageConfigStore(storageConfigStore);
        archivalService.setStorageProvider(storageProvider);
        archivalService.setTmpFolder(tmpFolder.toString());

        aipSipStore.save(SIP);
        XML1.setSip(SIP);
        XML2.setSip(SIP);
        aipXmlStore.save(XML1);
        aipXmlStore.save(XML2);

        storageConfig = new StorageConfig();
        storageConfig.setPriority(1);
        storageConfig.setName("test ceph storage");
        storageConfig.setStorageType(StorageType.CEPH);
        storageConfig.setConfig(STORAGE_CONFIG);
        storageConfigStore.save(storageConfig);
        when(storageService.getStorageConfig()).thenReturn(storageConfig);

        AipRetrievalResource aip1 = new AipRetrievalResource(null);
        aip1.setSip(sipStream());
        aip1.addXml(1, xml1Stream());

        AipRetrievalResource aip2 = new AipRetrievalResource(null);
        aip2.setSip(sipStream());
        aip2.addXml(2, xml1Stream());

        AipRetrievalResource aip3 = new AipRetrievalResource(null);
        aip3.setSip(sipStream());
        aip3.addXml(1, xml1Stream());
        aip3.addXml(2, xml2Stream());

        String xml1Id = toXmlId(SIP_ID, 1);
        String xml2Id = toXmlId(SIP_ID, 2);

        when(storageService.getAip(SIP_ID, 1)).thenReturn(aip1);
        when(storageService.getAip(SIP_ID, 2)).thenReturn(aip2);
        when(storageService.getAip(SIP_ID, 1, 2)).thenReturn(aip3);

        when(storageProvider.createAdapter(anyObject())).thenReturn(storageService);

        ObjectRetrievalResource xml1 = new ObjectRetrievalResource(xml1Stream(), null);
        ObjectRetrievalResource xml2 = new ObjectRetrievalResource(xml2Stream(), null);

        when(storageService.getObject(xml1Id)).thenReturn(xml1);
        when(storageService.getObject(xml2Id)).thenReturn(xml2);
    }

    @AfterClass
    public static void tearDown() throws IOException {
        FileUtils.cleanDirectory(tmpFolder.toFile());
    }

    @Test
    @Ignore
    public void getAll() throws Exception {
        AipRetrievalResource aip = archivalService.get(SIP_ID, Optional.of(true));

        assertTrue(IOUtils.contentEquals(aip.getSip(), sipStream()));

        Map<Integer, InputStream> xmls = aip.getXmls();
        assertThat(xmls.values(), hasSize(2));

        InputStream inputStream1 = aip.getXmls().get(2);
        assertThat(inputStream1, notNullValue());
        assertTrue(IOUtils.contentEquals(inputStream1, xml1Stream()));

        InputStream inputStream2 = aip.getXmls().get(2);
        assertThat(inputStream2, notNullValue());
        assertTrue(IOUtils.contentEquals(inputStream2, xml2Stream()));
    }

    @Test
    @Ignore
    public void getLatest() throws Exception {
        AipRetrievalResource aip = archivalService.get(SIP_ID, Optional.of(false));

        assertTrue(IOUtils.contentEquals(aip.getSip(), sipStream()));

        Map<Integer, InputStream> xmls = aip.getXmls();
        assertThat(xmls.values(), hasSize(1));

        InputStream inputStream = aip.getXmls().get(2);
        assertThat(inputStream, notNullValue());
        assertTrue(IOUtils.contentEquals(inputStream, xml2Stream()));
    }

    @Test
    @Ignore
    public void getXml() throws Exception {
        Optional<Integer> version = Optional.empty();
        ArchivalObjectDto xml = archivalService.getXml(SIP_ID, version);
        assertThat(extractXmlVersion(xml.getStorageId()), is(2));
    }

    @Test
    @Ignore
    public void getXmlVersionSpecified() throws Exception {
        Optional<Integer> version1 = Optional.of(1);
        ArchivalObjectDto xml = archivalService.getXml(SIP_ID, version1);
        assertThat(extractXmlVersion(xml.getStorageId()), is(1));

        Optional<Integer> version2 = Optional.of(2);
        ArchivalObjectDto xml2 = archivalService.getXml(SIP_ID, version2);
        assertThat(extractXmlVersion(xml.getStorageId()), is(2));
    }

    @Test
    public void getXmlNonExistentVersionSpecified() {
        Optional<Integer> version = Optional.of(3);
        assertThrown(() -> archivalService.getXml(SIP_ID, version)).isInstanceOf(MissingObject.class);
    }

    @Test
    public void getXmlIllegalStates() {
        Optional<Integer> version = Optional.of(2);

        XML2.setState(ObjectState.ROLLED_BACK);
        aipXmlStore.save(XML2);
        assertThrown(() -> archivalService.getXml(SIP_ID, version)).isInstanceOf(RollbackStateException.class);

        XML2.setState(ObjectState.PROCESSING);
        aipXmlStore.save(XML2);
        assertThrown(() -> archivalService.getXml(SIP_ID, version)).isInstanceOf(StillProcessingStateException.class);
    }

    @Test
    public void store() throws InvalidChecksumException, StorageNotReachableException, IOException {
        AipDto aipDto = new AipDto(SIP2_ID, sipStream(), SIP_CHECKSUM, xml1Stream(), XML1_CHECKSUM);
        archivalService.store(aipDto);

        ArgumentCaptor<byte[]> xmlBytesCaptor = ArgumentCaptor.forClass(byte[].class);

        AipSip aipSip = archivalDbService.getAip(SIP2_ID);
        assertThat(aipSip, notNullValue());
        verify(async).store(eq(aipDto), anyObject(), xmlBytesCaptor.capture(), anyList());
        String byteString = IOUtils.toString(xmlBytesCaptor.getValue());
        assertThat(byteString, containsString(IOUtils.toString(xml1Stream())));
    }

    @Test
    public void updateXml() throws StorageNotReachableException, IOException, InvalidChecksumException {
        Collection allXmls = aipXmlStore.findAll();
        assertThat(allXmls.size(), is(2));

        archivalService.updateXml(SIP_ID, xml1Stream(), XML1_CHECKSUM, Optional.empty());

        ArgumentCaptor<TmpSourceHolder> resourceHolderCaptor = ArgumentCaptor.forClass(TmpSourceHolder.class);

        allXmls = aipXmlStore.findAll();
        assertThat(allXmls.size(), is(3));
        AipXml newXml = aipXmlStore.findBySipAndVersion(SIP_ID, 3);

        ArchivalObjectDto xmlRef = new ArchivalObjectDto(newXml.getId(), toXmlId(SIP_ID, 3), null, XML1_CHECKSUM);
        verify(async).putObject(eq(xmlRef), eq(ObjectType.XML), resourceHolderCaptor.capture(), anyList());
        assertThat(resourceHolderCaptor.getValue(), instanceOf(ByteArrayHolder.class));
        assertTrue(IOUtils.contentEquals(resourceHolderCaptor.getValue().createInputStream(), xml1Stream()));
    }

    @Test
    public void getAipInfo() throws StillProcessingStateException, StorageException {
        when(storageService.getAipInfo(anyObject(), anyObject(), anyObject(), anyObject())).thenReturn(
                new AipStateInfoDto("", StorageType.CEPH, ObjectState.ARCHIVED, anyObject()));

        SIP.setState(ObjectState.ARCHIVED);
        aipSipStore.save(SIP);

        flushCache();

        List<AipStateInfoDto> aipStateInfoDtos = archivalService.getAipState(SIP_ID);

        assertThat(aipStateInfoDtos.size(), is(1));

        AipStateInfoDto aipStateInfoDto = aipStateInfoDtos.get(0);
        assertThat(aipStateInfoDto.getStorageType(), is(StorageType.CEPH));
        assertThat(aipStateInfoDto.getObjectState(), is(ObjectState.ARCHIVED));
    }

    @Test
    public void getIllegalStateSipTest() {
        SIP.setState(ObjectState.DELETED);
        aipSipStore.save(SIP);
        assertThrown(() -> archivalService.get(SIP_ID, Optional.of(true))).isInstanceOf(DeletedStateException.class);

        SIP.setState(ObjectState.ROLLED_BACK);
        aipSipStore.save(SIP);
        assertThrown(() -> archivalService.get(SIP_ID, Optional.of(true))).isInstanceOf(RollbackStateException.class);

        SIP.setState(ObjectState.PROCESSING);
        aipSipStore.save(SIP);
        assertThrown(() -> archivalService.get(SIP_ID, Optional.of(true))).isInstanceOf(StillProcessingStateException.class);

        XML1.setState(ObjectState.PROCESSING);
        aipXmlStore.save(XML1);
        assertThrown(() -> archivalService.get(SIP_ID, Optional.of(true))).isInstanceOf(StillProcessingStateException.class);

        XML1.setState(ObjectState.ROLLED_BACK);
        SIP.setState(ObjectState.ARCHIVED);
        aipSipStore.save(SIP);
        aipXmlStore.save(XML1);
        aipXmlStore.delete(XML2);
        //findall does the trick so that service sees up-to-date records
        aipXmlStore.findAll();
        assertThrown(() -> archivalService.get(SIP_ID, Optional.of(true))).isInstanceOf(RollbackStateException.class);
    }

    @Test
    public void delete() throws
            StorageException, StillProcessingStateException, RollbackStateException, FailedStateException, StorageNotReachableException {
        archivalService.delete(SIP_ID);

        AipSip sip = archivalDbService.getAip(SIP_ID);
        assertThat(sip.getState(), is(ObjectState.PROCESSING));
        verify(async).delete(eq(sip.getId()), anyList());
    }

    @Test
    public void remove() throws RollbackStateException, DeletedStateException, StorageException,
            StillProcessingStateException, FailedStateException, StorageNotReachableException {
        archivalService.remove(SIP_ID);

        AipSip sip = archivalDbService.getAip(SIP_ID);
        assertThat(sip.getState(), is(ObjectState.REMOVED));
        verify(async).remove(eq(sip.getId()), anyList());
    }
}
