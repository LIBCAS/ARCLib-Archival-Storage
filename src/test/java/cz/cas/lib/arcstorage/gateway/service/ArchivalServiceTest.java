package cz.cas.lib.arcstorage.gateway.service;

import cz.cas.lib.arcstorage.domain.*;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.gateway.dto.*;
import cz.cas.lib.arcstorage.gateway.exception.InvalidChecksumException;
import cz.cas.lib.arcstorage.gateway.exception.state.DeletedStateException;
import cz.cas.lib.arcstorage.gateway.exception.state.FailedStateException;
import cz.cas.lib.arcstorage.gateway.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.gateway.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.store.AipSipStore;
import cz.cas.lib.arcstorage.store.AipXmlStore;
import cz.cas.lib.arcstorage.store.StorageConfigStore;
import helper.DbTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static helper.ThrowableAssertion.assertThrown;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ArchivalServiceTest extends DbTest {

    private static final ArchivalService archivalService = new ArchivalService();
    private static final AipSipStore aipSipStore = new AipSipStore();
    private static final AipXmlStore aipXmlStore = new AipXmlStore();
    private static final StorageConfigStore storageConfigStore = new StorageConfigStore();
    private static final ArchivalDbService archivalDbService = new ArchivalDbService();


    private static final String SIP_ID = "SIPtestID";
    private static final String SIP2_ID = "testSipId";

    private static final String XML1_ID = "XML1testID";
    private static final String XML2_ID = "XML2testID";
    private static final String SIP_HASH = "SIPhash";
    private static final String XML1_HASH = "XML1hash";
    private static final String XML2_HASH = "XML2hash";
    private static final String STORAGE_CONFIG = "{\"adapterType\": \"S3\", \"userKey\": \"key\", \"userSecret\": \"secret\", \"region\": \"region\"}";

    private static final InputStream SIP_STREAM = new ByteArrayInputStream(SIP_ID.getBytes(StandardCharsets.UTF_8));
    private static final InputStream XML1_STREAM = new ByteArrayInputStream(XML1_ID.getBytes(StandardCharsets.UTF_8));
    private static final InputStream XML2_STREAM = new ByteArrayInputStream(XML2_ID.getBytes(StandardCharsets.UTF_8));

    @Mock
    private StorageProvider storageProvider;

    @Mock
    private StorageService storageService;

    @Mock
    private ArchivalAsyncService async;

    private AipSip sip;
    private AipXml xml1;
    private AipXml xml2;

    private Checksum sipHash;
    private Checksum xml1Hash;
    private Checksum xml2Hash;
    private StorageConfig storageConfig;

    @Before
    public void setup() throws StorageException {
        MockitoAnnotations.initMocks(this);

        initializeStores(aipSipStore, aipXmlStore, storageConfigStore);

        archivalDbService.setAipSipStore(aipSipStore);
        archivalDbService.setAipXmlStore(aipXmlStore);

        async.setArchivalDbService(archivalDbService);

        archivalService.setArchivalDbService(archivalDbService);
        archivalService.setAsyncService(async);
        archivalService.setStorageConfigStore(storageConfigStore);
        archivalService.setStorageProvider(storageProvider);

        sipHash = new Checksum();
        sipHash.setType(ChecksumType.MD5);
        sipHash.setHash(SIP_HASH);

        xml1Hash = new Checksum();
        xml1Hash.setType(ChecksumType.MD5);
        xml1Hash.setHash(XML1_HASH);

        xml2Hash = new Checksum();
        xml2Hash.setType(ChecksumType.MD5);
        xml2Hash.setHash(XML2_HASH);

        xml1 = new AipXml(XML1_ID, xml1Hash, null, 1, ObjectState.ARCHIVED);
        xml2 = new AipXml(XML2_ID, xml2Hash, null, 2, ObjectState.ARCHIVED);

        aipXmlStore.save(xml1);
        aipXmlStore.save(xml2);

        sip = new AipSip(SIP_ID, sipHash, ObjectState.ARCHIVED, xml1, xml2);
        aipSipStore.save(sip);

        xml1.setSip(sip);
        xml2.setSip(sip);

        aipXmlStore.save(xml1);
        aipXmlStore.save(xml2);

        storageConfig = new StorageConfig();
        storageConfig.setStorageType(StorageType.CEPH);
        storageConfig.setConfig(STORAGE_CONFIG);

        storageConfigStore.save(storageConfig);

        FileContentDto xml1FileContentDto = new FileContentDto(XML1_STREAM);
        FileContentDto xml2FileContentDto = new FileContentDto(XML2_STREAM);

        List<FileContentDto> fileRefs1 = new ArrayList<>();
        fileRefs1.add(new FileContentDto(SIP_STREAM));
        fileRefs1.add(xml1FileContentDto);

        List<FileContentDto> fileRefs2 = new ArrayList<>();
        fileRefs2.add(new FileContentDto(SIP_STREAM));
        fileRefs2.add(xml2FileContentDto);

        List<FileContentDto> fileRefs3 = new ArrayList<>();
        fileRefs3.add(new FileContentDto(SIP_STREAM));
        fileRefs3.add(xml1FileContentDto);
        fileRefs3.add(xml2FileContentDto);

        when(storageService.getAip(SIP_ID, 1)).thenReturn(fileRefs1);
        when(storageService.getAip(SIP_ID, 2)).thenReturn(fileRefs2);
        when(storageService.getAip(SIP_ID, 1, 2)).thenReturn(fileRefs3);

        when(storageProvider.createAdapter(anyObject())).thenReturn(storageService);

        when(storageService.getXml(SIP_ID, 1)).thenReturn(xml1FileContentDto);
        when(storageService.getXml(SIP_ID, 2)).thenReturn(xml2FileContentDto);
    }

    @Test
    @Ignore
    public void getAll() throws DeletedStateException, StillProcessingStateException, RollbackStateException, StorageException, FailedStateException {
        AipDto aip = archivalService.get(SIP_ID, Optional.of(true));

        assertThat(aip.getSip(), equalTo(new ArchivalObjectDto(SIP_ID, new FileContentDto(SIP_STREAM), sipHash)));

        List<XmlDto> xmls = aip.getXmls();
        assertThat(xmls, hasSize(2));

        XmlDto xmlRef1 = xmls.stream().filter(xml -> xml.getId().equals(XML1_ID)).collect(Collectors.toList()).get(0);
        assertThat(xmlRef1.getVersion(), is(1));
        assertThat(xmlRef1.getChecksum(), equalTo(xml1Hash));
        assertThat(xmlRef1.getInputStream(), equalTo(XML1_STREAM));

        XmlDto xmlRef2 = xmls.stream().filter(xml -> xml.getId().equals(XML2_ID)).collect(Collectors.toList()).get(0);
        assertThat(xmlRef2.getVersion(), is(2));
        assertThat(xmlRef2.getChecksum(), equalTo(xml2Hash));
        assertThat(xmlRef2.getInputStream(), equalTo(XML2_STREAM));
    }

    @Test
    @Ignore
    public void getLatest() throws
            RollbackStateException, DeletedStateException, StorageException, StillProcessingStateException, FailedStateException {
        AipDto aip = archivalService.get(SIP_ID, Optional.of(false));

        assertThat(aip.getSip(), equalTo(new ArchivalObjectDto(SIP_ID, new FileContentDto(SIP_STREAM), sipHash)));

        List<XmlDto> xmls = aip.getXmls();
        assertThat(xmls, hasSize(1));

        XmlDto xmlRef = xmls.stream().filter(xml -> xml.getId().equals(XML2_ID)).collect(Collectors.toList()).get(0);
        assertThat(xmlRef.getVersion(), is(2));
        assertThat(xmlRef.getChecksum(), equalTo(xml2Hash));
        assertThat(xmlRef.getInputStream(), equalTo(XML2_STREAM));
    }

    @Test
    @Ignore
    public void getXml() throws
            StorageException, FailedStateException, RollbackStateException, StillProcessingStateException {
        Optional<Integer> version = Optional.empty();
        XmlDto xml = archivalService.getXml(SIP_ID, version);
        assertThat(xml.getVersion(), is(2));
    }

    @Test
    @Ignore
    public void getXmlVersionSpecified() throws
            StorageException, FailedStateException, RollbackStateException, StillProcessingStateException {
        Optional<Integer> version1 = Optional.of(1);
        XmlDto xml = archivalService.getXml(SIP_ID, version1);
        assertThat(xml.getVersion(), is(1));

        Optional<Integer> version2 = Optional.of(2);
        XmlDto xml2 = archivalService.getXml(SIP_ID, version2);
        assertThat(xml2.getVersion(), is(2));
    }

    @Test
    public void getXmlNonExistentVersionSpecified() {
        Optional<Integer> version = Optional.of(3);
        assertThrown(() -> archivalService.getXml(SIP_ID, version)).isInstanceOf(MissingObject.class);
    }

    @Test
    public void getXmlIllegalStates() {
        Optional<Integer> version = Optional.of(2);

        xml2.setState(ObjectState.ROLLBACKED);
        aipXmlStore.save(xml2);
        assertThrown(() -> archivalService.getXml(SIP_ID, version)).isInstanceOf(RollbackStateException.class);

        xml2.setState(ObjectState.PROCESSING);
        aipXmlStore.save(xml2);
        assertThrown(() -> archivalService.getXml(SIP_ID, version)).isInstanceOf(StillProcessingStateException.class);
    }

    @Test
    public void store() throws InvalidChecksumException {
        AipDto aipDto = new AipDto(SIP2_ID, SIP_STREAM, sipHash, XML1_STREAM, xml1Hash);
        archivalService.store(aipDto);

        AipSip aipSip = archivalDbService.getAip(SIP2_ID);
        assertThat(aipSip, notNullValue());
        verify(async).store(eq(aipDto));
    }

    @Test
    public void updateXml() {
        Collection<AipXml> allXmls = aipXmlStore.findAll();
        assertThat(allXmls.size(), is(2));

        archivalService.updateXml(SIP_ID, XML1_STREAM, xml1Hash);

        allXmls = aipXmlStore.findAll();
        assertThat(allXmls.size(), is(3));

        AipXml latestAipXml = allXmls.stream()
                .sorted(Comparator.comparingInt(AipXml::getVersion).reversed())
                .findFirst()
                .get();

        XmlDto xmlRef = new XmlDto(latestAipXml.getId(), new FileContentDto(XML1_STREAM), xml1Hash, 1);
        verify(async).updateXml(eq(SIP_ID), eq(xmlRef));
    }

    @Test
    public void getAipInfo() throws StillProcessingStateException, StorageException {
        when(storageService.getAipInfo(anyObject(), anyObject(), anyObject(), anyObject())).thenReturn(
                new AipStateInfoDto("", StorageType.CEPH, ObjectState.ARCHIVED, anyObject()));

        sip.setState(ObjectState.ARCHIVED);
        aipSipStore.save(sip);

        flushCache();

        List<AipStateInfoDto> aipStateInfoDtos = archivalService.getAipState(SIP_ID);

        assertThat(aipStateInfoDtos.size(), is(1));

        AipStateInfoDto aipStateInfoDto = aipStateInfoDtos.get(0);
        assertThat(aipStateInfoDto.getStorageType(), is(StorageType.CEPH));
        assertThat(aipStateInfoDto.getObjectState(), is(ObjectState.ARCHIVED));
    }

    @Test
    public void getIllegalStateSipTest() {
        sip.setState(ObjectState.DELETED);
        aipSipStore.save(sip);
        assertThrown(() -> archivalService.get(SIP_ID, Optional.of(true))).isInstanceOf(DeletedStateException.class);

        sip.setState(ObjectState.ROLLBACKED);
        aipSipStore.save(sip);
        assertThrown(() -> archivalService.get(SIP_ID, Optional.of(true))).isInstanceOf(RollbackStateException.class);

        sip.setState(ObjectState.PROCESSING);
        aipSipStore.save(sip);
        assertThrown(() -> archivalService.get(SIP_ID, Optional.of(true))).isInstanceOf(StillProcessingStateException.class);

        AipXml xml = sip.getXml(0);
        xml.setState(ObjectState.PROCESSING);
        aipXmlStore.save(xml);
        assertThrown(() -> archivalService.get(SIP_ID, Optional.of(true))).isInstanceOf(StillProcessingStateException.class);

        xml = sip.getXml(0);
        xml.setState(ObjectState.ROLLBACKED);
        aipXmlStore.save(xml);

        AipSip aipSip = new AipSip(SIP2_ID, sipHash, ObjectState.ARCHIVED, xml);
        aipSipStore.save(aipSip);

        xml.setSip(aipSip);
        aipXmlStore.save(xml);

        assertThrown(() -> archivalService.get(SIP2_ID, Optional.of(true))).isInstanceOf(RollbackStateException.class);
    }

    @Test
    public void delete() throws
            StorageException, StillProcessingStateException, RollbackStateException, FailedStateException {
        archivalService.delete(SIP_ID);

        AipSip sip = archivalDbService.getAip(SIP_ID);
        assertThat(sip.getState(), is(ObjectState.PROCESSING));
        verify(async).delete(eq(sip.getId()));
    }

    @Test
    public void remove() throws RollbackStateException, DeletedStateException, StorageException,
            StillProcessingStateException, FailedStateException {
        archivalService.remove(SIP_ID);

        AipSip sip = archivalDbService.getAip(SIP_ID);
        assertThat(sip.getState(), is(ObjectState.REMOVED));
        verify(async).remove(eq(sip.getId()));
    }
}
