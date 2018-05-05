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
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static cz.cas.lib.arcstorage.storage.StorageUtils.extractXmlVersion;
import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
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
        storageConfig.setPriority(1);
        storageConfig.setStorageType(StorageType.CEPH);
        storageConfig.setConfig(STORAGE_CONFIG);

        storageConfigStore.save(storageConfig);

        AipRetrievalResource aip1 = new AipRetrievalResource(null);
        aip1.setSip(SIP_STREAM);
        aip1.addXml(1, XML1_STREAM);

        AipRetrievalResource aip2 = new AipRetrievalResource(null);
        aip2.setSip(SIP_STREAM);
        aip2.addXml(2, XML2_STREAM);

        AipRetrievalResource aip3 = new AipRetrievalResource(null);
        aip3.setSip(SIP_STREAM);
        aip3.addXml(1, XML1_STREAM);
        aip3.addXml(2, XML2_STREAM);

        String xml1Id = toXmlId(SIP_ID, 1);
        String xml2Id = toXmlId(SIP_ID, 2);

        when(storageService.getAip(SIP_ID, 1)).thenReturn(aip1);
        when(storageService.getAip(SIP_ID, 2)).thenReturn(aip2);
        when(storageService.getAip(SIP_ID, 1, 2)).thenReturn(aip3);

        when(storageProvider.createAdapter(anyObject())).thenReturn(storageService);

        ObjectRetrievalResource xml1 = new ObjectRetrievalResource(XML1_STREAM, null);
        ObjectRetrievalResource xml2 = new ObjectRetrievalResource(XML2_STREAM, null);

        when(storageService.getObject(xml1Id)).thenReturn(xml1);
        when(storageService.getObject(xml2Id)).thenReturn(xml2);
    }

    @Test
    @Ignore
    public void getAll() throws DeletedStateException, StillProcessingStateException, RollbackStateException, StorageException, FailedStateException {
        AipRetrievalResource aip = archivalService.get(SIP_ID, Optional.of(true));

        assertThat(aip.getSip(), equalTo(SIP_STREAM));

        Map<Integer, InputStream> xmls = aip.getXmls();
        assertThat(xmls.values(), hasSize(2));

        InputStream inputStream1 = aip.getXmls().get(2);
        assertThat(inputStream1, notNullValue());
        assertThat(inputStream1, equalTo(XML1_STREAM));

        InputStream inputStream2 = aip.getXmls().get(2);
        assertThat(inputStream2, notNullValue());
        assertThat(inputStream2, equalTo(XML2_STREAM));
    }

    @Test
    @Ignore
    public void getLatest() throws
            RollbackStateException, DeletedStateException, StorageException, StillProcessingStateException, FailedStateException, IOException {
        AipRetrievalResource aip = archivalService.get(SIP_ID, Optional.of(false));

        assertThat(aip.getSip(), is(SIP_STREAM));

        Map<Integer, InputStream> xmls = aip.getXmls();
        assertThat(xmls.values(), hasSize(1));

        InputStream inputStream = aip.getXmls().get(2);
        assertThat(inputStream, notNullValue());
        assertThat(inputStream, equalTo(XML2_STREAM));
    }

    @Test
    @Ignore
    public void getXml() throws
            StorageException, FailedStateException, RollbackStateException, StillProcessingStateException {
        Optional<Integer> version = Optional.empty();
        ArchivalObjectDto xml = archivalService.getXml(SIP_ID, version);
        assertThat(extractXmlVersion(xml.getId()), is(2));
    }

    @Test
    @Ignore
    public void getXmlVersionSpecified() throws
            StorageException, FailedStateException, RollbackStateException, StillProcessingStateException {
        Optional<Integer> version1 = Optional.of(1);
        ArchivalObjectDto xml = archivalService.getXml(SIP_ID, version1);
        assertThat(extractXmlVersion(xml.getId()), is(1));

        Optional<Integer> version2 = Optional.of(2);
        ArchivalObjectDto xml2 = archivalService.getXml(SIP_ID, version2);
        assertThat(extractXmlVersion(xml.getId()), is(2));
    }

    @Test
    public void getXmlNonExistentVersionSpecified() {
        Optional<Integer> version = Optional.of(3);
        assertThrown(() -> archivalService.getXml(SIP_ID, version)).isInstanceOf(MissingObject.class);
    }

    @Test
    public void getXmlIllegalStates() {
        Optional<Integer> version = Optional.of(2);

        xml2.setState(ObjectState.ROLLED_BACK);
        aipXmlStore.save(xml2);
        assertThrown(() -> archivalService.getXml(SIP_ID, version)).isInstanceOf(RollbackStateException.class);

        xml2.setState(ObjectState.PROCESSING);
        aipXmlStore.save(xml2);
        assertThrown(() -> archivalService.getXml(SIP_ID, version)).isInstanceOf(StillProcessingStateException.class);
    }

    @Test
    public void store() throws InvalidChecksumException {
        AipDto aipDto = new AipDto(SIP2_ID, SIP_STREAM, sipHash, toXmlId(SIP2_ID, 1), XML1_STREAM, xml1Hash);
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

        ArchivalObjectDto xmlRef = new ArchivalObjectDto(toXmlId(SIP_ID, 3), XML1_STREAM, xml1Hash);
        verify(async).updateObject(eq(xmlRef));
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

        sip.setState(ObjectState.ROLLED_BACK);
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
        xml.setState(ObjectState.ROLLED_BACK);
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
