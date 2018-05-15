package cz.cas.lib.arcstorage.service.service;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.domain.store.AipSipStore;
import cz.cas.lib.arcstorage.domain.store.AipXmlStore;
import cz.cas.lib.arcstorage.domain.store.ArchivalObjectStore;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ChecksumType;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.service.ArchivalDbService;
import cz.cas.lib.arcstorage.service.exception.state.DeletedStateException;
import cz.cas.lib.arcstorage.service.exception.state.FailedStateException;
import cz.cas.lib.arcstorage.service.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.service.exception.state.StillProcessingStateException;
import helper.DbTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static helper.ThrowableAssertion.assertThrown;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class ArchivalDbServiceTest extends DbTest {

    @Rule
    public TestName name = new TestName();

    private static final AipXmlStore xmlStore = new AipXmlStore();
    private static final AipSipStore sipStore = new AipSipStore();
    private static final ArchivalObjectStore archivalObjectStore = new ArchivalObjectStore();
    private static final ArchivalDbService service = new ArchivalDbService();

    private static final String SIP_ID = "SIPtestID";
    private static final String XML1_ID = "XML1testID";
    private static final String XML2_ID = "XML2testID";

    private static final String S = "somestring";

    private Checksum sipChecksum;
    private Checksum aipXmlChecksum;
    private AipSip sip;

    @Before
    public void before() {
        sipChecksum = new Checksum();
        sipChecksum.setType(ChecksumType.MD5);
        sipChecksum.setValue("sipMd5Checksum");

        aipXmlChecksum = new Checksum();
        aipXmlChecksum.setType(ChecksumType.MD5);
        aipXmlChecksum.setValue("aipXmlMd5Checksum");

        initializeStores(xmlStore, sipStore, archivalObjectStore);

        service.setAipSipStore(sipStore);
        service.setAipXmlStore(xmlStore);
        service.setArchivalObjectStore(archivalObjectStore);

        sip = new AipSip(SIP_ID, sipChecksum, ObjectState.ARCHIVED);
        sipStore.save(sip);
        xmlStore.save(new AipXml(XML1_ID, aipXmlChecksum, sip, 1, ObjectState.ARCHIVED));
        xmlStore.save(new AipXml(XML2_ID, aipXmlChecksum, sip, 2, ObjectState.ARCHIVED));
    }

    @After
    public void after() throws SQLException {
        clearDatabase();
    }

    @Test
    public void registerAipCreation() {
        String xmlId = toXmlId(name.getMethodName(), 1);
        service.registerAipCreation(name.getMethodName(), sipChecksum, xmlId, aipXmlChecksum);
        assertThat(sipStore.find(name.getMethodName()).getState(), equalTo(ObjectState.PROCESSING));
        assertThat(xmlStore.find(xmlId).getState(), equalTo(ObjectState.PROCESSING));
        assertThat(xmlStore.find(xmlId).getVersion(), is(1));
        assertThat(xmlStore.find(xmlId).getChecksum(), is(aipXmlChecksum));
        assertThat(sipStore.find(name.getMethodName()).getChecksum(), is(sipChecksum));
    }

    @Test
    public void finishAipCreation() {
        String xmlId = toXmlId(name.getMethodName(), 1);
        service.registerAipCreation(name.getMethodName(), sipChecksum, xmlId, aipXmlChecksum);
        service.finishAipCreation(name.getMethodName(), xmlId);
        assertThat(sipStore.find(name.getMethodName()).getState(), equalTo(ObjectState.ARCHIVED));
        assertThat(xmlStore.find(xmlId).getState(), equalTo(ObjectState.ARCHIVED));
    }

    @Test
    public void setSipFailed() {
        service.setAipFailed(SIP_ID, XML1_ID);
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(ObjectState.FAILED));
        assertThat(xmlStore.find(XML1_ID).getState(), equalTo(ObjectState.FAILED));
    }

    @Test
    public void setXmlFailed() {
        service.setObjectState(XML1_ID, ObjectType.XML, ObjectState.FAILED);
        assertThat(xmlStore.find(XML1_ID).getState(), equalTo(ObjectState.FAILED));
    }

    @Test
    public void registerXmlUpdate() {
        AipXml xmlEntity = service.registerXmlUpdate(SIP_ID, aipXmlChecksum, Optional.empty());
        assertThat(xmlStore.find(xmlEntity.getId()).getState(), equalTo(ObjectState.PROCESSING));
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(ObjectState.ARCHIVED));
        assertThat(xmlEntity.getVersion(), is(3));
    }

    @Test
    public void registerSipDeletion() throws StillProcessingStateException, RollbackStateException, FailedStateException {
        service.registerSipDeletion(SIP_ID);
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(ObjectState.PROCESSING));
    }

    @Test
    public void finishSipDeletion() throws StillProcessingStateException, RollbackStateException, FailedStateException {
        service.registerSipDeletion(SIP_ID);
        service.setObjectState(SIP_ID, ObjectType.SIP, ObjectState.DELETED);
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(ObjectState.DELETED));
    }

    @Test
    public void finishXmlProcess() {
        AipXml xml = xmlStore.find(XML1_ID);
        xml.setState(ObjectState.PROCESSING);
        xmlStore.save(xml);
        service.setObjectState(XML1_ID, ObjectType.XML, ObjectState.ARCHIVED);
        assertThat(xmlStore.find(XML1_ID).getState(), not(equalTo(ObjectState.PROCESSING)));
    }

    @Test
    @Transactional
    public void removeSip() throws DeletedStateException, StillProcessingStateException, RollbackStateException,
            FailedStateException {
        service.removeAip(SIP_ID);
        AipSip aip = service.getAip(SIP_ID);
        assertThat(aip.getState(), equalTo(ObjectState.REMOVED));
        assertThat(aip.getXmls(), hasSize(2));
    }

    @Test
    @Transactional
    public void getAip() {
        AipSip aip = service.getAip(SIP_ID);
        assertThat(aip.getState(), equalTo(ObjectState.ARCHIVED));
        assertThat(aip.getXmls(), hasSize(2));
    }

    @Test
    public void rollBackSip() {
        service.rollbackAip(SIP_ID, XML1_ID);
        AipSip aip = service.getAip(SIP_ID);
        AipXml xml = xmlStore.find(XML1_ID);
        assertThat(xml.getState(), equalTo(ObjectState.ROLLED_BACK));
        assertThat(aip.getState(), equalTo(ObjectState.ROLLED_BACK));
    }

    @Test
    public void fillUnfinishedFilesLists() {
        List<AipSip> unfinishedSips = new ArrayList<>();
        List<AipXml> unfinishedXmls = new ArrayList<>();

        AipSip sip2 = new AipSip("sip2", sipChecksum, ObjectState.PROCESSING);
        AipSip sip3 = new AipSip("sip3", sipChecksum, ObjectState.FAILED);

        sipStore.save(sip2);
        sipStore.save(sip3);

        AipXml aipXml1 = new AipXml("id1", aipXmlChecksum, sip2, 1, ObjectState.FAILED);
        xmlStore.save(aipXml1);
        AipXml aipXml2 = new AipXml("id2", aipXmlChecksum, sip2, 1, ObjectState.PROCESSING);
        xmlStore.save(aipXml2);

        AipXml aipXm3 = new AipXml("id3", aipXmlChecksum, sip3, 1, ObjectState.FAILED);
        xmlStore.save(aipXm3);
        AipXml aipXml4 = new AipXml("id4", aipXmlChecksum, sip3, 1, ObjectState.PROCESSING);
        xmlStore.save(aipXml4);

        flushCache();

        service.fillUnfinishedFilesLists(unfinishedSips, unfinishedXmls);

        assertThat(unfinishedSips.size(), is(2));
        assertThat(unfinishedXmls.size(), is(2));
    }

    @Test
    public void rollBackUnfinished() {
        AipSip sip2 = new AipSip("sip2", sipChecksum, ObjectState.PROCESSING);
        AipSip sip3 = new AipSip("sip3", sipChecksum, ObjectState.FAILED);

        sipStore.save(sip2);
        sipStore.save(sip3);

        String s2xmlId = toXmlId(sip2.getId(), 1);
        String s3xmlId = toXmlId(sip3.getId(), 1);
        AipXml aipXml1 = new AipXml(s2xmlId, aipXmlChecksum, sip2, 1, ObjectState.FAILED);
        xmlStore.save(aipXml1);
        AipXml aipXml2 = new AipXml(s3xmlId, aipXmlChecksum, sip3, 1, ObjectState.PROCESSING);
        xmlStore.save(aipXml2);

        flushCache();

        service.rollbackUnfinishedFilesRecords();

        aipXml1 = xmlStore.find(aipXml1.getId());
        aipXml2 = xmlStore.find(aipXml2.getId());

        sip2 = service.getAip(sip2.getId());
        sip3 = service.getAip(sip3.getId());

        assertThat(aipXml1.getState(), is(ObjectState.ROLLED_BACK));
        assertThat(aipXml2.getState(), is(ObjectState.ROLLED_BACK));

        assertThat(sip2.getState(), is(ObjectState.ROLLED_BACK));
        assertThat(sip3.getState(), is(ObjectState.ROLLED_BACK));
    }

    @Test
    public void rollBackXml() {
        service.setObjectState(XML1_ID, ObjectType.XML, ObjectState.ROLLED_BACK);
        AipXml xml = xmlStore.find(XML1_ID);
        assertThat(xml.getState(), equalTo(ObjectState.ROLLED_BACK));
    }

    @Test
    public void notFoundTest() {
        assertThrown(() -> service.registerSipDeletion(S)).isInstanceOf(MissingObject.class);
        assertThrown(() -> service.registerXmlUpdate(XML1_ID, aipXmlChecksum, Optional.empty())).isInstanceOf(MissingObject.class);
        assertThrown(() -> service.getAip(S)).isInstanceOf(MissingObject.class);
        assertThrown(() -> service.removeAip(S)).isInstanceOf(MissingObject.class);
    }

    @Test
    public void sipCanBeOverwritten() {
        service.setObjectState(SIP_ID, ObjectType.SIP, ObjectState.FAILED);
        service.registerAipCreation(SIP_ID, sipChecksum, XML1_ID, aipXmlChecksum);
        AipSip aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getState(), equalTo(ObjectState.PROCESSING));
    }

    @Test
    public void sipCantBeOverwritten() {
        assertThrown(() -> service.registerAipCreation(SIP_ID, sipChecksum, XML1_ID, aipXmlChecksum))
                .isInstanceOf(ConflictObject.class);
    }

    @Test
    public void xmlCantBeOverwritten() {
        assertThrown(() -> service.registerXmlUpdate(SIP_ID, aipXmlChecksum, Optional.of(1)))
                .isInstanceOf(ConflictObject.class);
    }

    @Test
    public void xmlCanBeOverwritten() {
        xmlStore.save(new AipXml(XML1_ID, aipXmlChecksum, sip, 3, ObjectState.FAILED));
        service.registerXmlUpdate(SIP_ID, aipXmlChecksum, Optional.of(3));
        AipXml retrieved = xmlStore.findBySipAndVersion(SIP_ID, 3);
        assertThat(retrieved.getState(), is(ObjectState.PROCESSING));
    }

    @Test
    public void illegalState() {
        AipSip sip = sipStore.find(SIP_ID);
        sip.setState(ObjectState.PROCESSING);
        sipStore.save(sip);
        assertThrown(() -> service.registerSipDeletion(SIP_ID)).isInstanceOf(StillProcessingStateException.class);
        assertThrown(() -> service.removeAip(SIP_ID)).isInstanceOf(StillProcessingStateException.class);
        sip.setState(ObjectState.ROLLED_BACK);
        sipStore.save(sip);
        assertThrown(() -> service.registerSipDeletion(SIP_ID)).isInstanceOf(RollbackStateException.class);
        assertThrown(() -> service.removeAip(SIP_ID)).isInstanceOf(RollbackStateException.class);
        sip.setState(ObjectState.DELETED);
        sipStore.save(sip);
        assertThrown(() -> service.removeAip(SIP_ID)).isInstanceOf(DeletedStateException.class);
    }

    @Test
    public void findAndDeleteUnfinished() {
        AipSip sip1 = new AipSip("sip1", sipChecksum, ObjectState.PROCESSING);
        AipXml xml1 = new AipXml("xml1", aipXmlChecksum, sip1, 1, ObjectState.PROCESSING);
        AipSip sip2 = new AipSip("sip2", sipChecksum, ObjectState.ARCHIVED);
        AipXml xml2 = new AipXml("xml2", aipXmlChecksum, sip2, 1, ObjectState.ARCHIVED);
        AipXml xml3 = new AipXml("xml3", aipXmlChecksum, sip2, 2, ObjectState.PROCESSING);
        sipStore.save(sip1);
        sipStore.save(sip2);
        xmlStore.save(xml1);
        xmlStore.save(xml2);
        xmlStore.save(xml3);

        service.rollbackUnfinishedFilesRecords();

        assertThat(sipStore.findUnfinishedSips(), empty());
        assertThat(xmlStore.findUnfinishedXmls(), empty());

        List<AipSip> sips = sipStore.findAll().stream().filter(sip -> sip.getState() != ObjectState.ROLLED_BACK).collect(Collectors.toList());
        List<AipXml> xmls = xmlStore.findAll().stream().filter(xml -> xml.getState() != ObjectState.ROLLED_BACK).collect(Collectors.toList());
        assertThat(xmls, hasSize(3));
        assertThat(sips, hasSize(2));
    }
}
