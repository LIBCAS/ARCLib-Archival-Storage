package cz.cas.lib.arcstorage.gateway.service;

import com.querydsl.jpa.impl.JPAQueryFactory;
import cz.cas.lib.arcstorage.domain.*;
import cz.cas.lib.arcstorage.exception.BadArgument;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.gateway.dto.Checksum;
import cz.cas.lib.arcstorage.gateway.exception.DeletedException;
import cz.cas.lib.arcstorage.gateway.exception.RollbackedException;
import cz.cas.lib.arcstorage.gateway.exception.StillProcessingException;
import cz.cas.lib.arcstorage.store.AipSipStore;
import cz.cas.lib.arcstorage.store.AipXmlStore;
import cz.cas.lib.arcstorage.store.Transactional;
import helper.DbTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static helper.ThrowableAssertion.assertThrown;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class ArchivalDbServiceTest extends DbTest {

    @Rule
    public TestName name = new TestName();

    private static final AipXmlStore xmlStore = new AipXmlStore();
    private static final AipSipStore sipStore = new AipSipStore();
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
        sipChecksum.setHash("sipMd5Checksum");

        aipXmlChecksum = new Checksum();
        aipXmlChecksum.setType(ChecksumType.MD5);
        aipXmlChecksum.setHash("aipXmlMd5Checksum");

        xmlStore.setEntityManager(getEm());
        xmlStore.setQueryFactory(new JPAQueryFactory(getEm()));

        sipStore.setEntityManager(getEm());
        sipStore.setQueryFactory(new JPAQueryFactory(getEm()));

        service.setAipSipStore(sipStore);
        service.setAipXmlStore(xmlStore);

        sip = new AipSip(SIP_ID, sipChecksum, AipState.ARCHIVED);
        sipStore.save(sip);
        xmlStore.save(new AipXml(XML1_ID, aipXmlChecksum, sip, 1, XmlState.ARCHIVED));
        xmlStore.save(new AipXml(XML2_ID, aipXmlChecksum, sip, 2, XmlState.ARCHIVED));
    }

    @After
    public void after() throws SQLException {
        clearDatabase();
    }

    @Test
    public void registerAipCreation() {
        String xmlId = service.registerAipCreation(name.getMethodName(), sipChecksum, aipXmlChecksum);
        assertThat(sipStore.find(name.getMethodName()).getState(), equalTo(AipState.PROCESSING));
        assertThat(xmlStore.find(xmlId).getState(), equalTo(XmlState.PROCESSING));
        assertThat(xmlStore.find(xmlId).getVersion(), is(1));
        assertThat(xmlStore.find(xmlId).getChecksum(), is(aipXmlChecksum));
        assertThat(sipStore.find(name.getMethodName()).getChecksum(), is(sipChecksum));
    }

    @Test
    public void finishAipCreation() {
        String xmlId = service.registerAipCreation(name.getMethodName(), sipChecksum, aipXmlChecksum);
        service.finishAipCreation(name.getMethodName(), xmlId);
        assertThat(sipStore.find(name.getMethodName()).getState(), equalTo(AipState.ARCHIVED));
        assertThat(xmlStore.find(xmlId).getState(), equalTo(XmlState.ARCHIVED));
    }

    @Test
    public void setSipFailed() {
        service.setSipFailed(SIP_ID, XML1_ID);
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(AipState.FAILED));
        assertThat(xmlStore.find(XML1_ID).getState(), equalTo(XmlState.FAILED));
    }

    @Test
    public void setXmlFailed() {
        service.setXmlFailed(XML1_ID);
        assertThat(xmlStore.find(XML1_ID).getState(), equalTo(XmlState.FAILED));
    }

    @Test
    public void registerXmlUpdate() {
        AipXml xmlEntity = service.registerXmlUpdate(SIP_ID, aipXmlChecksum);
        assertThat(xmlStore.find(xmlEntity.getId()).getState(), equalTo(XmlState.PROCESSING));
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(AipState.ARCHIVED));
    }

    @Test
    public void registerSipDeletion() throws StillProcessingException, RollbackedException {
        service.registerSipDeletion(SIP_ID);
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(AipState.PROCESSING));
    }

    @Test
    public void finishSipDeletion() throws StillProcessingException, RollbackedException {
        service.registerSipDeletion(SIP_ID);
        service.finishSipDeletion(SIP_ID);
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(AipState.DELETED));
    }

    @Test
    public void finishXmlProcess() {
        AipXml xml = xmlStore.find(XML1_ID);
        xml.setState(XmlState.PROCESSING);
        xmlStore.save(xml);
        service.finishXmlProcess(XML1_ID);
        assertThat(xmlStore.find(XML1_ID).getState(), not(equalTo(XmlState.PROCESSING)));
    }

    @Test
    @Transactional
    public void removeSip() throws DeletedException, StillProcessingException, RollbackedException {
        service.removeSip(SIP_ID);
        AipSip aip = service.getAip(SIP_ID);
        assertThat(aip.getState(), equalTo(AipState.REMOVED));
        assertThat(aip.getXmls(), hasSize(2));
    }

    @Test
    @Transactional
    public void getAip() {
        AipSip aip = service.getAip(SIP_ID);
        assertThat(aip.getState(), equalTo(AipState.ARCHIVED));
        assertThat(aip.getXmls(), hasSize(2));
    }

    @Test
    public void rollBackSip() {
        service.rollbackSip(SIP_ID, XML1_ID);
        AipSip aip = service.getAip(SIP_ID);
        AipXml xml = xmlStore.find(XML1_ID);
        assertThat(xml.getState(), equalTo(XmlState.ROLLBACKED));
        assertThat(aip.getState(), equalTo(AipState.ROLLBACKED));
    }

    @Test
    public void fillUnfinishedFilesLists() {
        List<AipSip> unfinishedSips = new ArrayList<>();
        List<AipXml> unfinishedXmls = new ArrayList<>();

        AipSip sip2 = new AipSip("sip2", sipChecksum, AipState.PROCESSING);
        AipSip sip3 = new AipSip("sip3", sipChecksum, AipState.FAILED);

        sipStore.save(sip2);
        sipStore.save(sip3);

        AipXml aipXml1 = new AipXml(aipXmlChecksum, sip2, 1, XmlState.FAILED);
        xmlStore.save(aipXml1);
        AipXml aipXml2 = new AipXml(aipXmlChecksum, sip2, 1, XmlState.PROCESSING);
        xmlStore.save(aipXml2);

        AipXml aipXm3 = new AipXml(aipXmlChecksum, sip3, 1, XmlState.FAILED);
        xmlStore.save(aipXm3);
        AipXml aipXml4 = new AipXml(aipXmlChecksum, sip3, 1, XmlState.PROCESSING);
        xmlStore.save(aipXml4);

        flushCache();

        service.fillUnfinishedFilesLists(unfinishedSips, unfinishedXmls);

        assertThat(unfinishedSips.size(), is(2));
        assertThat(unfinishedXmls.size(), is(2));
    }

    @Test
    public void rollBackUnfinished() {
        AipSip sip2 = new AipSip("sip2", sipChecksum, AipState.PROCESSING);
        AipSip sip3 = new AipSip("sip3", sipChecksum, AipState.FAILED);

        sipStore.save(sip2);
        sipStore.save(sip3);

        AipXml aipXml1 = new AipXml(aipXmlChecksum, sip2, 1, XmlState.FAILED);
        xmlStore.save(aipXml1);
        AipXml aipXml2 = new AipXml(aipXmlChecksum, sip3, 1, XmlState.PROCESSING);
        xmlStore.save(aipXml2);

        flushCache();

        service.rollbackUnfinishedFilesRecords();

        aipXml1 = xmlStore.find(aipXml1.getId());
        aipXml2 = xmlStore.find(aipXml2.getId());

        sip2 = service.getAip(sip2.getId());
        sip3 = service.getAip(sip3.getId());

        assertThat(aipXml1.getState(), is(XmlState.ROLLBACKED));
        assertThat(aipXml2.getState(), is(XmlState.ROLLBACKED));

        assertThat(sip2.getState(), is(AipState.ROLLBACKED));
        assertThat(sip3.getState(), is(AipState.ROLLBACKED));
    }

    @Test
    public void rollBackXml() {
        service.rollbackXml(XML1_ID);
        AipXml xml = xmlStore.find(XML1_ID);
        assertThat(xml.getState(), equalTo(XmlState.ROLLBACKED));
    }

    @Test
    public void notFoundTest() {
        assertThrown(() -> service.registerSipDeletion(S)).isInstanceOf(MissingObject.class);
        assertThrown(() -> service.registerXmlUpdate(XML1_ID, aipXmlChecksum)).isInstanceOf(MissingObject.class);
        assertThrown(() -> service.getAip(S)).isInstanceOf(MissingObject.class);
        assertThrown(() -> service.removeSip(S)).isInstanceOf(MissingObject.class);
    }

    @Test
    public void alreadyExistsTest() {
        assertThrown(() -> service.registerAipCreation(SIP_ID, sipChecksum, aipXmlChecksum)).isInstanceOf(ConflictObject.class);
    }

    @Test
    public void nullTest() {
        assertThrown(() -> service.registerAipCreation(null, sipChecksum, aipXmlChecksum)).isInstanceOf(BadArgument.class);
        assertThrown(() -> service.registerAipCreation(S, null, aipXmlChecksum)).isInstanceOf(BadArgument.class);
        assertThrown(() -> service.registerAipCreation("blah", sipChecksum, null)).isInstanceOf(BadArgument.class);
        assertThrown(() -> service.registerXmlUpdate(null, aipXmlChecksum)).isInstanceOf(BadArgument.class);
        assertThrown(() -> service.registerXmlUpdate(S, null)).isInstanceOf(BadArgument.class);
    }

    @Test
    public void illegalState() {
        AipSip sip = sipStore.find(SIP_ID);
        sip.setState(AipState.PROCESSING);
        sipStore.save(sip);
        assertThrown(() -> service.registerSipDeletion(SIP_ID)).isInstanceOf(StillProcessingException.class);
        assertThrown(() -> service.removeSip(SIP_ID)).isInstanceOf(StillProcessingException.class);
        sip.setState(AipState.ROLLBACKED);
        sipStore.save(sip);
        assertThrown(() -> service.registerSipDeletion(SIP_ID)).isInstanceOf(RollbackedException.class);
        assertThrown(() -> service.removeSip(SIP_ID)).isInstanceOf(RollbackedException.class);
        sip.setState(AipState.DELETED);
        sipStore.save(sip);
        assertThrown(() -> service.removeSip(SIP_ID)).isInstanceOf(DeletedException.class);
    }

    @Test
    public void findAndDeleteUnfinished() {
        AipSip sip1 = new AipSip("sip1", sipChecksum, AipState.PROCESSING);
        AipXml xml1 = new AipXml("xml1", aipXmlChecksum, sip1, 1, XmlState.PROCESSING);
        AipSip sip2 = new AipSip("sip2", sipChecksum, AipState.ARCHIVED);
        AipXml xml2 = new AipXml("xml2", aipXmlChecksum, sip2, 1, XmlState.ARCHIVED);
        AipXml xml3 = new AipXml("xml3", aipXmlChecksum, sip2, 2, XmlState.PROCESSING);
        sipStore.save(sip1);
        sipStore.save(sip2);
        xmlStore.save(xml1);
        xmlStore.save(xml2);
        xmlStore.save(xml3);

        service.rollbackUnfinishedFilesRecords();

        assertThat(sipStore.findUnfinishedSips(), empty());
        assertThat(xmlStore.findUnfinishedXmls(), empty());
        List<AipSip> sips = sipStore.findAll().stream().filter(sip -> sip.getState() != AipState.ROLLBACKED).collect(Collectors.toList());
        List<AipXml> xmls = xmlStore.findAll().stream().filter(xml -> xml.getState() != XmlState.ROLLBACKED).collect(Collectors.toList());
        assertThat(xmls, hasSize(3));
        assertThat(sips, hasSize(2));
    }
}
