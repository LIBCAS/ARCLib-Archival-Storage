package cz.cas.lib.arcstorage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cas.lib.arcstorage.domain.entity.*;
import cz.cas.lib.arcstorage.domain.store.AipSipStore;
import cz.cas.lib.arcstorage.domain.store.AipXmlStore;
import cz.cas.lib.arcstorage.domain.store.ArchivalObjectStore;
import cz.cas.lib.arcstorage.domain.store.SystemStateStore;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ChecksumType;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.security.audit.AuditLogger;
import cz.cas.lib.arcstorage.security.user.UserDelegate;
import cz.cas.lib.arcstorage.security.user.UserStore;
import cz.cas.lib.arcstorage.service.exception.BadXmlVersionProvidedException;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.state.DeletedStateException;
import cz.cas.lib.arcstorage.service.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.service.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.storagesync.ObjectAuditStore;
import helper.DbTest;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.springframework.orm.jpa.JpaTransactionManager;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static cz.cas.lib.arcstorage.util.Utils.asList;
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
    private static final SystemStateStore SYSTEM_STATE_STORE = new SystemStateStore();
    private static final ObjectAuditStore objectAuditStore = new ObjectAuditStore();
    private static final AuditLogger auditLogger = new AuditLogger();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String SIP_ID = "SIPtestID";
    private static final String XML1_ID = "XML1testID";
    private static final String XML2_ID = "XML2testID";
    private static final String USER_ID = "dd23923a-923b-43b1-8a8e-3eebc7598432";

    private static final String S = "somestring";

    private Checksum sipChecksum;
    private Checksum aipXmlChecksum;
    private AipSip sip;
    private SystemState systemState;

    @Before
    public void before() {
        service.setTransactionTemplate(new JpaTransactionManager(getFactory()), 5);
        sipChecksum = new Checksum();
        sipChecksum.setType(ChecksumType.MD5);
        sipChecksum.setValue("sipMd5Checksum");

        aipXmlChecksum = new Checksum();
        aipXmlChecksum.setType(ChecksumType.MD5);
        aipXmlChecksum.setValue("aipXmlMd5Checksum");
        UserStore userStore = new UserStore();


        initializeStores(xmlStore, sipStore, archivalObjectStore, SYSTEM_STATE_STORE, objectAuditStore, userStore);
        auditLogger.setMapper(objectMapper);
        sipStore.setAuditLogger(auditLogger);
        xmlStore.setAuditLogger(auditLogger);
        archivalObjectStore.setAuditLogger(auditLogger);

        SystemStateService systemStateService = new SystemStateService();
        systemStateService.setSystemStateStore(SYSTEM_STATE_STORE);

        service.setAipSipStore(sipStore);
        service.setAipXmlStore(xmlStore);
        service.setArchivalObjectStore(archivalObjectStore);
        service.setObjectAuditStore(objectAuditStore);
        service.setSystemStateService(new SystemStateService());
        service.setSystemStateService(systemStateService);
        service.setUserDetails(new UserDelegate(new User(USER_ID)));
        service.setUserStore(userStore);

        userStore.save(new User(USER_ID));
        systemState = new SystemState(2, false);
        SYSTEM_STATE_STORE.save(systemState);
        sip = new AipSip(SIP_ID, sipChecksum, new User(USER_ID), ObjectState.ARCHIVED);
        sipStore.save(sip);
        xmlStore.save(new AipXml(XML1_ID, aipXmlChecksum, new User(USER_ID), sip, 1, ObjectState.ARCHIVED));
        xmlStore.save(new AipXml(XML2_ID, aipXmlChecksum, new User(USER_ID), sip, 2, ObjectState.ARCHIVED));
    }

    @After
    public void after() throws SQLException {
        clearDatabase();
    }

    //tests inheritance and querydsl
    @Test
    public void generalObjectStoreTest() {
        AipSip sip = new AipSip(UUID.randomUUID().toString(), null, null, ObjectState.PROCESSING);
        AipXml xml = new AipXml(UUID.randomUUID().toString(), null, null, sip, 1, ObjectState.PROCESSING);
        ArchivalObject object = new ArchivalObject(null, null, ObjectState.PROCESSING);
        sipStore.save(sip);
        xmlStore.save(xml);
        archivalObjectStore.save(object);
        List<ArchivalObject> objects = new ArrayList<>(archivalObjectStore.findAllInList(asList(sip.getId(), xml.getId(), object.getId())));
        //can obtain parent object as parent instances
        assertThat(objects, hasSize(3));
        assertThat(objects.get(0), instanceOf(AipSip.class));
        assertThat(objects.get(1), instanceOf(AipXml.class));
        assertThat(objects.get(2), instanceOf(ArchivalObject.class));

        //can change property of parent object
        objects.get(1).setState(ObjectState.DELETED);
        archivalObjectStore.save(objects.get(1));
        xml = xmlStore.find(xml.getId());
        assertThat(xml.getState(), is(ObjectState.DELETED));
        assertThat(xml.getVersion(), is(1));
    }

    @Test
    public void registerAipCreation() throws Exception {
        Pair<AipSip, Boolean> registrationResult = service.registerAipCreation(name.getMethodName(), sipChecksum, aipXmlChecksum, Instant.now());
        AipSip aipSip = registrationResult.getLeft();
        assertThat(registrationResult.getRight(), is(false));
        String xmlId = aipSip.getLatestXml().getId();

        assertThat(sipStore.find(name.getMethodName()).getState(), equalTo(ObjectState.PRE_PROCESSING));

        assertThat(xmlStore.find(xmlId).getState(), equalTo(ObjectState.PRE_PROCESSING));
        assertThat(xmlStore.find(xmlId).getVersion(), is(1));
        assertThat(xmlStore.find(xmlId).getChecksum(), is(aipXmlChecksum));
        assertThat(sipStore.find(name.getMethodName()).getChecksum(), is(sipChecksum));

        systemState.setReadOnly(true);
        SYSTEM_STATE_STORE.save(systemState);
        assertThrown(() -> service.registerAipCreation(UUID.randomUUID().toString(), sipChecksum, aipXmlChecksum, Instant.now())).isInstanceOf(ReadOnlyStateException.class);
    }

    @Test
    public void finishAipCreation() throws Exception {
        AipSip aipSip = service.registerAipCreation(name.getMethodName(), sipChecksum, aipXmlChecksum, Instant.now()).getLeft();
        String xmlId = aipSip.getLatestXml().getId();
        service.setObjectsState(ObjectState.ARCHIVED, name.getMethodName(), xmlId);
        assertThat(sipStore.find(name.getMethodName()).getState(), equalTo(ObjectState.ARCHIVED));
        assertThat(xmlStore.find(xmlId).getState(), equalTo(ObjectState.ARCHIVED));
    }

    @Test
    public void setSipFailed() {
        service.setObjectsState(ObjectState.ARCHIVAL_FAILURE, SIP_ID, XML1_ID);
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(ObjectState.ARCHIVAL_FAILURE));
        assertThat(xmlStore.find(XML1_ID).getState(), equalTo(ObjectState.ARCHIVAL_FAILURE));
    }

    @Test
    public void setXmlFailed() {
        service.setObjectsState(ObjectState.ARCHIVAL_FAILURE, XML1_ID);
        assertThat(xmlStore.find(XML1_ID).getState(), equalTo(ObjectState.ARCHIVAL_FAILURE));
    }

    @Test
    public void registerXmlUpdate() throws Exception {
        Pair<AipXml, Boolean> regResult = service.registerXmlUpdate(SIP_ID, aipXmlChecksum, null);
        AipXml xmlEntity = regResult.getLeft();
        assertThat(regResult.getRight(), is(false));
        assertThat(xmlStore.find(xmlEntity.getId()).getState(), equalTo(ObjectState.PRE_PROCESSING));
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(ObjectState.ARCHIVED));
        assertThat(xmlEntity.getVersion(), is(3));
        systemState.setReadOnly(true);
        SYSTEM_STATE_STORE.save(systemState);
        assertThrown(() -> service.registerXmlUpdate(SIP_ID, aipXmlChecksum, null)).isInstanceOf(ReadOnlyStateException.class);
    }

    @Test
    public void registerSipDeletion() throws Exception {
        service.deleteObject(SIP_ID);
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(ObjectState.DELETED));
        systemState.setReadOnly(true);
        SYSTEM_STATE_STORE.save(systemState);
        assertThrown(() -> service.deleteObject(SIP_ID)).isInstanceOf(ReadOnlyStateException.class);
    }

    @Test
    public void finishSipDeletion() throws Exception {
        service.deleteObject(SIP_ID);
        service.setObjectsState(ObjectState.DELETED, SIP_ID);
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(ObjectState.DELETED));
    }

    @Test
    public void finishXmlProcess() {
        AipXml xml = xmlStore.find(XML1_ID);
        xml.setState(ObjectState.PROCESSING);
        xmlStore.save(xml);
        service.setObjectsState(ObjectState.ARCHIVED, XML1_ID);
        assertThat(xmlStore.find(XML1_ID).getState(), not(equalTo(ObjectState.PROCESSING)));
    }

    @Test
    public void removeSip() throws Exception {
        service.removeObject(SIP_ID);
        AipSip aip = service.getAip(SIP_ID);
        assertThat(aip.getState(), equalTo(ObjectState.REMOVED));
        assertThat(aip.getXmls(), hasSize(2));
        systemState.setReadOnly(true);
        SYSTEM_STATE_STORE.save(systemState);
        assertThrown(() -> service.removeObject(SIP_ID)).isInstanceOf(ReadOnlyStateException.class);
    }

    @Test
    public void getAip() {
        AipSip aip = service.getAip(SIP_ID);
        assertThat(aip.getState(), equalTo(ObjectState.ARCHIVED));
        assertThat(aip.getXmls(), hasSize(2));
    }

    @Test
    public void rollBackSip() {
        service.setObjectsState(ObjectState.ROLLED_BACK, SIP_ID, XML1_ID);
        AipSip aip = service.getAip(SIP_ID);
        AipXml xml = xmlStore.find(XML1_ID);
        assertThat(xml.getState(), equalTo(ObjectState.ROLLED_BACK));
        assertThat(aip.getState(), equalTo(ObjectState.ROLLED_BACK));
    }

    @Test
    public void rollBackXml() {
        service.setObjectsState(ObjectState.ROLLED_BACK, XML1_ID);
        AipXml xml = xmlStore.find(XML1_ID);
        assertThat(xml.getState(), equalTo(ObjectState.ROLLED_BACK));
    }

    @Test
    public void notFoundTest() {
        assertThrown(() -> service.deleteObject(S)).isInstanceOf(MissingObject.class);
        assertThrown(() -> service.registerXmlUpdate(XML1_ID, aipXmlChecksum, null)).isInstanceOf(MissingObject.class);
        assertThrown(() -> service.getAip(S)).isInstanceOf(MissingObject.class);
        assertThrown(() -> service.removeObject(S)).isInstanceOf(MissingObject.class);
    }

    @Test
    public void sipCanBeOverwritten() throws Exception {
        service.setObjectsState(ObjectState.ARCHIVAL_FAILURE, SIP_ID);
        AipXml xml2 = new AipXml();
        xml2.setId(XML2_ID);
        xmlStore.delete(xml2);
        xmlStore.findAll();
        Pair<AipSip, Boolean> regResult = service.registerAipCreation(SIP_ID, sipChecksum, aipXmlChecksum, Instant.now());
        assertThat(regResult.getRight(), is(true));
        AipSip aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getState(), equalTo(ObjectState.PRE_PROCESSING));
    }

    @Test
    public void sipCantBeOverwritten() {
        assertThrown(() -> service.registerAipCreation(SIP_ID, sipChecksum, aipXmlChecksum, Instant.now()))
                .isInstanceOf(ConflictObject.class);
    }

    @Test
    public void xmlCantBeOverwritten() {
        assertThrown(() -> service.registerXmlUpdate(SIP_ID, aipXmlChecksum, 1))
                .isInstanceOf(BadXmlVersionProvidedException.class);
    }

    @Test
    public void xmlCanBeOverwritten() throws Exception {
        AipXml failedXml = xmlStore.save(new AipXml("xml3", aipXmlChecksum, new User(USER_ID), sip, 3, ObjectState.ARCHIVAL_FAILURE));
        Pair<AipXml, Boolean> regResult = service.registerXmlUpdate(SIP_ID, aipXmlChecksum, 3);
        AipXml successfulXml = regResult.getLeft();
        assertThat(regResult.getRight(), is(true));
        AipXml retrieved = xmlStore.findBySipAndVersion(SIP_ID, 3);
        assertThat(retrieved.getState(), is(ObjectState.PRE_PROCESSING));
        assertThat(retrieved, is(successfulXml));
        assertThat(retrieved, is(failedXml));
    }

    @Test
    public void illegalState() {
        AipSip sip = sipStore.find(SIP_ID);
        sip.setState(ObjectState.PROCESSING);
        sipStore.save(sip);
        assertThrown(() -> service.deleteObject(SIP_ID)).isInstanceOf(StillProcessingStateException.class);
        assertThrown(() -> service.removeObject(SIP_ID)).isInstanceOf(StillProcessingStateException.class);
        sip.setState(ObjectState.ROLLED_BACK);
        sipStore.save(sip);
        assertThrown(() -> service.deleteObject(SIP_ID)).isInstanceOf(RollbackStateException.class);
        assertThrown(() -> service.removeObject(SIP_ID)).isInstanceOf(RollbackStateException.class);
        sip.setState(ObjectState.DELETED);
        sipStore.save(sip);
        assertThrown(() -> service.removeObject(SIP_ID)).isInstanceOf(DeletedStateException.class);
    }

    @Test
    public void setObjectStates() throws Exception {
        clearDatabase();
        ArchivalObject o1 = new ArchivalObject(null, null, ObjectState.DELETED);
        AipSip s1 = new AipSip(UUID.randomUUID().toString(), null, null, ObjectState.PROCESSING);
        AipXml x1 = new AipXml(UUID.randomUUID().toString(), null, null, s1, 1, ObjectState.ARCHIVAL_FAILURE);
        List<ArchivalObject> entities = asList(o1, s1, x1);
        archivalObjectStore.save(entities);
        service.setObjectsState(ObjectState.ARCHIVED, entities.stream().map(ArchivalObject::getId).toArray(String[]::new));
        Collection<ArchivalObject> all = archivalObjectStore.findAll();
        assertThat(all, containsInAnyOrder(o1, s1, x1));
        for (ArchivalObject archivalObject : all) {
            assertThat(archivalObject.getState(), is(ObjectState.ARCHIVED));
        }
    }
}
