package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.*;
import cz.cas.lib.arcstorage.domain.store.*;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ChecksumType;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.security.user.UserDelegate;
import cz.cas.lib.arcstorage.security.user.UserStore;
import cz.cas.lib.arcstorage.service.exception.BadXmlVersionProvidedException;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.state.DeletedStateException;
import cz.cas.lib.arcstorage.service.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.service.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.storagesync.ObjectAuditStore;
import helper.DbTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.springframework.orm.jpa.JpaTransactionManager;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
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
    private static final ConfigurationStore configurationStore = new ConfigurationStore();
    private static final ObjectAuditStore objectAuditStore = new ObjectAuditStore();

    private static final String SIP_ID = "SIPtestID";
    private static final String XML1_ID = "XML1testID";
    private static final String XML2_ID = "XML2testID";
    private static final String USER_ID = "dd23923a-923b-43b1-8a8e-3eebc7598432";

    private static final String S = "somestring";

    private Checksum sipChecksum;
    private Checksum aipXmlChecksum;
    private AipSip sip;
    private Configuration configuration;

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

        initializeStores(xmlStore, sipStore, archivalObjectStore, configurationStore, objectAuditStore, userStore);

        service.setAipSipStore(sipStore);
        service.setAipXmlStore(xmlStore);
        service.setArchivalObjectStore(archivalObjectStore);
        service.setObjectAuditStore(objectAuditStore);
        service.setConfigurationStore(configurationStore);
        service.setUserDetails(new UserDelegate(new User(USER_ID)));
        service.setUserStore(userStore);

        userStore.save(new User(USER_ID));
        configuration = new Configuration(2, false);
        configurationStore.save(configuration);
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
        String xmlId = toXmlId(name.getMethodName(), 1);
        service.registerAipCreation(name.getMethodName(), sipChecksum, xmlId, aipXmlChecksum);
        assertThat(sipStore.find(name.getMethodName()).getState(), equalTo(ObjectState.PRE_PROCESSING));
        assertThat(xmlStore.find(xmlId).getState(), equalTo(ObjectState.PRE_PROCESSING));
        assertThat(xmlStore.find(xmlId).getVersion(), is(1));
        assertThat(xmlStore.find(xmlId).getChecksum(), is(aipXmlChecksum));
        assertThat(sipStore.find(name.getMethodName()).getChecksum(), is(sipChecksum));

        configuration.setReadOnly(true);
        configurationStore.save(configuration);
        assertThrown(() -> service.registerAipCreation(UUID.randomUUID().toString(), sipChecksum, xmlId, aipXmlChecksum)).isInstanceOf(ReadOnlyStateException.class);
    }

    @Test
    public void finishAipCreation() throws Exception {
        String xmlId = toXmlId(name.getMethodName(), 1);
        service.registerAipCreation(name.getMethodName(), sipChecksum, xmlId, aipXmlChecksum);
        service.finishAipCreation(name.getMethodName(), xmlId);
        assertThat(sipStore.find(name.getMethodName()).getState(), equalTo(ObjectState.ARCHIVED));
        assertThat(xmlStore.find(xmlId).getState(), equalTo(ObjectState.ARCHIVED));
    }

    @Test
    public void setSipFailed() {
        service.setAipFailed(SIP_ID, XML1_ID);
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(ObjectState.ARCHIVAL_FAILURE));
        assertThat(xmlStore.find(XML1_ID).getState(), equalTo(ObjectState.ARCHIVAL_FAILURE));
    }

    @Test
    public void setXmlFailed() {
        service.setObjectState(XML1_ID, ObjectState.ARCHIVAL_FAILURE);
        assertThat(xmlStore.find(XML1_ID).getState(), equalTo(ObjectState.ARCHIVAL_FAILURE));
    }

    @Test
    public void registerXmlUpdate() throws Exception {
        AipXml xmlEntity = service.registerXmlUpdate(SIP_ID, aipXmlChecksum, null);
        assertThat(xmlStore.find(xmlEntity.getId()).getState(), equalTo(ObjectState.PRE_PROCESSING));
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(ObjectState.ARCHIVED));
        assertThat(xmlEntity.getVersion(), is(3));
        configuration.setReadOnly(true);
        configurationStore.save(configuration);
        assertThrown(() -> service.registerXmlUpdate(SIP_ID, aipXmlChecksum, null)).isInstanceOf(ReadOnlyStateException.class);
    }

    @Test
    public void registerSipDeletion() throws Exception {
        service.registerObjectDeletion(SIP_ID);
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(ObjectState.DELETED));
        configuration.setReadOnly(true);
        configurationStore.save(configuration);
        assertThrown(() -> service.registerObjectDeletion(SIP_ID)).isInstanceOf(ReadOnlyStateException.class);
    }

    @Test
    public void finishSipDeletion() throws Exception {
        service.registerObjectDeletion(SIP_ID);
        service.setObjectState(SIP_ID, ObjectState.DELETED);
        assertThat(sipStore.find(SIP_ID).getState(), equalTo(ObjectState.DELETED));
    }

    @Test
    public void finishXmlProcess() {
        AipXml xml = xmlStore.find(XML1_ID);
        xml.setState(ObjectState.PROCESSING);
        xmlStore.save(xml);
        service.setObjectState(XML1_ID, ObjectState.ARCHIVED);
        assertThat(xmlStore.find(XML1_ID).getState(), not(equalTo(ObjectState.PROCESSING)));
    }

    @Test
    @Transactional
    public void removeSip() throws Exception {
        service.removeObject(SIP_ID);
        AipSip aip = service.getAip(SIP_ID);
        assertThat(aip.getState(), equalTo(ObjectState.REMOVED));
        assertThat(aip.getXmls(), hasSize(2));
        configuration.setReadOnly(true);
        configurationStore.save(configuration);
        assertThrown(() -> service.removeObject(SIP_ID)).isInstanceOf(ReadOnlyStateException.class);
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
    public void rollBackXml() {
        service.setObjectState(XML1_ID, ObjectState.ROLLED_BACK);
        AipXml xml = xmlStore.find(XML1_ID);
        assertThat(xml.getState(), equalTo(ObjectState.ROLLED_BACK));
    }

    @Test
    public void notFoundTest() {
        assertThrown(() -> service.registerObjectDeletion(S)).isInstanceOf(MissingObject.class);
        assertThrown(() -> service.registerXmlUpdate(XML1_ID, aipXmlChecksum, null)).isInstanceOf(MissingObject.class);
        assertThrown(() -> service.getAip(S)).isInstanceOf(MissingObject.class);
        assertThrown(() -> service.removeObject(S)).isInstanceOf(MissingObject.class);
    }

    @Test
    public void sipCanBeOverwritten() throws Exception {
        service.setObjectState(SIP_ID, ObjectState.ARCHIVAL_FAILURE);
        service.registerAipCreation(SIP_ID, sipChecksum, XML1_ID, aipXmlChecksum);
        AipSip aipSip = sipStore.find(SIP_ID);
        assertThat(aipSip.getState(), equalTo(ObjectState.PRE_PROCESSING));
    }

    @Test
    public void sipCantBeOverwritten() {
        assertThrown(() -> service.registerAipCreation(SIP_ID, sipChecksum, XML1_ID, aipXmlChecksum))
                .isInstanceOf(ConflictObject.class);
    }

    @Test
    public void xmlCantBeOverwritten() {
        assertThrown(() -> service.registerXmlUpdate(SIP_ID, aipXmlChecksum, 1))
                .isInstanceOf(BadXmlVersionProvidedException.class);
    }

    @Test
    public void xmlCanBeOverwritten() throws Exception {
        AipXml failedXml = xmlStore.save(new AipXml(XML1_ID, aipXmlChecksum, new User(USER_ID), sip, 3, ObjectState.ARCHIVAL_FAILURE));
        AipXml successfulXml = service.registerXmlUpdate(SIP_ID, aipXmlChecksum, 3);
        List<AipXml> retrieved = xmlStore.findBySipAndVersion(SIP_ID, 3);
        assertThat(retrieved, containsInAnyOrder(failedXml, successfulXml));
    }

    @Test
    public void illegalState() {
        AipSip sip = sipStore.find(SIP_ID);
        sip.setState(ObjectState.PROCESSING);
        sipStore.save(sip);
        assertThrown(() -> service.registerObjectDeletion(SIP_ID)).isInstanceOf(StillProcessingStateException.class);
        assertThrown(() -> service.removeObject(SIP_ID)).isInstanceOf(StillProcessingStateException.class);
        sip.setState(ObjectState.ROLLED_BACK);
        sipStore.save(sip);
        assertThrown(() -> service.registerObjectDeletion(SIP_ID)).isInstanceOf(RollbackStateException.class);
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
        service.setObjectsState(ObjectState.ARCHIVED, entities.stream().map(ArchivalObject::getId).collect(Collectors.toList()));
        Collection<ArchivalObject> all = archivalObjectStore.findAll();
        assertThat(all, containsInAnyOrder(o1, s1, x1));
        for (ArchivalObject archivalObject : all) {
            assertThat(archivalObject.getState(), is(ObjectState.ARCHIVED));
        }
    }
}
