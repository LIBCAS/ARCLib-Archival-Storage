package cz.cas.lib.arcstorage.storage;

import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.security.Role;
import cz.cas.lib.arcstorage.storage.exception.FileDoesNotExistException;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static cz.cas.lib.arcstorage.util.Utils.asList;
import static helper.ThrowableAssertion.assertThrown;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * parent class for test of logical storages: tests written in this class can run at any logical storage (ceph,fs ...)
 * <p>
 * there are also abstract tests (contract) which have to be implemented by extending class
 * </p>
 */
public abstract class StorageServiceTest {

    public static final String SIP_CONTENT = "blah";
    public static final Checksum SIP_CHECKSUM = new Checksum(ChecksumType.MD5, "6F1ED002AB5595859014EBF0951522D9");
    public static final String XML_CONTENT = "blik";
    public static final Checksum XML_CHECKSUM = new Checksum(ChecksumType.SHA512, "7EE090163B74E20DFEA30A7DD3CA969F75B1CCD713844F6B6ECD08F101AD04711C0D931BF372C32284BBF656CAC459AFC217C1F290808D0EB35AFFD569FF899C");
    public static final String XML_CONTENT_2 = "blob";
    public static final Checksum XML_CHECKSUM_2 = new Checksum(ChecksumType.MD5, "ee26908bf9629eeb4b37dac350f4754a");
    public static final String LARGE_SIP_PATH = "src/test/resources/8MiB+file";
    public static final Checksum LARGE_SIP_CHECKSUM = new Checksum(ChecksumType.MD5, "A95E65A3DE9704CB0C5B5C68AE41AE6F");
    public static final User USER = new User("ownerId", "username", "passwd", "dataSpace", Role.ROLE_READ_WRITE, "mail");

    @Rule
    public TestName testName = new TestName();

    public InputStream getSipStream() {
        return new ByteArrayInputStream(SIP_CONTENT.getBytes());
    }

    public InputStream getXmlStream() {
        return new ByteArrayInputStream(XML_CONTENT.getBytes());
    }

    public InputStream getXml2Stream() {
        return new ByteArrayInputStream(XML_CONTENT_2.getBytes());
    }

    public String streamToString(InputStream is) {
        try {
            return IOUtils.toString(is, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public abstract StorageService getService();

    public abstract String getDataSpace();

    public abstract void storeFileSuccessTest() throws Exception;

    public abstract void storeFileRollbackAware() throws Exception;

    public abstract void storeFileSettingRollback() throws Exception;

    public abstract void storeAipOk() throws Exception;

    public abstract void storeXmlOk() throws Exception;

    public abstract void removeSipMultipleTimesOk() throws Exception;

    public abstract void renewSipMultipleTimesOk() throws Exception;

    public abstract void deleteSipMultipleTimesOk() throws Exception;

    public abstract void rollbackProcessingFile() throws Exception;

    public abstract void rollbackStoredFileMultipleTimes() throws Exception;

    public abstract void rollbackCompletlyMissingFile() throws Exception;

    public abstract void rollbackAipOk() throws Exception;

    public abstract void rollbackXmlOk() throws Exception;

    public abstract void testConnection() throws Exception;

    @Test
    public void rollbackArchiveRollback() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        aip.getSip().setOwner(USER);
        aip.getXmls().forEach(x -> {
            x.setState(ObjectState.ROLLED_BACK);
            x.setOwner(USER);
        });
        AtomicBoolean rollback = new AtomicBoolean(false);
        getService().storeAip(aip, rollback, getDataSpace());
        getService().rollbackAip(aip, getDataSpace());
        aip.getSip().setState(ObjectState.ROLLED_BACK);

        getService().verifyStateOfObjects(asList(aip.getXmls(), aip.getSip()), new AtomicLong(0));
        aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        aip.getSip().setOwner(USER);
        aip.getXmls().forEach(x -> {
            x.setState(ObjectState.ROLLED_BACK);
            x.setOwner(USER);
        });
        getService().storeAip(aip, rollback, getDataSpace());
        aip.getSip().setState(ObjectState.ARCHIVED);
        aip.getXmls().forEach(x -> x.setState(ObjectState.ARCHIVED));
        getService().verifyStateOfObjects(asList(aip.getXmls(), aip.getSip()), new AtomicLong(0));
        getService().rollbackAip(aip, getDataSpace());
        aip.getSip().setState(ObjectState.ROLLED_BACK);
        aip.getXmls().forEach(x -> x.setState(ObjectState.ROLLED_BACK));
        getService().verifyStateOfObjects(asList(aip.getXmls(), aip.getSip()), new AtomicLong(0));

        getService().storeObject(aip.getXml(), rollback, getDataSpace());
        aip.getXml().setState(ObjectState.ARCHIVED);
        getService().verifyStateOfObjects(asList(aip.getXml()), new AtomicLong(0));
        getService().rollbackObject(aip.getXml(), getDataSpace());
        aip.getXml().setState(ObjectState.ROLLED_BACK);
        getService().verifyStateOfObjects(asList(aip.getXml()), new AtomicLong(0));
    }

    @Test
    public void getAipWithMoreXmlsOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        getService().storeAip(aip, rollback, getDataSpace());
        getService().storeObject(new ArchivalObjectDto(toXmlId(sipId, 2), "databaseId", XML_CHECKSUM_2, new User("ownerId"), getXml2Stream(), ObjectState.PROCESSING, Instant.now(), ObjectType.XML), rollback, getDataSpace());

        AipRetrievalResource aip1 = getService().getAip(sipId, getDataSpace(), 2, 1);
        assertThat(streamToString(aip1.getSip()), is(SIP_CONTENT));
        assertThat(streamToString(aip1.getXmls().get(2)), is("blob"));
        assertThat(streamToString(aip1.getXmls().get(1)), is(XML_CONTENT));
    }

    @Test
    public void getAipWithSpecificXmlOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        getService().storeAip(aip, rollback, getDataSpace());
        getService().storeObject(new ArchivalObjectDto(toXmlId(sipId, 99), "dbId", XML_CHECKSUM_2, new User("ownerId"), getXml2Stream(), ObjectState.PROCESSING, Instant.now(), ObjectType.XML), rollback, getDataSpace());

        AipRetrievalResource aip1 = getService().getAip(sipId, getDataSpace(), 99);
        assertThat(streamToString(aip1.getSip()), is(SIP_CONTENT));
        assertThat(streamToString(aip1.getXmls().get(99)), is("blob"));
    }

    @Test
    public void getAipMissing() throws Exception {
        String sipId = testName.getMethodName();
        assertThrown(() -> getService().getAip(sipId, getDataSpace())).isInstanceOf(FileDoesNotExistException.class);
    }

    @Test
    public void getAipMissingXml() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        getService().storeAip(aip, rollback, getDataSpace());
        assertThrown(() -> getService().getAip(sipId, getDataSpace(), 2)).isInstanceOf(FileDoesNotExistException.class);
    }

    @Test
    public void getXmlOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        getService().storeAip(aip, rollback, getDataSpace());

        ObjectRetrievalResource object = getService().getObject(toXmlId(sipId, 1), getDataSpace());
        assertThat(streamToString(object.getInputStream()), is(XML_CONTENT));
    }

    @Test
    public void getXmlMissing() throws Exception {
        String sipId = testName.getMethodName();
        assertThrown(() -> getService().getObject(toXmlId(sipId, 1), getDataSpace())).isInstanceOf(FileDoesNotExistException.class);
    }

    @Test
    public void storeAipSetsRollback() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), null, getXmlStream(), null);
        AtomicBoolean rollback = new AtomicBoolean(false);
        assertThrown(() -> getService().storeAip(aip, rollback, getDataSpace())).isInstanceOf(GeneralException.class);
        assertThat(rollback.get(), is(true));
    }

    @Test
    public void getAipInfoOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        getService().storeAip(aip, rollback, getDataSpace());
        ArchivalObjectDto xml2 = new ArchivalObjectDto(toXmlId(sipId, 2), "xmlId", new Checksum(ChecksumType.MD5, "ee26908bf9629eeb4b37dac350f4754a"), new User("ownerId"), new ByteArrayInputStream("blob".getBytes()), ObjectState.ROLLED_BACK, Instant.now(), ObjectType.XML);
        getService().storeObject(xml2, rollback, getDataSpace());
        aip.getSip().setState(ObjectState.REMOVED);
        aip.getXml().setState(ObjectState.ARCHIVED);

        Map<Integer, ArchivalObjectDto> map = new HashMap<>();
        map.put(1, aip.getXml());
        map.put(2, xml2);
        AipConsistencyVerificationResultDto aipInfo = getService().getAipInfo(aip.getSip(), map, getDataSpace());

        assertThat(aipInfo.getStorageType(), is(getService().getStorage().getStorageType()));
        assertThat(aipInfo.getStorageName(), is(getService().getStorage().getName()));

        assertThat(aipInfo.getAipState().getState(), is(ObjectState.REMOVED));
        assertThat(aipInfo.getAipState().getStorageChecksum(), is(SIP_CHECKSUM));
        assertThat(aipInfo.getAipState().getDatabaseChecksum(), is(SIP_CHECKSUM));
        assertThat(aipInfo.getAipState().isContentConsistent(), is(true));
        assertThat(aipInfo.getAipState().isMetadataConsistent(), is(false));
        assertThat(aipInfo.getAipState().getDatabaseId(), is(aip.getSip().getDatabaseId()));
        assertThat(aipInfo.getAipState().getStorageId(), is(aip.getSip().getStorageId()));
        assertThat(aipInfo.getAipState().getCreated(), is(aip.getSip().getCreated()));

        List<XmlConsistencyVerificationResultDto> xmlsStates = aipInfo.getXmlStates();
        assertThat(xmlsStates, hasSize(2));

        XmlConsistencyVerificationResultDto xmlInfo = xmlsStates.get(0);
        assertThat(xmlInfo.getStorageChecksum(), is(XML_CHECKSUM));
        assertThat(xmlInfo.getDatabaseChecksum(), is(XML_CHECKSUM));
        assertThat(xmlInfo.getVersion(), is(1));
        assertThat(xmlInfo.isContentConsistent(), is(true));
        assertThat(xmlInfo.isMetadataConsistent(), is(true));
        assertThat(xmlInfo.getStorageId(), is(aip.getXml().getStorageId()));
        assertThat(xmlInfo.getDatabaseId(), is(aip.getXml().getDatabaseId()));
        assertThat(xmlInfo.getCreated(), is(aip.getXml().getCreated()));


        XmlConsistencyVerificationResultDto xmlInfo2 = xmlsStates.get(1);
        assertThat(xmlInfo2.getStorageChecksum(), nullValue());
        assertThat(xmlInfo2.getDatabaseChecksum(), is( new Checksum(ChecksumType.MD5, "ee26908bf9629eeb4b37dac350f4754a")));
        assertThat(xmlInfo2.getVersion(), is(2));
        assertThat(xmlInfo2.isContentConsistent(), is(false));
        assertThat(xmlInfo2.isMetadataConsistent(), is(true));
        assertThat(xmlInfo2.getCreated(), is(xml2.getCreated()));
    }

    @Test
    public void getAipInfoMissingSip() throws Exception {
        String sipId = testName.getMethodName();
        ArchivalObjectDto sipDto = new ArchivalObjectDto(sipId, sipId, SIP_CHECKSUM, null, null, ObjectState.ARCHIVED, Instant.now(), ObjectType.SIP);
        assertThrown(() -> getService().getAipInfo(sipDto, new HashMap<>(), getDataSpace())).isInstanceOf(FileDoesNotExistException.class);
    }

    @Test
    public void getAipInfoMissingXml() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        getService().storeAip(aip, rollback, getDataSpace());
        ArchivalObjectDto nonExistentXml = new ArchivalObjectDto(toXmlId(sipId, 99), null, XML_CHECKSUM, null, null, ObjectState.ARCHIVED, Instant.now(), ObjectType.XML);
        Map<Integer, ArchivalObjectDto> map = new HashMap<>();
        map.put(99, nonExistentXml);
        assertThrown(() -> getService().getAipInfo(aip.getSip(), map, getDataSpace())).isInstanceOf(FileDoesNotExistException.class);
    }

    @Test
    public void getAipInfoDeletedSip() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        getService().storeAip(aip, rollback, getDataSpace());
        getService().delete(sipId, getDataSpace());
        aip.getSip().setState(ObjectState.DELETED);
        aip.getXml().setState(ObjectState.ARCHIVED);

        Map<Integer, ArchivalObjectDto> map = new HashMap<>();
        map.put(1, aip.getXml());
        AipConsistencyVerificationResultDto aipInfo = getService().getAipInfo(aip.getSip(), map, getDataSpace());

        assertThat(aipInfo.getAipState().getState(), is(ObjectState.DELETED));
        assertThat(aipInfo.getAipState().getStorageChecksum(), nullValue());
        assertThat(aipInfo.getAipState().getDatabaseChecksum(), is(SIP_CHECKSUM));
        assertThat(aipInfo.getAipState().isContentConsistent(), is(false));
        assertThat(aipInfo.getAipState().isMetadataConsistent(), is(true));
        assertThat(aipInfo.getAipState().getCreated(), is(aip.getSip().getCreated()));

        List<XmlConsistencyVerificationResultDto> xmlsStates = aipInfo.getXmlStates();
        assertThat(xmlsStates, hasSize(1));

        XmlConsistencyVerificationResultDto xmlInfo = xmlsStates.get(0);
        assertThat(xmlInfo.getStorageChecksum(), is(XML_CHECKSUM));
        assertThat(xmlInfo.getDatabaseChecksum(), is(XML_CHECKSUM));
        assertThat(xmlInfo.getVersion(), is(1));
        assertThat(xmlInfo.isContentConsistent(), is(true));
        assertThat(xmlInfo.getState(), is(ObjectState.ARCHIVED));
        assertThat(xmlInfo.isMetadataConsistent(), is(true));
        assertThat(xmlInfo.getCreated(), is(aip.getXml().getCreated()));
    }

    @Test
    public void verifyStateOfObjects() throws Exception {
        AtomicBoolean rollback = new AtomicBoolean(false);
        AtomicLong counter = new AtomicLong(0);
        User u = new User("ownerId");
        u.setDataSpace(getDataSpace());

        String object1Id = testName.getMethodName() + 1;
        String object2Id = testName.getMethodName() + 2;
        ArchivalObjectDto object1 = new ArchivalObjectDto(object1Id, object1Id, XML_CHECKSUM, u, getXmlStream(), ObjectState.ARCHIVED, Instant.now(), ObjectType.XML);
        ArchivalObjectDto object2 = new ArchivalObjectDto(object2Id, object1Id, XML_CHECKSUM, u, getXmlStream(), ObjectState.DELETED, Instant.now().plusSeconds(1), ObjectType.XML);
        getService().storeObject(object1, rollback, getDataSpace());
        getService().storeObject(object2, rollback, getDataSpace());

        ArchivalObjectDto failingDto = getService().verifyStateOfObjects(asList(object1, object2), counter);
        assertThat(failingDto, nullValue());

        object1.setState(ObjectState.ARCHIVAL_FAILURE);
        object2.setState(ObjectState.ARCHIVED);

        failingDto = getService().verifyStateOfObjects(asList(object1, object2), counter);
        assertThat(failingDto, is(object2));
    }

}
