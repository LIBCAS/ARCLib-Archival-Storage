package cz.cas.lib.arcstorage.storage;

import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.dto.*;
import cz.cas.lib.arcstorage.exception.GeneralException;
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

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;
import static helper.ThrowableAssertion.assertThrown;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public abstract class StorageServiceTest {

    public static final String SIP_CONTENT = "blah";
    public static final Checksum SIP_CHECKSUM = new Checksum(ChecksumType.MD5, "6F1ED002AB5595859014EBF0951522D9");
    public static final String XML_CONTENT = "blik";
    public static final Checksum XML_CHECKSUM = new Checksum(ChecksumType.SHA512, "7EE090163B74E20DFEA30A7DD3CA969F75B1CCD713844F6B6ECD08F101AD04711C0D931BF372C32284BBF656CAC459AFC217C1F290808D0EB35AFFD569FF899C");
    public static final String XML_CONTENT_2 = "blob";
    public static final Checksum XML_CHECKSUM_2 = new Checksum(ChecksumType.MD5, "ee26908bf9629eeb4b37dac350f4754a");
    public static final String LARGE_SIP_PATH = "src/test/resources/8MiB+file";
    public static final Checksum LARGE_SIP_CHECKSUM = new Checksum(ChecksumType.MD5, "A95E65A3DE9704CB0C5B5C68AE41AE6F");


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
    public void getAipWithMoreXmlsOk() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        getService().storeAip(aip, rollback, getDataSpace());
        getService().storeObject(new ArchivalObjectDto(toXmlId(sipId, 2), "databaseId", XML_CHECKSUM_2, new User("ownerId"), getXml2Stream(), ObjectState.PROCESSING, Instant.now()), rollback, getDataSpace());

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
        getService().storeObject(new ArchivalObjectDto(toXmlId(sipId, 99), "dbId", XML_CHECKSUM_2, new User("ownerId"), getXml2Stream(), ObjectState.PROCESSING, Instant.now()), rollback, getDataSpace());

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
        getService().storeObject(new ArchivalObjectDto(toXmlId(sipId, 2), "xmlId", new Checksum(ChecksumType.MD5, "ee26908bf9629eeb4b37dac350f4754a"), new User("ownerId"), new ByteArrayInputStream("blob".getBytes()), ObjectState.PROCESSING, Instant.now()), rollback, getDataSpace());

        Map<Integer, Checksum> map = new HashMap<>();
        map.put(1, XML_CHECKSUM);
        map.put(2, SIP_CHECKSUM);
        AipStateInfoDto aipInfo = getService().getAipInfo(sipId, SIP_CHECKSUM, ObjectState.ARCHIVED, map, getDataSpace());

        assertThat(aipInfo.getObjectState(), is(ObjectState.ARCHIVED));
        assertThat(aipInfo.getSipStorageChecksum(), is(SIP_CHECKSUM));
        assertThat(aipInfo.getSipDatabaseChecksum(), is(SIP_CHECKSUM));
        assertThat(aipInfo.getStorageType(), is(getService().getStorage().getStorageType()));
        assertThat(aipInfo.getStorageName(), is(getService().getStorage().getName()));
        assertThat(aipInfo.isConsistent(), is(true));

        List<XmlStateInfoDto> xmlsStates = aipInfo.getXmlsState();
        assertThat(xmlsStates, hasSize(2));

        XmlStateInfoDto xmlInfo = xmlsStates.get(0);
        assertThat(xmlInfo.getStorageChecksum(), is(XML_CHECKSUM));
        assertThat(xmlInfo.getDatabaseChecksum(), is(XML_CHECKSUM));
        assertThat(xmlInfo.getVersion(), is(1));
        assertThat(xmlInfo.isConsistent(), is(true));

        XmlStateInfoDto xmlInfo2 = xmlsStates.get(1);
        assertThat(xmlInfo2.getStorageChecksum(), is(new Checksum(ChecksumType.MD5, "ee26908bf9629eeb4b37dac350f4754a")));
        assertThat(xmlInfo2.getDatabaseChecksum(), is(SIP_CHECKSUM));
        assertThat(xmlInfo2.getVersion(), is(2));
        assertThat(xmlInfo2.isConsistent(), is(false));
    }

    @Test
    public void getAipInfoMissingSip() throws Exception {
        String sipId = testName.getMethodName();
        assertThrown(() -> getService().getAipInfo(sipId, SIP_CHECKSUM, ObjectState.ARCHIVED, new HashMap<>(), getDataSpace())).isInstanceOf(FileDoesNotExistException.class);
    }

    @Test
    public void getAipInfoMissingXml() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        getService().storeAip(aip, rollback, getDataSpace());

        Map<Integer, Checksum> map = new HashMap<>();
        map.put(99, XML_CHECKSUM);
        assertThrown(() -> getService().getAipInfo(sipId, SIP_CHECKSUM, ObjectState.ARCHIVED, map, getDataSpace())).isInstanceOf(FileDoesNotExistException.class);
    }

    @Test
    public void getAipInfoDeletedSip() throws Exception {
        String sipId = testName.getMethodName();
        AipDto aip = new AipDto("ownerId", sipId, getSipStream(), SIP_CHECKSUM, getXmlStream(), XML_CHECKSUM);
        AtomicBoolean rollback = new AtomicBoolean(false);
        getService().storeAip(aip, rollback, getDataSpace());
        getService().delete(sipId, getDataSpace());

        Map<Integer, Checksum> map = new HashMap<>();
        map.put(1, XML_CHECKSUM);
        AipStateInfoDto aipInfo = getService().getAipInfo(sipId, SIP_CHECKSUM, ObjectState.DELETED, map, getDataSpace());

        assertThat(aipInfo.getObjectState(), is(ObjectState.DELETED));
        assertThat(aipInfo.getSipStorageChecksum(), nullValue());
        assertThat(aipInfo.getSipDatabaseChecksum(), is(SIP_CHECKSUM));
        assertThat(aipInfo.isConsistent(), is(false));

        List<XmlStateInfoDto> xmlsStates = aipInfo.getXmlsState();
        assertThat(xmlsStates, hasSize(1));

        XmlStateInfoDto xmlInfo = xmlsStates.get(0);
        assertThat(xmlInfo.getStorageChecksum(), is(XML_CHECKSUM));
        assertThat(xmlInfo.getDatabaseChecksum(), is(XML_CHECKSUM));
        assertThat(xmlInfo.getVersion(), is(1));
        assertThat(xmlInfo.isConsistent(), is(true));
    }
}
