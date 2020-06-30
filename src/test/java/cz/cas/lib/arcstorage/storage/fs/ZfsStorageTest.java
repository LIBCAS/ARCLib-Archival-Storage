package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ChecksumType;
import cz.cas.lib.arcstorage.dto.StorageStateDto;
import cz.cas.lib.arcstorage.dto.StorageType;
import net.schmizz.sshj.sftp.SFTPClient;
import org.apache.commons.io.IOUtils;
import org.junit.*;
import org.junit.rules.TestName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static cz.cas.lib.arcstorage.util.Utils.asSet;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class ZfsStorageTest {

    public static final String SIP_CONTENT = "blah";
    public static final Checksum SIP_CHECKSUM = new Checksum(ChecksumType.MD5, "6F1ED002AB5595859014EBF0951522D9");
    public static final String XML_CONTENT = "blik";
    public static final Checksum XML_CHECKSUM = new Checksum(ChecksumType.SHA512, "7EE090163B74E20DFEA30A7DD3CA969F75B1CCD713844F6B6ECD08F101AD04711C0D931BF372C32284BBF656CAC459AFC217C1F290808D0EB35AFFD569FF899C");
    public static final String XML_CONTENT_2 = "blob";
    public static final Checksum XML_CHECKSUM_2 = new Checksum(ChecksumType.MD5, "ee26908bf9629eeb4b37dac350f4754a");
    public static final String LARGE_SIP_PATH = "src/test/resources/8MiB+file";
    public static final Checksum LARGE_SIP_CHECKSUM = new Checksum(ChecksumType.MD5, "A95E65A3DE9704CB0C5B5C68AE41AE6F");

    private ZfsStorageService service;
    private static Storage storage = new Storage();
    private static String S = "/";
    private static final String KEY_PATH = "src/main/resources/arcstorage.ppk";
    private static final String USER = "arcstorage";
    private static SFTPClient sftp;
    private static String dataSpace;
    private static Properties props = new Properties();


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
        } catch(IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @BeforeClass
    public static void beforeClass() throws IOException {
        props = new Properties();
        props.load(ClassLoader.getSystemResourceAsStream("application.properties"));
        dataSpace = props.getProperty("test.sftp.dataspace");
        S = props.getProperty("test.sftp.separator");
        storage.setName("sftp storage");
        storage.setHost(props.getProperty("test.sftp.host"));
        storage.setPort(Integer.parseInt(props.getProperty("test.sftp.port")));
        storage.setStorageType(StorageType.ZFS);
    }

    @Before
    public void before() throws IOException {
        service = new ZfsStorageService(storage, props.getProperty("test.sftp.folderpath"), props.getProperty("test.sftp.poolname"), KEY_PATH,USER, 10000);
    }

    @Test
    public void getStorageStateTest() throws Exception {
        StorageStateDto storageState = service.getStorageState();
        assertThat(storageState.getStorageStateData().keySet(), hasSize(3));

        ZfsStorageService.DatasetInfoDto dataset = (ZfsStorageService.DatasetInfoDto) storageState.getStorageStateData().get("dataset");
        assertThat(dataset.getNAME(), not(isEmptyOrNullString()));
        assertThat(dataset.getAVAILABLE(), not(isEmptyOrNullString()));
        assertThat(dataset.getUSED(), not(isEmptyOrNullString()));
        Set<String> valueSet = asSet(dataset.getAVAILABLE(), dataset.getUSED(), dataset.getNAME());
        assertThat(valueSet, hasSize(3));

        Map<String, String> pool = (Map<String, String>) storageState.getStorageStateData().get("pool");
        assertThat(pool.keySet(), hasSize(10));
        for (String key : pool.keySet()) {
            assertThat(pool.get(key), not(isEmptyOrNullString()));
        }
        assertThat(pool.get("NAME"),not(isEmptyOrNullString()));
        assertThat(pool.get("HEALTH"),not(isEmptyOrNullString()));
        assertThat(pool.get("SIZE"),not(isEmptyOrNullString()));
        assertThat(pool.get("ALLOC"),not(isEmptyOrNullString()));
        assertThat(pool.get("FREE"),not(isEmptyOrNullString()));
        assertThat((Collection<String>) storageState.getStorageStateData().get("cmd: " + service.CMD_STATUS), hasSize(greaterThan(0)));
    }
}
