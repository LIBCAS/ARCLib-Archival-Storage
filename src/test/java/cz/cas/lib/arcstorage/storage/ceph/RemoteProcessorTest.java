package cz.cas.lib.arcstorage.storage.ceph;

import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.storage.fs.RemoteFsProcessor;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class RemoteProcessorTest {
    private RemoteFsProcessor service = new RemoteFsProcessor(config, "/", "src/main/resources/arcstorage.ppk");
    private static StorageConfig config = new StorageConfig();

    @BeforeClass
    public static void beforeClass() throws IOException {
        config.setName("local storage");
        config.setHost("192.168.10.60");
        config.setPort(22);
        config.setLocation("/arcpool/test");
    }

    @Test
    public void testConnection() {
        RemoteFsProcessor badService = new RemoteFsProcessor(config, "/", "src/main/resources/arcstorage.pub");
        assertThat(service.testConnection(), is(true));
        assertThat(badService.testConnection(), is(false));
    }

}
