package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class RemoteProcessorTest {
    private RemoteFsProcessor service = new RemoteFsProcessor(storage, "/", "src/main/resources/arcstorage.ppk");
    private static Storage storage = new Storage();

    @BeforeClass
    public static void beforeClass() throws IOException {
        storage.setName("local storage");
        storage.setHost("192.168.10.60");
        storage.setPort(22);
        storage.setLocation("/arcpool/test");
    }

    @Test
    public void testConnection() {
        RemoteFsProcessor badService = new RemoteFsProcessor(storage, "/", "src/main/resources/arcstorage.pub");
        assertThat(service.testConnection(), is(true));
        assertThat(badService.testConnection(), is(false));
    }

}
