package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class LocalProcessorTest {
    private LocalFsProcessor service = new LocalFsProcessor(storage);
    private static Storage storage = new Storage();

    @BeforeClass
    public static void beforeClass() throws IOException {
        storage.setName("local storage");
        storage.setLocation(new File(".").getCanonicalPath());
    }

    @Test
    public void testConnection() {
        Storage badConfig = new Storage();
        badConfig.setName("bad storage");
        badConfig.setLocation("blah");
        LocalFsProcessor badService = new LocalFsProcessor(badConfig);
        assertThat(service.testConnection(), is(true));
        assertThat(badService.testConnection(), is(false));
    }
}
