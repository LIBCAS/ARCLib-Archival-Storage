package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.StorageConfig;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class LocalProcessorTest {
    private LocalFsProcessor service = new LocalFsProcessor(config);
    private static StorageConfig config = new StorageConfig();

    @BeforeClass
    public static void beforeClass() throws IOException {
        config.setName("local storage");
        config.setLocation(new File(".").getCanonicalPath());
    }

    @Test
    public void testConnection() {
        StorageConfig badConfig = new StorageConfig();
        badConfig.setName("bad storage");
        badConfig.setLocation("blah");
        LocalFsProcessor badService = new LocalFsProcessor(badConfig);
        assertThat(service.testConnection(), is(true));
        assertThat(badService.testConnection(), is(false));
    }
}
