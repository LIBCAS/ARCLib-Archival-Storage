package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.domain.entity.Configuration;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.store.ConfigurationStore;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.dto.StorageType;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import helper.ApiTest;
import helper.DbTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.io.IOException;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
public class StorageAdministrationApiTest extends DbTest implements ApiTest {

    private static final String STORAGE1_ID = "8f719ff7-8756-4101-9e87-42391ced37f1";
    private static final String STORAGE2_ID = "e6ea6043-dccc-4af3-ba4c-4d6f3d298e4b";
    private static final String STORAGE3_ID = "4f4e3c93-c22c-4ca0-9b50-6bcbe503ced7";
    private static final String BASE = "/api/administration";
    private static final String BASE_STORAGE = BASE + "/storage";
    private static final Configuration CONFIG = new Configuration(2,false);

    private StorageAdministrationApi api;

    @Mock
    private ConfigurationStore configurationStore;

    private StorageStore storageStore;

    @Before
    public void before() throws StorageException, IOException, NoLogicalStorageAttachedException {
        storageStore = new StorageStore();
        initializeStores(storageStore);
        when(configurationStore.get()).thenReturn(CONFIG);

        api = new StorageAdministrationApi();
        api.setConfigurationStore(configurationStore);
        api.setStorageStore(storageStore);

        saveStorage(STORAGE1_ID);
        saveStorage(STORAGE2_ID);
        saveStorage(STORAGE3_ID);
    }


    @Test
    public void deleteStorage() throws Exception {
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE_STORAGE + "/{id}", STORAGE1_ID))
                .andExpect(status().isOk());
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE_STORAGE + "/{id}", STORAGE1_ID))
                .andExpect(status().isNotFound());
        mvc(api)
                .perform(MockMvcRequestBuilders.delete(BASE_STORAGE + "/{id}", STORAGE2_ID))
                .andExpect(status().isForbidden());
    }

    private void saveStorage(String id) {
        Storage storage = new Storage("", "", 0, 0, StorageType.FS, "", "", false,true);
        storage.setId(id);
        storageStore.save(storage);
    }
}
