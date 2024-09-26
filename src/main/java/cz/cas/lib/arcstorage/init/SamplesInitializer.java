package cz.cas.lib.arcstorage.init;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.dto.StorageType;
import cz.cas.lib.arcstorage.security.Role;
import cz.cas.lib.arcstorage.security.user.UserStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.annotation.Order;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
@Order(2)
@ConditionalOnProperty(prefix = "arcstorage.init.samples", name = "enabled", havingValue = "true")
public class SamplesInitializer implements ApplicationListener<ApplicationReadyEvent> {

    private static final String SAMPLE_USER_ADMIN_ID = "e45955b0-000c-40e4-a125-98adb56adf6a";
    private static final String SAMPLE_USER_READ_ID = "3d687091-0150-4ab2-8d0d-99e0686db422";
    private static final String SAMPLE_USER_READ_WRITE_ID = "46724b85-a6f9-4f2a-99e4-22eeef8f6499";
    private static final String SAMPLE_STORAGE_ID = "62d33c09-24ff-4fa8-8169-e9dc77f7e9df";

    @Autowired
    private UserStore userStore;
    @Autowired
    private PasswordEncoder passwordEncoder;
    @Autowired
    private StorageStore storageStore;

    @Override
    @Transactional
    public void onApplicationEvent(ApplicationReadyEvent event) {
        log.info("samples initializer started");

        if (userStore.countAll() == 0) {
            User admin = new User(SAMPLE_USER_ADMIN_ID, "admin", passwordEncoder.encode("admin"), "sample", Role.ROLE_ADMIN, null);
            User read = new User(SAMPLE_USER_READ_ID, "read", passwordEncoder.encode("read"), "sample", Role.ROLE_READ, null);
            User readWrite = new User(SAMPLE_USER_READ_WRITE_ID, "readwrite", passwordEncoder.encode("readwrite"), "sample", Role.ROLE_READ_WRITE, null);
            userStore.save(List.of(admin, read, readWrite));

            Storage storage = new Storage(SAMPLE_STORAGE_ID);
            storage.setName("sample storage");
            storage.setStorageType(StorageType.FS);
            storage.setHost("localhost");
            storage.setConfig("{\"rootDirPath\":\"sample-storage-dir\"}");
            storageStore.save(storage);

            log.info("samples initializer finished");
        } else {
            log.info("samples initializer skipped since there are already some users present in DB");
        }
    }
}
