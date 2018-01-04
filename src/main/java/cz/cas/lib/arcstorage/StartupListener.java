package cz.cas.lib.arcstorage;

import cz.cas.lib.arcstorage.gateway.storage.shared.StorageUtils;
import cz.cas.lib.arcstorage.store.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

@Component
@Slf4j
public class StartupListener implements ApplicationListener<ApplicationReadyEvent> {

    Environment env;

    @Override
    @Transactional
    public void onApplicationEvent(ApplicationReadyEvent event) {
        setUp();
    }

    public void setUp() {
        StorageUtils.keyFilePath = env.getProperty("arcstorage.auth-key");
    }

    @Inject
    public void setEnv(Environment env) {
        this.env = env;
    }
}
