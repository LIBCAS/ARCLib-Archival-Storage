package cz.cas.lib.arcstorage.jms;

import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.jms.context.StoragesContextRegistry;
import cz.cas.lib.arcstorage.service.StorageAdministrationService;
import cz.cas.lib.arcstorage.service.StorageProvider;
import cz.cas.lib.arcstorage.storage.StorageService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@Component
@Slf4j
@Order(2)
public class JmsInitializer implements ApplicationListener<ApplicationReadyEvent> {

    @Autowired
    private StorageProvider storageProvider;

    @Autowired
    private StorageAdministrationService storageAdministrationService;

    @Autowired
    private JmsQueueManager storageQueueManager;

    @Autowired
    private StoragesContextRegistry registry;

    @Override
    @Transactional
    public void onApplicationEvent(ApplicationReadyEvent event) {
        Pair<List<StorageService>, List<StorageService>> res = storageProvider.checkReachabilityOfAllStorages(true);
        String primaryStorageId = storageProvider.getPrimaryStorage() != null ? storageProvider.getPrimaryStorage().getId() : null;
        Stream.concat(res.getLeft().stream(), res.getRight().stream()).forEach(storageService -> {
            String storageId = storageService.getStorage().getId();
            registry.registerStorage(storageId, Objects.equals(storageId, primaryStorageId));
            storageQueueManager.startListener(storageId);
        });
    }
}
