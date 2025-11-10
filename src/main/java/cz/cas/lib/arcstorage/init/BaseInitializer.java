package cz.cas.lib.arcstorage.init;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.datatype.hibernate6.Hibernate6Module;
import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.domain.store.ArchivalObjectStore;
import cz.cas.lib.arcstorage.jms.JmsHealthCheckException;
import cz.cas.lib.arcstorage.jms.JmsSender;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.service.IntervalJobService;
import cz.cas.lib.arcstorage.service.StorageProvider;
import cz.cas.lib.arcstorage.service.SystemAdministrationService;
import cz.cas.lib.arcstorage.service.SystemStateService;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

@Component
@Slf4j
@Order(0)
public class BaseInitializer implements ApplicationListener<ApplicationReadyEvent> {
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private ArcstorageMailCenter arcstorageMailCenter;
    @Autowired
    private IntervalJobService intervalJobService;

    @Value("${arcstorage.cleanUpAtApplicationStart:false}")
    private boolean startUpCleanUp;
    @Autowired
    private StorageProvider storageProvider;
    @Autowired
    private SystemStateService systemStateService;
    @Autowired
    private SystemAdministrationService systemAdministrationService;
    @Autowired
    private ArchivalObjectStore archivalObjectStore;
    @Autowired
    private JmsSender jmsSender;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent events) {
        log.info("base initializer started");
        objectMapper.registerModule(new Hibernate6Module());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.enable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID);
        objectMapper.setDateFormat(new StdDateFormat());
        SystemState systemState = systemStateService.createDefaultIfNotExist();
        jmsSender.healthCheckWithoutSettingReadOnlyOnFailure();
        checkAttachedStorages(systemState);
        intervalJobService.scheduleReachabilityChecks(systemState.getReachabilityCheckIntervalInMinutes());

        if (startUpCleanUp) {
            try {
                systemAdministrationService.cleanup(true);
            } catch (SomeLogicalStoragesNotReachableException | NoLogicalStorageAttachedException | IOException |
                     JmsHealthCheckException e) {
                throw new RuntimeException("Cant perform startup cleanup", e);
            }
        }
        log.info("base initializer finished");
    }

    private void checkAttachedStorages(SystemState systemState) {
        int minStorageCount = systemState.getMinStorageCount();
        Pair<List<StorageService>, List<StorageService>> services = storageProvider.checkReachabilityOfAllStorages(false);
        long attachedReachableStorages = services.getLeft().stream().filter(s -> !s.getStorage().isDetached()).count();
        List<StorageService> unreachableOrDetachedStorages = Stream.concat(
                services.getLeft().stream().filter(s -> s.getStorage().isDetached()),
                services.getRight().stream()
        ).toList();
        if (!unreachableOrDetachedStorages.isEmpty() || attachedReachableStorages < minStorageCount) {
            arcstorageMailCenter.sendInitialStoragesCheckWarning(attachedReachableStorages, minStorageCount, unreachableOrDetachedStorages);
        }
    }
}

