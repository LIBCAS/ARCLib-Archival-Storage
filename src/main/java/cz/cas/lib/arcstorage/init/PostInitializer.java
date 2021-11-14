package cz.cas.lib.arcstorage.init;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.datatype.hibernate5.Hibernate5Module;
import cz.cas.lib.arcstorage.domain.entity.DomainObject;
import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.domain.store.ArchivalObjectStore;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.service.IntervalJobService;
import cz.cas.lib.arcstorage.service.StorageProvider;
import cz.cas.lib.arcstorage.service.SystemAdministrationService;
import cz.cas.lib.arcstorage.service.SystemStateService;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.SynchronizationInProgressException;
import cz.cas.lib.arcstorage.util.ApplicationContextUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@Component
@Slf4j
public class PostInitializer implements ApplicationListener<ApplicationReadyEvent> {
    @Inject
    private ObjectMapper objectMapper;
    @Inject
    private ArcstorageMailCenter arcstorageMailCenter;
    @Inject
    private IntervalJobService intervalJobService;

    @Value("${env}")
    private String env;

    @Value("${arcstorage.cleanUpAtApplicationStart:false}")
    private boolean startUpCleanUp;
    @Inject
    private StorageProvider storageProvider;
    @Inject
    private SystemStateService systemStateService;
    @Inject
    private SystemAdministrationService systemAdministrationService;
    @Inject
    private ArchivalObjectStore archivalObjectStore;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent events) {
        objectMapper.registerModule(new Hibernate5Module());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.setDateFormat(new StdDateFormat());
        SystemState systemState = systemStateService.createDefaultIfNotExist();
        if (!env.equals("test")) {
            checkAttachedStorages(systemState);
            intervalJobService.scheduleReachabilityChecks(systemState.getReachabilityCheckIntervalInMinutes());
        }
        if (startUpCleanUp) {
            try {
                systemAdministrationService.cleanup(true);
            } catch (SomeLogicalStoragesNotReachableException | NoLogicalStorageAttachedException | IOException | SynchronizationInProgressException e) {
                throw new RuntimeException("Cant perform startup cleanup", e);
            }
        } else {
            Map<String, Pair<AtomicBoolean, Lock>> collect = archivalObjectStore.findProcessingObjects()
                    .stream()
                    .collect(Collectors.toMap(DomainObject::getId, o -> Pair.of(new AtomicBoolean(true), new ReentrantLock())));
            ApplicationContextUtils.getProcessingObjects().putAll(collect);
        }

    }

    private void checkAttachedStorages(SystemState systemState) {
        long storagesCount = storageProvider.getStoragesCount();
        int minStorageCount = systemState.getMinStorageCount();
        Pair<List<StorageService>, List<StorageService>> services = storageProvider.checkReachabilityOfAllStorages();
        if (!services.getRight().isEmpty() || storagesCount < minStorageCount)
            arcstorageMailCenter.sendInitialStoragesCheckWarning(storagesCount, minStorageCount, services.getRight());
    }
}

