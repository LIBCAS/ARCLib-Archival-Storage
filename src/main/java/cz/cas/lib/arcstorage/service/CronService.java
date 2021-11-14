package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.domain.store.AipSipStore;
import cz.cas.lib.arcstorage.dto.StorageStateDto;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.SynchronizationInProgressException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.transaction.support.TransactionTemplate;

import javax.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Configuration
@Slf4j
public class CronService implements SchedulingConfigurer {

    private String consistencyCheckCron;
    private Integer consistencyCheckCount;
    private String storageStateCheckCron;

    private SystemStateService systemStateService;
    private AipSipStore aipSipStore;
    private AipService aipService;
    private TransactionTemplate transactionTemplate;
    private StorageAdministrationService storageAdministrationService;
    private ArcstorageMailCenter arcstorageMailCenter;

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.addTriggerTask(
                () -> {
                    try {
                        aipsVerification();
                    } catch (Exception e) {
                        log.error("Periodic AIPS Concistency Verification job failed", e);
                    }
                },
                new CronTrigger(consistencyCheckCron));
        taskRegistrar.addTriggerTask(this::systemStateCheck, new CronTrigger(storageStateCheckCron));
    }

    private void aipsVerification() throws NoLogicalStorageAttachedException, NoLogicalStorageReachableException, SomeLogicalStoragesNotReachableException, SynchronizationInProgressException {
        SystemState systemState = systemStateService.get();
        Instant lastVerifiedObjectCreation = systemState.getLastVerifiedObjectCreation();
        List<AipSip> aipsToCheck = aipSipStore.findAfter(lastVerifiedObjectCreation, consistencyCheckCount);
        if (aipsToCheck.size() == 0) {
            aipsToCheck = aipSipStore.findAfter(null, consistencyCheckCount);
            if (aipsToCheck.size() == 0)
                return;
        }
        aipService.verifyAipsAtStorage(aipsToCheck, null);
        AipSip lastCheckedAip = aipsToCheck.get(aipsToCheck.size() - 1);
        transactionTemplate.execute(s -> {
            SystemState currentState = systemStateService.get();
            currentState.setLastVerifiedObjectCreation(lastCheckedAip.getCreated());
            systemStateService.save(currentState);
            return null;
        });
    }

    private void systemStateCheck() {
        Collection<Storage> all = storageAdministrationService.getAll();
        List<StorageStateDto> states = new ArrayList<>();
        for (Storage storage : all) {
            states.add(storageAdministrationService.getStorageState(storage.getId()));
        }
        arcstorageMailCenter.sendStorageStateReport(states);
    }

    @Inject
    public void setAipSipStore(AipSipStore aipSipStore) {
        this.aipSipStore = aipSipStore;
    }

    @Inject
    public void setAipService(AipService aipService) {
        this.aipService = aipService;
    }

    @Inject
    public void setTransactionTemplate(TransactionTemplate transactionTemplate) {
        this.transactionTemplate = transactionTemplate;
    }

    @Inject
    public void setSystemStateService(SystemStateService systemStateService) {
        this.systemStateService = systemStateService;
    }

    @Inject
    public void setStorageAdministrationService(StorageAdministrationService storageAdministrationService) {
        this.storageAdministrationService = storageAdministrationService;
    }

    @Inject
    public void setArcstorageMailCenter(ArcstorageMailCenter arcstorageMailCenter) {
        this.arcstorageMailCenter = arcstorageMailCenter;
    }

    @Inject
    public void setConsistencyCheckCron(@Value("${arcstorage.consistencyCheck.cron}") String consistencyCheckCron) {
        this.consistencyCheckCron = consistencyCheckCron;
    }

    @Inject
    public void setStorageStateCheckCron(@Value("${arcstorage.storageStateCheck.cron}") String storageStateCheckCron) {
        this.storageStateCheckCron = storageStateCheckCron;
    }

    @Inject
    public void setConsistencyCheckCount(@Value("${arcstorage.consistencyCheck.count:#{null}}") Integer consistencyCheckCount) {
        this.consistencyCheckCount = consistencyCheckCount;
    }
}