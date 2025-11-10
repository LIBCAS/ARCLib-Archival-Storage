package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.storage.StorageService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@Slf4j
public class IntervalJobService {

    private ScheduledExecutorService scheduledExecutorService;
    private StorageProvider storageProvider;
    private ScheduledFuture<?> reachabilityCheckFuture = null;
    private final Set<Storage> attachedUnreachableStoragesFromLastCheck = new HashSet<>();
    private ArcstorageMailCenter mailCenter;

    public void scheduleReachabilityChecks(int intervalInMinutes) {
        if (intervalInMinutes < 1)
            throw new IllegalArgumentException("Reachability check interval must be greater than 0");
        log.debug("Scheduling reachability check to run every {} minutes", intervalInMinutes);
        Runnable reachabilityCheckRunnable = () -> {

            Pair<List<StorageService>, List<StorageService>> storages = storageProvider.checkReachabilityOfAllStorages(false);
            String prefix = "periodical check of reachability of logical storages:";

            Set<Storage> attachedReachableStorages = storages.getLeft().stream().map(StorageService::getStorage).filter(s -> !s.isDetached()).collect(Collectors.toSet());
            Set<Storage> attachedUnreachableStorages = storages.getRight().stream().map(StorageService::getStorage).filter(s -> !s.isDetached()).collect(Collectors.toSet());
            Set<Storage> detachedReachableStorages = storages.getLeft().stream().map(StorageService::getStorage).filter(Storage::isDetached).collect(Collectors.toSet());
            Set<Storage> detachedUnreachableStorages = storages.getRight().stream().map(StorageService::getStorage).filter(Storage::isDetached).collect(Collectors.toSet());

            log.debug("{} counts: {} attached & reachable, {} attached but unreachable, {} detached but reachable, {} detached and unreachable", prefix,
                    attachedReachableStorages.size(), attachedUnreachableStorages.size(), detachedReachableStorages.size(), detachedUnreachableStorages.size());

            if (!attachedUnreachableStorages.isEmpty() &&
                    !attachedUnreachableStoragesFromLastCheck.equals(attachedUnreachableStorages)) {
                mailCenter.sendAttachedUnreachableStoragesWarning(attachedUnreachableStorages);
                attachedUnreachableStoragesFromLastCheck.clear();
                attachedUnreachableStoragesFromLastCheck.addAll(attachedUnreachableStorages);
            }

            if (attachedUnreachableStorages.isEmpty() &&
                    !attachedUnreachableStoragesFromLastCheck.isEmpty()) {
                mailCenter.sendAttachedReachableStoragesInfo(attachedUnreachableStoragesFromLastCheck);
                attachedUnreachableStoragesFromLastCheck.clear();
            }
        };

        if (reachabilityCheckFuture != null) {
            reachabilityCheckFuture.cancel(true);
        }
        reachabilityCheckFuture = scheduledExecutorService.scheduleAtFixedRate(reachabilityCheckRunnable, 0, intervalInMinutes, TimeUnit.MINUTES);
    }


    @Autowired
    public void setScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
    }

    @Autowired
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    @Autowired
    public void setMailCenter(ArcstorageMailCenter mailCenter) {
        this.mailCenter = mailCenter;
    }
}
