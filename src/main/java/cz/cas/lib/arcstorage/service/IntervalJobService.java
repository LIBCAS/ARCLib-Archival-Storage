package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.storage.StorageService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class IntervalJobService {

    private ScheduledExecutorService scheduledExecutorService;
    private StorageProvider storageProvider;
    private ScheduledFuture<?> reachabilityCheckFuture = null;

    public void scheduleReachabilityChecks(int intervalInMinutes) {
        if (intervalInMinutes < 1)
            throw new IllegalArgumentException("Reachability check interval must be greater than 0");
        log.debug("Scheduling reachability check to run every {} minutes", intervalInMinutes);
        Runnable reachabilityCheckRunnable = () -> {
            Pair<List<StorageService>, List<StorageService>> listListPair = storageProvider.checkReachabilityOfAllStorages();
            String prefix = "periodical check of reachability of logical storages:";
            if (listListPair.getRight().isEmpty())
                log.debug("{} all {} storages are reachable", prefix, listListPair.getLeft().size());
            else
                log.debug("{} some storages are unreachable: {}", prefix, Arrays.toString(listListPair.getRight().stream().map(s -> s.getStorage().getId()).toArray()));
        };
        if (reachabilityCheckFuture != null) {
            reachabilityCheckFuture.cancel(true);
        }
        reachabilityCheckFuture = scheduledExecutorService.scheduleAtFixedRate(reachabilityCheckRunnable, 0, intervalInMinutes, TimeUnit.MINUTES);
    }


    @Inject
    public void setScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
    }

    @Inject
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }
}
