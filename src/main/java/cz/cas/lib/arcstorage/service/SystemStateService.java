package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.domain.store.SystemStateStore;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;

import javax.inject.Inject;
import java.time.Instant;

import static cz.cas.lib.arcstorage.util.Utils.notNull;

@Service
@Slf4j
public class SystemStateService {

    private SystemStateStore systemStateStore;
    private IntervalJobService intervalJobService;

    public SystemState get() {
        return systemStateStore.get();
    }

    @org.springframework.transaction.annotation.Transactional(propagation = Propagation.REQUIRES_NEW)
    public SystemState save(SystemState entity) {
        notNull(entity, () -> new IllegalArgumentException("entity"));
        SystemState current = systemStateStore.get();
        if (!entity.getId().equals(current.getId()))
            throw new ConflictObject("there is already an configuration object with id: " + current.getId() + " only one configuration object is allowed");
        systemStateStore.save(entity);
        if (current.getReachabilityCheckIntervalInMinutes() != entity.getReachabilityCheckIntervalInMinutes())
            intervalJobService.scheduleReachabilityChecks(entity.getReachabilityCheckIntervalInMinutes());
        return entity;
    }

    @Transactional
    public SystemState createDefaultIfNotExist() {
        SystemState any = systemStateStore.findAny();
        if (any == null) {
            any = systemStateStore.save(new SystemState(2, false, 60, null, null));
            log.info("No system state entity found, created default: " + any);
        }
        return any;
    }

    @Transactional
    public SystemState setReachabilityCheckedNow() {
        SystemState systemState = systemStateStore.get();
        systemState.setLastReachabilityCheck(Instant.now());
        systemStateStore.save(systemState);
        return systemState;
    }

    @Inject
    public void setSystemStateStore(SystemStateStore systemStateStore) {
        this.systemStateStore = systemStateStore;
    }

    @Inject
    public void setIntervalJobService(IntervalJobService intervalJobService) {
        this.intervalJobService = intervalJobService;
    }
}
