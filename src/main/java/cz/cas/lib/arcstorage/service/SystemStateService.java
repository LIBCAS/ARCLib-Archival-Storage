package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.domain.store.SystemStateStore;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;

import static cz.cas.lib.arcstorage.util.Utils.executeAfterTransactionCommits;
import static cz.cas.lib.arcstorage.util.Utils.notNull;

@Service
@Slf4j
public class SystemStateService {

    private SystemStateStore systemStateStore;
    private ArcstorageMailCenter arcstorageMailCenter;

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
        return entity;
    }

    @Transactional
    public SystemState createDefaultIfNotExist() {
        SystemState any = systemStateStore.findAny();
        if (any == null) {
            any = systemStateStore.save(new SystemState(2, false, 60, null, null, null));
            log.info("No system state entity found, created default: " + any);
        }
        return any;
    }

    @org.springframework.transaction.annotation.Transactional(propagation = Propagation.REQUIRES_NEW)
    public void setReadWrite(SystemState systemState) {
        systemState.setReadOnly(false);
        save(systemState);
        log.info("system set to read-write mode");
    }

    /**
     * @param systemState
     * @param e           if exception is filled then also mail with exception stacktrace is sent to admin, the exception is not thrown by this method
     */
    @org.springframework.transaction.annotation.Transactional(propagation = Propagation.REQUIRES_NEW)
    public void setReadOnly(SystemState systemState, Exception e) {
        systemState.setReadOnly(true);
        save(systemState);
        if (e != null) {
            executeAfterTransactionCommits(() -> arcstorageMailCenter.sendFatalError(e, true));
        }
        log.info("system set to read-only mode");
    }

    @Autowired
    public void setSystemStateStore(SystemStateStore systemStateStore) {
        this.systemStateStore = systemStateStore;
    }

    @Autowired
    public void setArcstorageMailCenter(ArcstorageMailCenter arcstorageMailCenter) {
        this.arcstorageMailCenter = arcstorageMailCenter;
    }
}
