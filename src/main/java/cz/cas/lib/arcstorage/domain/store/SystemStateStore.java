package cz.cas.lib.arcstorage.domain.store;

import cz.cas.lib.arcstorage.domain.entity.QSystemState;
import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.exception.MissingObject;
import org.springframework.stereotype.Repository;

import static cz.cas.lib.arcstorage.util.Utils.notNull;

@Repository
public class SystemStateStore extends DomainStore<SystemState, QSystemState> {
    public SystemStateStore() {
        super(SystemState.class, QSystemState.class);
    }

    public SystemState get() {
        SystemState systemState = query().select(qObject()).fetchOne();
        notNull(systemState, () -> new MissingObject(SystemState.class, "no system state record found"));
        detachAll();
        return systemState;
    }
}