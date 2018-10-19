package cz.cas.lib.arcstorage.storagesync;

import cz.cas.lib.arcstorage.domain.store.DomainStore;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public class ObjectAuditStore extends DomainStore<ObjectAudit, QObjectAudit> {
    public ObjectAuditStore() {
        super(ObjectAudit.class, QObjectAudit.class);
    }

    public List<ObjectAudit> findOperationsToBeSyncedInPhase2(Instant from) {
        List<ObjectAudit> fetch = query()
                .select(qObject())
                .where(qObject().created.goe(from))
                .where(qObject().operation.in(AuditedOperation.DELETION, AuditedOperation.REMOVAL, AuditedOperation.RENEWAL))
                .fetch();
        detachAll();
        return fetch;
    }

    /**
     * returns all operations of object in ASC order
     * @param objectId
     * @return
     */
    public List<ObjectAudit> findOperationsOfObject(String objectId) {
        List<ObjectAudit> fetch = query()
                .select(qObject())
                .where(qObject().objectId.eq(objectId))
                .orderBy(qObject().created.asc())
                .fetch();
        detachAll();
        return fetch;
    }
}
