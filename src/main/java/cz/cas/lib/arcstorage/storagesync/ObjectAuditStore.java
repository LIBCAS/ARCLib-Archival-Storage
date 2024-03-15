package cz.cas.lib.arcstorage.storagesync;

import com.querydsl.jpa.impl.JPAQuery;
import cz.cas.lib.arcstorage.domain.store.DomainStore;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.*;

@Repository
public class ObjectAuditStore extends DomainStore<ObjectAudit, QObjectAudit> {
    public ObjectAuditStore() {
        super(ObjectAudit.class, QObjectAudit.class);
    }

    /**
     * Finds object operations (which should be synced) within specified time range, ordered by operation registration in time ASC.
     * If there are multiple operations of the same object, only the newest audit is present in the result list.
     *
     * @param from null for no lower limit
     * @param to   null for no upper limit
     * @return list of operations to sync
     */
    public List<ObjectAudit> findAuditsForSync(Instant from, Instant to) {
        JPAQuery<ObjectAudit> query = query()
                .select(qObject())
                .orderBy(qObject().created.desc());
        if (from != null)
            query.where(qObject().created.goe(from));
        if (to != null)
            query.where(qObject().created.loe(to));
        List<ObjectAudit> fetch = query.fetch();
        detachAll();
        List<ObjectAudit> lastOpsList = new ArrayList<>();
        Set<String> registeredOps = new HashSet<>();
        fetch.forEach(op -> {
            if (!registeredOps.contains(op.getIdInDatabase())) {
                registeredOps.add(op.getIdInDatabase());
                lastOpsList.add(op);
            }
        });
        Collections.reverse(lastOpsList);
        return lastOpsList;
    }

    /**
     * returns all operations of object in ASC order
     * @param objectId
     * @return
     */
    public List<ObjectAudit> findOperationsOfObject(String objectId) {
        List<ObjectAudit> fetch = query()
                .select(qObject())
                .where(qObject().idInDatabase.eq(objectId))
                .orderBy(qObject().created.asc())
                .fetch();
        detachAll();
        return fetch;
    }

    public List<ObjectAudit> findAll(Instant from, Integer count, String dataSpace) {
        QObjectAudit audit = qObject();
        JPAQuery<ObjectAudit> q = query().select(audit).orderBy(audit.created.asc());
        if (from != null) {
            q.where(audit.created.after(from));
        }
        if (dataSpace != null) {
            q.where(audit.user.dataSpace.eq(dataSpace));
        }
        if (count != null) {
            q.limit(count);
        }
        List<ObjectAudit> res = q.fetch();
        detachAll();
        return res;
    }
}
