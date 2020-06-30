package cz.cas.lib.arcstorage.domain.store;

import com.querydsl.jpa.impl.JPAQuery;
import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.QAipSip;
import cz.cas.lib.arcstorage.security.authorization.assign.audit.EntitySaveEvent;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public class AipSipStore extends DomainStore<AipSip, QAipSip> {
    public AipSipStore() {
        super(AipSip.class, QAipSip.class);
    }

    @Override
    protected void logSaveEvent(AipSip entity) {
        if (entity.getOwner() != null && auditLogger != null)
            auditLogger.logEvent(new EntitySaveEvent(Instant.now(), entity.getOwner().getId(), type.getSimpleName(), entity.getId()));
        else super.logSaveEvent(entity);
    }

    @Override
    protected void logDeleteEvent(AipSip entity) {
        if (entity.getOwner() != null && auditLogger != null)
            auditLogger.logEvent(new EntitySaveEvent(Instant.now(), entity.getOwner().getId(), type.getSimpleName(), entity.getId()));
        else super.logSaveEvent(entity);
    }

    /**
     * Finds limited count of AIP DB records since specified creation time ordered by creation time ASC
     *
     * @param timestmap null for no time bound (since beginning of time)
     * @param count     null for no limit, i.e. all records since the specified time
     * @return
     */
    public List<AipSip> findAfter(Instant timestmap, Integer count) {
        JPAQuery<AipSip> query = query()
                .select(qObject())
                .orderBy(qObject().created.asc());
        if (timestmap != null)
            query.where(qObject().created.after(timestmap));
        if (count != null)
            query.limit(count);
        List<AipSip> fetch = query.fetch();
        detachAll();
        return fetch;
    }
}
