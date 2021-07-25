package cz.cas.lib.arcstorage.domain.store;

import com.querydsl.jpa.impl.JPAUpdateClause;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.QArchivalObject;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.security.authorization.assign.audit.EntitySaveEvent;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Repository
public class ArchivalObjectStore extends DomainStore<ArchivalObject, QArchivalObject> {
    public ArchivalObjectStore() {
        super(ArchivalObject.class, QArchivalObject.class);
    }

    public List<ArchivalObject> findProcessingObjects() {
        List<ArchivalObject> fetch = query()
                .select(qObject())
                .where(qObject().state.in(ObjectState.PROCESSING, ObjectState.PRE_PROCESSING))
                .fetch();
        detachAll();
        return fetch;
    }

    public List<ArchivalObject> findObjectsForCleanup(boolean alsoProcessing) {
        List<ObjectState> objectStates = new ArrayList<>();
        objectStates.add(ObjectState.ARCHIVAL_FAILURE);
        objectStates.add(ObjectState.DELETION_FAILURE);
        objectStates.add(ObjectState.ROLLBACK_FAILURE);
        if (alsoProcessing) {
            objectStates.add(ObjectState.PROCESSING);
            objectStates.add(ObjectState.PRE_PROCESSING);
        }
        List<ArchivalObject> fetch = query()
                .select(qObject())
                .where(qObject().state.in(objectStates))
                .fetch();
        detachAll();
        return fetch;
    }

    public void setObjectsState(ObjectState state, List<String> ids) {
        QArchivalObject q = qObject();
        JPAUpdateClause jpaUpdateClause = new JPAUpdateClause(entityManager, q);
        jpaUpdateClause.set(q.state, state).where(q.id.in(ids)).execute();
    }

    @Override
    protected void logSaveEvent(ArchivalObject entity) {
        if (entity.getOwner() != null && auditLogger != null)
            auditLogger.logEvent(new EntitySaveEvent(Instant.now(), entity.getOwner().getId(), type.getSimpleName(), entity.getId()));
        else super.logSaveEvent(entity);
    }

    @Override
    protected void logDeleteEvent(ArchivalObject entity) {
        if (entity.getOwner() != null && auditLogger != null)
            auditLogger.logEvent(new EntitySaveEvent(Instant.now(), entity.getOwner().getId(), type.getSimpleName(), entity.getId()));
        else super.logSaveEvent(entity);
    }
}
