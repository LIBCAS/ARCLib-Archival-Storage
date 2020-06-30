package cz.cas.lib.arcstorage.domain.store;

import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAUpdateClause;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.QArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.security.authorization.assign.audit.EntitySaveEvent;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Repository
public class ArchivalObjectStore extends DomainStore<ArchivalObject, QArchivalObject> {
    public ArchivalObjectStore() {
        super(ArchivalObject.class, QArchivalObject.class);
    }

    /**
     * find objects of all types to be copied to new storage
     */
    public List<ArchivalObject> findObjectsForNewStorage(Instant from, Instant to) {
        JPAQuery<ArchivalObject> query = query()
                .select(qObject())
                //objects in state DELETION_FAILURE are not omitted as the state will change to DELETED during cleanup
                .where(qObject().state.notIn(ObjectState.PROCESSING, ObjectState.PRE_PROCESSING))
                .orderBy(qObject().created.asc());
        if (from != null)
            query.where(qObject().created.goe(from));
        if (to != null)
            query.where(qObject().created.loe(to));
        List<ArchivalObject> fetch = query.fetch();
        detachAll();
        return fetch;
    }

    public List<ArchivalObject> findObjectsOfUser(User u) {
        List<ArchivalObject> fetch = query().select(qObject()).where(qObject().owner.eq(u)).fetch();
        detachAll();
        return fetch;
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

    public List<ArchivalObjectDto> findAllCreatedWithinTimeRange(Instant from, Instant to) {
        JPAQuery<ArchivalObject> query = query()
                .select(qObject())
                .orderBy(qObject().created.asc());
        if (from != null)
            query.where(qObject().created.goe(from));
        if (to != null)
            query.where(qObject().created.loe(to));
        List<ArchivalObjectDto> collect = query
                .fetch().stream().map(ArchivalObject::toDto).collect(Collectors.toList());
        detachAll();
        return collect;
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
