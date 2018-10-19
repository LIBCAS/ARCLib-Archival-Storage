package cz.cas.lib.arcstorage.domain.store;

import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAUpdateClause;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.QArchivalObject;
import cz.cas.lib.arcstorage.dto.ObjectState;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Repository
public class ArchivalObjectStore extends DomainStore<ArchivalObject, QArchivalObject> {
    public ArchivalObjectStore() {
        super(ArchivalObject.class, QArchivalObject.class);
    }

    /**
     * find objects of all types to be copied to new storage
     *
     * @param from required filter, only objects which are >= <i>from</i> are retrieved, if null then all objects are retrieved
     * @param to   required filter, only objects which are <= <i>to</i> are retrieved
     * @return
     */
    public List<ArchivalObject> findObjectsForNewStorage(Instant from, Instant to) {
        JPAQuery<ArchivalObject> query = query()
                .select(qObject())
                .where(qObject().state.notIn(ObjectState.PROCESSING, ObjectState.ARCHIVAL_FAILURE, ObjectState.PRE_PROCESSING))
                .where(qObject().created.loe(to))
                .orderBy(qObject().created.asc());
        if (from != null)
            query.where(qObject().created.goe(from));
        List<ArchivalObject> fetch = query.fetch();
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
}
