package cz.cas.lib.arcstorage.domain.store;

import com.querydsl.core.types.dsl.EntityPathBase;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.domain.views.*;
import cz.cas.lib.arcstorage.dto.ObjectState;
import org.springframework.stereotype.Repository;

import javax.inject.Inject;
import javax.persistence.EntityManager;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.Collections.emptyList;

@Repository
public class ArchivalObjectLightweightViewStore {
    /**
     * RDBMS may have limit for bind params count in a query, limit of PGSQL is 32767
     */
    public static final int BIND_PARAMS_LIMIT = 32000;
    protected EntityManager entityManager;
    protected JPAQueryFactory queryFactory;

    /**
     * Finds all the instances corresponding to the specified {@link List} of ids.
     *
     * <p>
     * The returned {@link List} of instances is ordered according to the order of provided ids. If the instance
     * with provided id is not found, it is skipped, therefore the size of returned {@link List} might be of
     * different size that of the provided ids {@link List}.
     * </p>
     *
     * @param ids Ordered {@link List} of ids
     * @return ordered {@link List} of instances
     */
    public List<ArchivalObjectLightweightView> findAllInList(List<String> ids) {
        if (ids.isEmpty()) {
            return emptyList();
        }
        List<ArchivalObjectLightweightView> result = new ArrayList<>();

        QAipXmlLightweightView qAipXml = QAipXmlLightweightView.aipXmlLightweightView;
        for (int i = 0; i < ids.size(); i = i + BIND_PARAMS_LIMIT) {
            List<AipXmlLightweightView> batch = query(qAipXml)
                    .select(qAipXml)
                    .where(qAipXml.id.in(ids.subList(i, Math.min(i + BIND_PARAMS_LIMIT, ids.size()))))
                    .fetch();
            result.addAll(batch);
        }

        QAipSipLightweightView qAipSip = QAipSipLightweightView.aipSipLightweightView;
        for (int i = 0; i < ids.size(); i = i + BIND_PARAMS_LIMIT) {
            List<AipSipLightweightView> batch = query(qAipSip)
                    .select(qAipSip)
                    .where(qAipSip.id.in(ids.subList(i, Math.min(i + BIND_PARAMS_LIMIT, ids.size()))))
                    .fetch();
            result.addAll(batch);
        }

        QGeneralObjectLightweightView qGeneral = QGeneralObjectLightweightView.generalObjectLightweightView;
        for (int i = 0; i < ids.size(); i = i + BIND_PARAMS_LIMIT) {
            List<GeneralObjectLightweightView> batch = query(qGeneral)
                    .select(qGeneral)
                    .where(qGeneral.id.in(ids.subList(i, Math.min(i + BIND_PARAMS_LIMIT, ids.size()))))
                    .fetch();
            result.addAll(batch);
        }

        detachAll();
        return DomainStore.sortByIDs(ids, result);
    }

    public List<ArchivalObjectLightweightView> findObjectsOfUser(User u) {
        List<ArchivalObjectLightweightView> result = new ArrayList<>();

        QAipXmlLightweightView qAipXml = QAipXmlLightweightView.aipXmlLightweightView;
        result.addAll(query(qAipXml).select(qAipXml).where(qAipXml.owner.eq(u)).fetch());

        QAipSipLightweightView qAipSip = QAipSipLightweightView.aipSipLightweightView;
        result.addAll(query(qAipSip).select(qAipSip).where(qAipSip.owner.eq(u)).fetch());

        QGeneralObjectLightweightView qGeneralObject = QGeneralObjectLightweightView.generalObjectLightweightView;
        result.addAll(query(qGeneralObject).select(qGeneralObject).where(qGeneralObject.owner.eq(u)).fetch());

        detachAll();

        return result;
    }

    /**
     * find objects of all types to be copied to new storage
     */
    public List<ArchivalObjectLightweightView> findObjectsForNewStorage(Instant from, Instant to) {
        return findObjectsWithinTimeRange(from, to, ObjectState.PRE_PROCESSING, ObjectState.PROCESSING);
    }

    public List<ArchivalObjectLightweightView> findAllCreatedWithinTimeRange(Instant from, Instant to) {
        return findObjectsWithinTimeRange(from, to);
    }

    private List<ArchivalObjectLightweightView> findObjectsWithinTimeRange(Instant from, Instant to, ObjectState... excludedStates) {
        List<ArchivalObjectLightweightView> result = new ArrayList<>();

        QAipXmlLightweightView qAipXml = QAipXmlLightweightView.aipXmlLightweightView;
        result.addAll(findObjectsWithinTimeRange(qAipXml, qAipXml._super, from, to, excludedStates));

        QAipSipLightweightView qAipSip = QAipSipLightweightView.aipSipLightweightView;
        result.addAll(findObjectsWithinTimeRange(qAipSip, qAipSip._super, from, to, excludedStates));

        QGeneralObjectLightweightView qGeneral = QGeneralObjectLightweightView.generalObjectLightweightView;
        result.addAll(findObjectsWithinTimeRange(qGeneral, qGeneral._super, from, to, excludedStates));

        detachAll();

        result.sort(Comparator.comparing(ArchivalObjectLightweightView::getCreated));
        return result;
    }

    private <W extends ArchivalObjectLightweightView, QW extends EntityPathBase<W>> List<W> findObjectsWithinTimeRange(
            QW qObj, QArchivalObjectLightweightView qObjParent, Instant from, Instant to, ObjectState... excludedStates) {

        JPAQuery<W> query = query(qObj).select(qObj).orderBy(qObjParent.created.asc());

        if (from != null)
            query.where(qObjParent.created.goe(from));
        if (to != null)
            query.where(qObjParent.created.loe(to));
        if (excludedStates != null)
            query.where(qObjParent.state.notIn(excludedStates));

        return query.fetch();
    }

    /**
     * Creates QueryDSL query object for other entity than the store one.
     *
     * @return Query object
     */
    protected <C> JPAQuery<?> query(EntityPathBase<C> base) {
        return queryFactory.from(base);
    }

    protected void detachAll() {
        entityManager.clear();
    }

    @Inject
    public void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Inject
    public void setQueryFactory(JPAQueryFactory queryFactory) {
        this.queryFactory = queryFactory;
    }


}
