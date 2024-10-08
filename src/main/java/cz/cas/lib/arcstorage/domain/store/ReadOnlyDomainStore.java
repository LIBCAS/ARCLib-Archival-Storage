package cz.cas.lib.arcstorage.domain.store;


import com.querydsl.core.types.dsl.*;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import cz.cas.lib.arcstorage.domain.entity.DomainObject;
import cz.cas.lib.arcstorage.exception.GeneralException;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;

import jakarta.persistence.EntityManager;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

/**
 * Facade around JPA {@link EntityManager} and QueryDSL providing CRUD operations.
 *
 * <p>
 * All the entity instances should have externally set {@link DomainObject#id} to an {@link java.util.UUID},
 * therefore we do not now if the instance is already saved in database or is completely new. Because of that, there
 * is no create/update method, only the {@link DomainStore#save(DomainObject)}, which handles both cases.
 * </p>
 *
 * <p>
 * JPA concept of managed/detached instances is prone to development errors. Therefore every instance should be
 * detached upon retrieving. All methods in {@link DomainStore} adhere to this rule.
 * </p>
 * <p>
 * All find methods in child classes should retrieve only ids on their own and then use
 * {@link DomainStore#findAllInList(List)} which detach the instances or use
 * {@link DomainStore#detachAll()} explicitly.
 * </p>
 *
 * <p>
 * After every saving of instance, the {@link EntityManager}'s context is flushed. This is a rather expensive
 * operation and therefore if more than a few instances should be saved in a row, one should use
 * {@link DomainStore#save(Collection)} which provides batching and only after saving all instances the context is
 * flushed.
 * </p>
 *
 * @param <T> Type of entity to hold
 * @param <Q> Type of query object
 */
public abstract class ReadOnlyDomainStore<T extends DomainObject, Q extends EntityPathBase<T>> {

    /**
     * RDBMS may have limit for bind params count in a query, limit of PGSQL is 32767
     */
    public static final int BIND_PARAMS_LIMIT = 32000;

    /**
     * Entity manager used for JPA
     */
    protected EntityManager entityManager;

    /**
     * QueryDSL query factory
     */
    protected JPAQueryFactory queryFactory;

    /**
     * Entity class object
     */
    @Getter
    protected Class<T> type;

    /**
     * QueryDSL meta class object
     */
    protected Class<Q> qType;

    private Q qObject;

    public ReadOnlyDomainStore(Class<T> type, Class<Q> qType) {
        this.type = type;
        this.qType = qType;

        this.qObject = constructQObject(type, qType);
    }

    /**
     * Finds all instances.
     *
     * <p>
     * Possibly very cost operation. Should be used only if we know there is not many instances or for
     * debugging purposes.
     * </p>
     *
     * @return {@link Collection} of instances
     */
    public Collection<T> findAll() {
        return findAll(0, 0);
    }

    /**
     * Returns a portion of all instances.
     * Limit 0 for limitless.
     *
     * <p>
     * Possibly less costly operation. Should be used especially if we know there is many instances or for
     * debugging purposes.
     * </p>
     *
     * @return {@link Collection} of instances
     */
    public Collection<T> findAll(long offset, long limit) {
        JPAQuery<T> query = query().select(qObject());
        applyWhereExpression(query);

        if (offset != 0) {
            query.offset(offset);
        }

        if (limit != 0) {
            query.limit(limit);
        }

        List<T> list = query.fetch();

        detachAll();

        return list;
    }

    /**
     * Count entities in db.
     *
     * @return count
     */
    public long countAll() {
        JPAQuery<T> query = query().select(qObject());
        applyWhereExpression(query);
        return query.fetchCount();
    }

    /**
     * Finds the first instance.
     *
     * <p>
     * Because there is no ordering it is not defined which instance will be returned. Should be used if there
     * is only one instance or in unit tests.
     * </p>
     *
     * @return Single instance or null if no instance exists
     */
    public T findAny() {
        JPAQuery<T> query = query().select(qObject);
        applyWhereExpression(query);

        T entity = query.fetchFirst();

        detachAll();

        return entity;
    }

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
    public List<T> findAllInList(List<String> ids) {
        if (ids.isEmpty()) {
            return emptyList();
        }

        StringPath idPath = propertyPath("id");

        List<T> list = new ArrayList<>();
        for (int i = 0; i < ids.size(); i = i + BIND_PARAMS_LIMIT) {
            List<T> batch = query().select(qObject)
                    .where(idPath.in(ids.subList(i, Math.min(i + BIND_PARAMS_LIMIT, ids.size()))))
                    .fetch();
            list.addAll(batch);
        }

        detachAll();

        return sortByIDs(ids, list);
    }

    /**
     * Finds the single instance with provided id.
     *
     * @param id Id of instance to find
     * @return Single instance or null if not found
     */
    public T find(String id) {
        StringPath idPath = propertyPath("id");

        JPAQuery<T> query = query().select(qObject).where(idPath.eq(id));
        applyWhereExpression(query);

        T entity = query.fetchFirst();

        detachAll();

        return entity;
    }

    public T getReference(String id) {
        return entityManager.getReference(type, id);
    }

    /**
     * Sorts given collection of domain objects by the order specified in the list of their IDs.
     */
    public static <T extends DomainObject> List<T> sortByIDs(List<String> ids, Collection<T> objects) {
        return objects.stream()
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingInt(o -> ids.indexOf(o.getId())))
                .collect(Collectors.toList());
    }

    /**
     * Creates QueryDSL query object.
     *
     * @return Query object
     */
    protected JPAQuery<?> query() {
        return queryFactory.from(qObject);
    }

    /**
     * Creates QueryDSL query object for other entity than the store one.
     *
     * @return Query object
     */
    protected <C> JPAQuery<?> query(EntityPathBase<C> base) {
        return queryFactory.from(base);
    }

    /**
     * Gets the used Query DSL object
     *
     * @return The QueryDSL Q instance
     */
    protected Q qObject() {
        return qObject;
    }

    protected void detachAll() {
        entityManager.clear();
    }

    /**
     * Creates meta object attribute.
     *
     * <p>
     * Used for addressing QueryDSL attributes, which are not known at compile time. Should be used with caution,
     * because it circumvents type safety.
     * </p>
     *
     * @param name Name of the attribute
     * @return Meta object attribute
     */
    protected StringPath propertyPath(String name) {
        PathBuilder<T> builder = new PathBuilder<>(qObject.getType(), qObject.getMetadata().getName());
        return builder.getString(name);
    }

    /**
     * Creates meta object attribute for enum type
     *
     * <p>
     * Used for addressing QueryDSL attributes, which are not known at compile time. Should be used with caution,
     * because it circumvents type safety.
     * </p>
     *
     * @param name Name of the attribute
     * @return Meta object attribute
     */
    protected <X extends Enum<X>> EnumPath<X> propertyPathEnum(String name, Class<X> type) {
        PathBuilder<T> builder = new PathBuilder<>(qObject.getType(), qObject.getMetadata().getName());
        return builder.getEnum(name, type);
    }

    /**
     * Provides extension point for inheriting classes to define a where clause for all find* methods
     * in {@link DomainStore}.
     *
     * @return A where clause or null
     */
    protected BooleanExpression findWhereExpression() {
        return null;
    }

    private void applyWhereExpression(JPAQuery<T> query) {
        BooleanExpression expression = findWhereExpression();

        if (expression != null) {
            query.where(expression);
        }
    }

    private Q constructQObject(Class<T> type, Class<Q> qType) {
        String name = type.getSimpleName();

        try {
            Constructor<Q> constructor = qType.getConstructor(String.class);
            return constructor.newInstance(name);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException |
                 InvocationTargetException e) {
            throw new GeneralException("Error creating Q object for + " + type.getName());
        }
    }

    @Autowired
    public void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    @Autowired
    public void setQueryFactory(JPAQueryFactory queryFactory) {
        this.queryFactory = queryFactory;
    }
}

