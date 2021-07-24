package cz.cas.lib.arcstorage.domain.store;

import com.querydsl.core.types.dsl.EntityPathBase;
import cz.cas.lib.arcstorage.domain.entity.DomainObject;
import cz.cas.lib.arcstorage.security.audit.AuditLogger;
import cz.cas.lib.arcstorage.security.authorization.assign.audit.EntityDeleteEvent;
import cz.cas.lib.arcstorage.security.authorization.assign.audit.EntitySaveEvent;
import cz.cas.lib.arcstorage.security.user.UserDetails;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;

import javax.inject.Inject;
import javax.persistence.EntityManager;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.util.Utils.notNull;
import static cz.cas.lib.arcstorage.util.Utils.unwrap;

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
public abstract class DomainStore<T extends DomainObject, Q extends EntityPathBase<T>> extends ReadOnlyDomainStore<T, Q> {

    protected AuditLogger auditLogger;
    protected UserDetails userDetails;

    public DomainStore(Class<T> type, Class<Q> qType) {
        super(type, qType);
    }

    /**
     * Creates or updates instance.
     *
     * <p>
     * Corresponds to {@link EntityManager#merge(Object)} method.
     * </p>
     *
     * @param entity Instance to save
     * @return Saved detached instance
     * @throws IllegalArgumentException If entity is NULL
     */
    public T save(T entity) {
        notNull(entity, () -> new IllegalArgumentException("entity"));

        T obj = entityManager.merge(entity);

        entityManager.flush();
        detachAll();

        logSaveEvent(entity);

        return obj;
    }

    /**
     * Provides batching for {@link DomainStore#save(DomainObject)} method.
     *
     * @param entities Instances to save
     * @throws IllegalArgumentException If entity is NULL
     */
    public Collection<? extends T> save(Collection<? extends T> entities) {
        notNull(entities, () -> new IllegalArgumentException("entities"));

        Set<? extends T> saved = entities.stream()
                .map(entityManager::merge)
                .collect(Collectors.toSet());

        entityManager.flush();
        detachAll();

        entities.forEach(this::logSaveEvent);

        return saved;
    }

    /**
     * Deletes an instance.
     *
     * <p>
     * Non existing instance is silently skipped.
     * </p>
     *
     * @param entity Instance to delete
     * @throws IllegalArgumentException If entity is NULL
     */
    public void delete(T entity) {
        if (!entityManager.contains(entity) && entity != null) {
            entity = entityManager.find(type, entity.getId());
        }

        if (entity != null) {
            entityManager.remove(entity);

            logDeleteEvent(entity);
        }
    }

    protected void logSaveEvent(T entity) {
        if (unwrap(auditLogger) != null) {
            String userId;
            try {
                userDetails.toString();
                userId = unwrap(userDetails) != null ? userDetails.getId() : null;
            } catch (BeanCreationException | NullPointerException ex) {
                userId = null;
            }
            auditLogger.logEvent(new EntitySaveEvent(Instant.now(), userId, type.getSimpleName(), entity.getId()));
        }
    }

    protected void logDeleteEvent(T entity) {
        if (unwrap(auditLogger) != null) {
            String userId;
            try {
                userDetails.toString();
                userId = unwrap(userDetails) != null ? userDetails.getId() : null;
            } catch (BeanCreationException | NullPointerException ex) {
                userId = null;
            }
            auditLogger.logEvent(new EntityDeleteEvent(Instant.now(), userId, type.getSimpleName(), entity.getId()));
        }
    }

    @Inject
    public void setAuditLogger(AuditLogger auditLogger) {
        this.auditLogger = auditLogger;
    }

    @Autowired(required = false)
    public void setUserDetails(UserDetails userDetails) {
        this.userDetails = userDetails;
    }
}
