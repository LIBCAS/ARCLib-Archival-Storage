package cz.cas.lib.arcstorage.domain.store;

import cz.cas.lib.arcstorage.domain.entity.Configuration;
import cz.cas.lib.arcstorage.domain.entity.QConfiguration;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import cz.cas.lib.arcstorage.exception.MissingObject;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;

import static cz.cas.lib.arcstorage.util.Utils.notNull;

@Repository
public class ConfigurationStore extends DomainStore<Configuration, QConfiguration> {
    public ConfigurationStore() {
        super(Configuration.class, QConfiguration.class);
    }

    public Configuration get() {
        Configuration configuration = query().select(qObject()).fetchOne();
        notNull(configuration, () -> new MissingObject(Configuration.class, "configuration object"));
        detachAll();
        return configuration;
    }

    @Override
    @org.springframework.transaction.annotation.Transactional(propagation = Propagation.REQUIRES_NEW)
    public Configuration save(Configuration entity) {
        notNull(entity, () -> new IllegalArgumentException("entity"));
        Configuration current = query().select(qObject()).fetchOne();
        if (current!=null && !entity.getId().equals(current.getId()))
            throw new ConflictObject("there is already an configuration object with id: " + current.getId() + " only one configuration object is allowed");
        Configuration obj = entityManager.merge(entity);
        entityManager.flush();
        detachAll();
        return obj;
    }
}