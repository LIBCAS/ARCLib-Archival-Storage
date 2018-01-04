package cz.cas.lib.arcstorage.store;

import com.querydsl.jpa.JPAExpressions;
import cz.cas.lib.arcstorage.domain.QStorageConfig;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Repository;

@Repository
@Log4j
public class StorageConfigStore extends DomainStore<StorageConfig, QStorageConfig> {

    public StorageConfigStore() {
        super(StorageConfig.class, QStorageConfig.class);
    }

    public StorageConfig getByPriority() {
        QStorageConfig qConfig = qObject();
        QStorageConfig qConfig2 = new QStorageConfig("nested");
        StorageConfig config;
        config = query().select(qConfig).where(qConfig.priority.eq(JPAExpressions.select(qConfig2.priority.max()).from(qConfig2))).fetchOne();
        detachAll();
        return config;
    }
}
