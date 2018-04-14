package cz.cas.lib.arcstorage.store;

import com.querydsl.jpa.JPAExpressions;
import cz.cas.lib.arcstorage.domain.QStorageConfig;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.exception.GeneralException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Random;

@Repository
@Slf4j
public class StorageConfigStore extends DomainStore<StorageConfig, QStorageConfig> {

    public StorageConfigStore() {
        super(StorageConfig.class, QStorageConfig.class);
    }

    public StorageConfig getByPriority() {
        QStorageConfig qConfig = qObject();
        QStorageConfig qConfig2 = new QStorageConfig("nested");
        List<StorageConfig> configs = query().select(qConfig).where(qConfig.priority.eq(JPAExpressions.select(qConfig2.priority.max()).from(qConfig2))).fetch();
        if (configs.isEmpty())
            throw new GeneralException("no active storage attached");
        StorageConfig config = configs.get(new Random().nextInt(configs.size()));
        detachAll();
        return config;
    }
}
