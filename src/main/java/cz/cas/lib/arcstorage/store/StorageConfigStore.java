package cz.cas.lib.arcstorage.store;

import cz.cas.lib.arcstorage.domain.QStorageConfig;
import cz.cas.lib.arcstorage.domain.StorageConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

@Repository
@Slf4j
public class StorageConfigStore extends DomainStore<StorageConfig, QStorageConfig> {

    public StorageConfigStore() {
        super(StorageConfig.class, QStorageConfig.class);
    }

    @Transactional
    @Override
    public StorageConfig save(StorageConfig entity) {
        return super.save(entity);
    }
}
