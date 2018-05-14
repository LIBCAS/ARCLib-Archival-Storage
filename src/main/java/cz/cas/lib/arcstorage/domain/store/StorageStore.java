package cz.cas.lib.arcstorage.domain.store;

import cz.cas.lib.arcstorage.domain.entity.QStorage;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

@Repository
@Slf4j
public class StorageStore extends DomainStore<Storage, QStorage> {

    public StorageStore() {
        super(Storage.class, QStorage.class);
    }

    @Transactional
    @Override
    public Storage save(Storage entity) {
        return super.save(entity);
    }
}
