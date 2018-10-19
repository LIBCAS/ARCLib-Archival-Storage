package cz.cas.lib.arcstorage.storagesync;

import cz.cas.lib.arcstorage.domain.store.DomainStore;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import org.springframework.stereotype.Repository;

@Repository
public class StorageSyncStatusStore extends DomainStore<StorageSyncStatus, QStorageSyncStatus> {
    public StorageSyncStatusStore() {
        super(StorageSyncStatus.class, QStorageSyncStatus.class);
    }

    public StorageSyncStatus findSyncStatusOfStorage(String storageId) {
        StorageSyncStatus fetch = query()
                .select(qObject())
                .where(qObject().storage.id.eq(storageId))
                .orderBy(qObject().created.desc())
                .fetchFirst();
        detachAll();
        return fetch;
    }

    public boolean anyInInitialOrFinishingPhase() {
        Object o = query().where(qObject().phase.in(StorageSyncPhase.INIT,StorageSyncPhase.FINISHING)).fetchFirst();
        detachAll();
        return o != null;
    }

    @Override
    @Transactional
    public void delete(StorageSyncStatus entity) {
        super.delete(entity);
    }

    @Transactional
    @Override
    public StorageSyncStatus save(StorageSyncStatus entity) {
        return super.save(entity);
    }
}
