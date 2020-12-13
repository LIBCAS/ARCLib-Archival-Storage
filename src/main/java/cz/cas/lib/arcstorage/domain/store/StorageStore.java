package cz.cas.lib.arcstorage.domain.store;

import cz.cas.lib.arcstorage.domain.entity.QStorage;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.StorageBasicDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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

    @Transactional
    @Override
    public void delete(Storage entity) {
        super.delete(entity);
    }

    /**
     * does not check for reachability, i.e. if the storage has become unreachabe/reachable after the last reachability check
     * this method does not reflect it.. storages are checked for reachability during every get/store request
     */
    public List<Storage> findUnreachableStorages() {
        List<Storage> fetch = query().select(qObject()).where(qObject().reachable.eq(false)).fetch();
        detachAll();
        return fetch;
    }

    public List<StorageBasicDto> getAllAsDtos() {
        Collection<Storage> storages = findAll();
        return storages.stream().map(StorageBasicDto::transmute).collect(Collectors.toList());
    }

    public long getCount() {
        return query().fetchCount();
    }
}
