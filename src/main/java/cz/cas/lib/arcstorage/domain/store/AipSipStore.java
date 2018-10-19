package cz.cas.lib.arcstorage.domain.store;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.QAipSip;
import cz.cas.lib.arcstorage.dto.ObjectState;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class AipSipStore extends DomainStore<AipSip, QAipSip> {
    public AipSipStore() {
        super(AipSip.class, QAipSip.class);
    }

    @Override
    @Transactional
    public AipSip save(AipSip entity) {
        return super.save(entity);
    }

    @Override
    @Transactional
    public void delete(AipSip entity) {
        super.delete(entity);
    }
}
