package cz.cas.lib.arcstorage.domain.store;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.QArchivalObject;
import org.springframework.stereotype.Repository;

@Repository
public class ArchivalObjectStore extends DomainStore<ArchivalObject, QArchivalObject> {
    public ArchivalObjectStore() {
        super(ArchivalObject.class, QArchivalObject.class);
    }
}
