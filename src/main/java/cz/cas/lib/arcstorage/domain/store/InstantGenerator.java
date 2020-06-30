package cz.cas.lib.arcstorage.domain.store;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import org.hibernate.Session;
import org.hibernate.tuple.ValueGenerator;

import java.time.Instant;

public class InstantGenerator implements ValueGenerator<Instant> {
    @Override
    public Instant generateValue(Session session, Object owner) {
        if (owner instanceof ArchivalObject) {
            Instant assigned = ((ArchivalObject) owner).getCreated();
            if (assigned != null)
                return assigned;
        }
        return Instant.now();
    }
}
