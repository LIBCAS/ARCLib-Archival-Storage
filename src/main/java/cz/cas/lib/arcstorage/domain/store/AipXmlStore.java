package cz.cas.lib.arcstorage.domain.store;

import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.QAipXml;
import cz.cas.lib.arcstorage.security.authorization.assign.audit.EntitySaveEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.time.Instant;

@Repository
@Slf4j
public class AipXmlStore extends DomainStore<AipXml, QAipXml> {
    public AipXmlStore() {
        super(AipXml.class, QAipXml.class);
    }

    public AipXml findBySipAndVersion(String sipId, int xmlVersion) {
        QAipXml qObj = qObject();
        AipXml xml = query().select(qObj).where(qObj.sip.id.eq(sipId)).where(qObj.version.eq(xmlVersion)).fetchOne();
        detachAll();
        return xml;
    }

    @Override
    protected void logSaveEvent(AipXml entity) {
        if (entity.getOwner() != null && auditLogger != null)
            auditLogger.logEvent(new EntitySaveEvent(Instant.now(), entity.getOwner().getId(), type.getSimpleName(), entity.getId()));
        else super.logSaveEvent(entity);
    }

    @Override
    protected void logDeleteEvent(AipXml entity) {
        if (entity.getOwner() != null && auditLogger != null)
            auditLogger.logEvent(new EntitySaveEvent(Instant.now(), entity.getOwner().getId(), type.getSimpleName(), entity.getId()));
        else super.logSaveEvent(entity);
    }
}
