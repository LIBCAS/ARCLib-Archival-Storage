package cz.cas.lib.arcstorage.domain.store;

import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.QAipXml;
import cz.cas.lib.arcstorage.dto.ObjectState;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@Slf4j
public class AipXmlStore extends DomainStore<AipXml, QAipXml> {
    public AipXmlStore() {
        super(AipXml.class, QAipXml.class);
    }

    @Override
    @Transactional
    public AipXml save(AipXml entity) {
        return super.save(entity);
    }

    @Override
    @Transactional
    public void delete(AipXml entity) {
        super.delete(entity);
    }

    /**
     * for a particular XML version number returns all entities of unsuccessful tries and also the last, successful, try one if present
     *
     * @param sipId
     * @param xmlVersion
     * @return
     */
    public List<AipXml> findBySipAndVersion(String sipId, int xmlVersion) {
        QAipXml xml = qObject();
        List<AipXml> aipXmls = query().select(xml).where(xml.sip.id.eq(sipId)).where(xml.version.eq(xmlVersion)).fetch();
        detachAll();
        return aipXmls;
    }
}
