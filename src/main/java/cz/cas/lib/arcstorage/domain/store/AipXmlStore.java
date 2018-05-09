package cz.cas.lib.arcstorage.domain.store;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.QAipXml;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.exception.MissingObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@Slf4j
public class AipXmlStore extends DomainStore<AipXml, QAipXml> {
    public AipXmlStore() {
        super(AipXml.class, QAipXml.class);
    }

    public int getNextXmlVersionNumber(String sipId) {
        QAipXml xml = qObject();
        Integer lastVersion = query().select(xml.version.max()).where(xml.sip.id.eq(sipId)).fetchFirst();
        if (lastVersion == null) {
            log.warn("Could not find any XML version of AIP: " + sipId);
            throw new MissingObject(AipSip.class, sipId);
        }
        return 1 + lastVersion;
    }

    /**
     * @return all XMLs in processing state which SIP is not in processing state
     */
    public List<AipXml> findUnfinishedXmls() {
        QAipXml xml = qObject();
        return (List<AipXml>) query().where(xml.state.in(ObjectState.PROCESSING, ObjectState.FAILED)).where(xml.sip.state.eq(ObjectState.PROCESSING).not()).fetch();
    }

    public void rollbackUnfinishedXmlsRecords() {
        QAipXml xml = qObject();
        queryFactory.update(xml).where(xml.state.in(ObjectState.PROCESSING, ObjectState.FAILED)).set(xml.state, ObjectState.ROLLED_BACK).execute();
    }
}
