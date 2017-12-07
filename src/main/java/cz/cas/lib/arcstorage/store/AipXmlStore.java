package cz.cas.lib.arcstorage.store;

import cz.cas.lib.arcstorage.domain.*;
import cz.cas.lib.arcstorage.exception.MissingObject;
import lombok.extern.log4j.Log4j;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@Log4j
public class AipXmlStore extends DomainStore<AipXml, QAipXml> {
    public AipXmlStore() {
        super(AipXml.class, QAipXml.class);
    }

    public int getNextXmlVersionNumber(String sipId) {
        QAipXml xml = qObject();
        Integer lastVersion = query().select(xml.version.max()).where(xml.sip.id.eq(sipId)).fetchFirst();
        if (lastVersion == null) {
            log.warn(String.format("Could not find any XML version of AIP: %s", sipId));
            throw new MissingObject(AipSip.class, sipId);
        }
        return 1 + lastVersion;
    }

    /**
     * @return all XMLs in processing state which SIP is not in processing state
     */
    public List<AipXml> findUnfinishedXmls() {
        QAipXml xml = qObject();
        return (List<AipXml>) query().where(xml.state.eq(XmlState.PROCESSING)).where(xml.sip.state.eq(AipState.PROCESSING).not()).fetch();
    }

    public void rollbackUnfinishedXmlsRecords() {
        QAipXml xml = qObject();
        queryFactory.update(xml).where(xml.state.eq(XmlState.PROCESSING)).set(xml.state, XmlState.ROLLBACKED).execute();
    }
}
