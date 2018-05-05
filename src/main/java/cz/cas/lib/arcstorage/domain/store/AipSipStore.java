package cz.cas.lib.arcstorage.domain.store;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.domain.QAipSip;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class AipSipStore extends DomainStore<AipSip, QAipSip> {
    public AipSipStore() {
        super(AipSip.class, QAipSip.class);
    }

    /**
     * @return all SIPs in processing || failed state
     */
    public List<AipSip> findUnfinishedSips() {
        QAipSip sip = qObject();
        return (List<AipSip>) query().where(sip.state.in(ObjectState.PROCESSING, ObjectState.FAILED)).fetch();
    }

    public void rollbackUnfinishedSipsRecords() {
        QAipSip sip = qObject();
        queryFactory.update(sip).where(sip.state.in(ObjectState.PROCESSING, ObjectState.FAILED)).set(sip.state, ObjectState.ROLLED_BACK).execute();
    }
}
