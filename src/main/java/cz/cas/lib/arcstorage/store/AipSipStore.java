package cz.cas.lib.arcstorage.store;

import cz.cas.lib.arcstorage.domain.AipSip;
import cz.cas.lib.arcstorage.domain.AipState;
import cz.cas.lib.arcstorage.domain.QAipSip;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class AipSipStore extends DomainStore<AipSip, QAipSip> {
    public AipSipStore() {
        super(AipSip.class, QAipSip.class);
    }

    /**
     * @return all SIPs in processing state
     */
    public List<AipSip> findUnfinishedSips() {
        QAipSip sip = qObject();
        return (List<AipSip>) query().where(sip.state.eq(AipState.PROCESSING)).fetch();
    }

    public void rollbackUnfinishedSipsRecords() {
        QAipSip sip = qObject();
        queryFactory.update(sip).where(sip.state.eq(AipState.PROCESSING)).set(sip.state, AipState.ROLLBACKED).execute();
    }
}
