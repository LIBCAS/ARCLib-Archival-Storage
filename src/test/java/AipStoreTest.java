import com.querydsl.jpa.impl.JPAQueryFactory;
import cz.cas.lib.arcstorage.domain.AipSip;
import cz.cas.lib.arcstorage.domain.AipState;
import cz.cas.lib.arcstorage.store.AipSipStore;
import helper.DbTest;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.util.Utils.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class AipStoreTest extends DbTest {
    private AipSipStore aipStore;

    @Before
    public void before() {
        aipStore = new AipSipStore();
        initializeStores(aipStore);
    }
    @Test
    public void testFindUnfinishedSips() {
        AipSip aipSip1 = new AipSip();
        aipSip1.setState(AipState.FAILED);
        aipStore.save(aipSip1);

        AipSip aipSip2 = new AipSip();
        aipSip2.setState(AipState.PROCESSING);
        aipStore.save(aipSip2);

        AipSip aipSip3 = new AipSip();
        aipSip3.setState(AipState.ARCHIVED);
        aipStore.save(aipSip3);

        AipSip aipSip4 = new AipSip();
        aipSip4.setState(AipState.ROLLBACKED);
        aipStore.save(aipSip4);

        AipSip aipSip5 = new AipSip();
        aipSip5.setState(AipState.DELETED);
        aipStore.save(aipSip5);

        AipSip aipSip6 = new AipSip();
        aipSip6.setState(AipState.REMOVED);
        aipStore.save(aipSip6);

        flushCache();

        List<AipSip> unfinishedSips = aipStore.findUnfinishedSips();

        List<AipState> states = unfinishedSips.stream()
                .map(AipSip::getState)
                .collect(Collectors.toList());

        assertThat(states.containsAll(asList(AipState.FAILED, AipState.PROCESSING)));
    }
}
