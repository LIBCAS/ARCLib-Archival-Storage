import cz.cas.lib.arcstorage.domain.AipSip;
import cz.cas.lib.arcstorage.domain.ObjectState;
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
        aipSip1.setState(ObjectState.FAILED);
        aipStore.save(aipSip1);

        AipSip aipSip2 = new AipSip();
        aipSip2.setState(ObjectState.PROCESSING);
        aipStore.save(aipSip2);

        AipSip aipSip3 = new AipSip();
        aipSip3.setState(ObjectState.ARCHIVED);
        aipStore.save(aipSip3);

        AipSip aipSip4 = new AipSip();
        aipSip4.setState(ObjectState.ROLLED_BACK);
        aipStore.save(aipSip4);

        AipSip aipSip5 = new AipSip();
        aipSip5.setState(ObjectState.DELETED);
        aipStore.save(aipSip5);

        AipSip aipSip6 = new AipSip();
        aipSip6.setState(ObjectState.REMOVED);
        aipStore.save(aipSip6);

        flushCache();

        List<AipSip> unfinishedSips = aipStore.findUnfinishedSips();

        List<ObjectState> states = unfinishedSips.stream()
                .map(AipSip::getState)
                .collect(Collectors.toList());

        assertThat(states.containsAll(asList(ObjectState.FAILED, ObjectState.PROCESSING)));
    }
}
