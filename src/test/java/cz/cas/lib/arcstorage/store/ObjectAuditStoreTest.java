package cz.cas.lib.arcstorage.store;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.security.user.UserStore;
import cz.cas.lib.arcstorage.storagesync.AuditedOperation;
import cz.cas.lib.arcstorage.storagesync.ObjectAudit;
import cz.cas.lib.arcstorage.storagesync.ObjectAuditStore;
import helper.DbTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class ObjectAuditStoreTest extends DbTest {

    private ObjectAuditStore objectAuditStore = new ObjectAuditStore();
    private UserStore userStore = new UserStore();

    @Before
    public void before() {
        initializeStores(objectAuditStore, userStore);
    }

    @Test
    public void findAuditsOfModifyOps() throws InterruptedException {
        User user = new User();
        userStore.save(user);
        ArchivalObject obj = new ArchivalObject();
        ObjectAudit oa1 = new ObjectAudit(obj, user, AuditedOperation.ROLLBACK);
        ObjectAudit oa2 = new ObjectAudit(obj, user, AuditedOperation.ARCHIVAL_RETRY);
        ObjectAudit oa3 = new ObjectAudit(obj, user, AuditedOperation.REMOVAL);
        ObjectAudit oa4 = new ObjectAudit(obj, user, AuditedOperation.RENEWAL);
        ObjectAudit oa5 = new ObjectAudit(obj, user, AuditedOperation.DELETION);
        ArchivalObject otherEarlyObj = new ArchivalObject();
        ObjectAudit otherEarlyObjA = new ObjectAudit(otherEarlyObj, user, AuditedOperation.ROLLBACK);
        ArchivalObject otherLateObj = new ArchivalObject();
        ObjectAudit otherLateObjA = new ObjectAudit(otherLateObj, user, AuditedOperation.RENEWAL);

        ObjectAudit[] allAudits = new ObjectAudit[]{oa1, otherEarlyObjA, oa2, oa3, oa4, otherLateObjA, oa5};
        for (int i = 0; i < allAudits.length; i++) {
            Thread.sleep(100);
            allAudits[i] = objectAuditStore.save(allAudits[i]);
        }

        List<ObjectAudit> auditsOfModifyOps = objectAuditStore.findAuditsForSync(null, null);
        assertThat(auditsOfModifyOps, contains(otherEarlyObjA, otherLateObjA, oa5));

        auditsOfModifyOps = objectAuditStore.findAuditsForSync(allAudits[0].getCreated(), allAudits[0].getCreated());
        assertThat(auditsOfModifyOps, contains(oa1));

        auditsOfModifyOps = objectAuditStore.findAuditsForSync(null, allAudits[3].getCreated());
        assertThat(auditsOfModifyOps, contains(otherEarlyObjA, oa3));

        auditsOfModifyOps = objectAuditStore.findAuditsForSync(allAudits[1].getCreated(), allAudits[5].getCreated());
        assertThat(auditsOfModifyOps, contains(otherEarlyObjA, oa4, otherLateObjA));
    }

    @After
    public void after() throws SQLException {
        clearDatabase();
    }
}
