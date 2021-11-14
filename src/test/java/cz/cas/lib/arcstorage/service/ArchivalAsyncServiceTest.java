package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.*;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ChecksumType;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.storage.exception.IOStorageException;
import cz.cas.lib.arcstorage.storage.fs.LocalFsProcessor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;

import static cz.cas.lib.arcstorage.util.Utils.asList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class ArchivalAsyncServiceTest {
    private static final ArchivalAsyncService service = new ArchivalAsyncService();
    @Mock
    private ArchivalDbService archivalDbService;
    @Mock
    private ArcstorageMailCenter mailCenter;
    @Mock
    private LocalFsProcessor localFsProcessor;
    private static final User USER = new User(UUID.randomUUID().toString(), null, null, "SPACE", null, null);
    private Storage s;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        service.setArchivalDbService(archivalDbService);
        service.setMailCenter(mailCenter);
        service.setExecutor(Executors.newFixedThreadPool(1));
        s = new Storage();
        s.setName("name");
        when(localFsProcessor.getStorage()).thenReturn(s);
    }


    @Test
    public void cleanUp() throws Exception {
        Checksum c = new Checksum(ChecksumType.MD5, "placeholder");
        ArchivalObject o1 = new ArchivalObject(c, USER, ObjectState.ARCHIVAL_FAILURE);
        ArchivalObject o2 = new ArchivalObject(c, USER, ObjectState.DELETION_FAILURE);
        ArchivalObject o3 = new ArchivalObject(c, USER, ObjectState.ROLLBACK_FAILURE);
        AipSip s1 = new AipSip(UUID.randomUUID().toString(), c, USER, ObjectState.PROCESSING);
        AipXml x1 = new AipXml(UUID.randomUUID().toString(), c, USER, s1, 1, ObjectState.DELETION_FAILURE);
        AipXml x2 = new AipXml(UUID.randomUUID().toString(), c, USER, s1, 2, ObjectState.PRE_PROCESSING);
        List<ArchivalObject> objects = asList(o1, o2, o3, s1, x1, x2);

        doThrow(new IOStorageException(s)).when(localFsProcessor).rollbackObject(x2.toDto(), USER.getDataSpace());
        doThrow(new IOStorageException(s)).when(localFsProcessor).delete(o2.toDto(), USER.getDataSpace(), false);

        service.cleanUp(objects, asList(localFsProcessor));

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(archivalDbService).setObjectsState(eq(ObjectState.DELETED), captor.capture());
        assertThat(captor.getAllValues(), containsInAnyOrder(x1.getId()));

        captor = ArgumentCaptor.forClass(String.class);
        verify(archivalDbService).setObjectsState(eq(ObjectState.ROLLED_BACK), captor.capture());
        assertThat(captor.getAllValues(), containsInAnyOrder(o1.getId(), s1.getId(), o3.getId()));

        verify(archivalDbService, times(1)).setObjectsState(any(), any());
        verify(archivalDbService, times(1)).setObjectsState(any(), any(), any(), any());

    }
}
