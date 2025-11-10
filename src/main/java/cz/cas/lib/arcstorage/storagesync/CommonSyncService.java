package cz.cas.lib.arcstorage.storagesync;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.SystemState;
import cz.cas.lib.arcstorage.jms.JmsQueueManager;
import cz.cas.lib.arcstorage.jms.JmsQueueNotEmptyException;
import cz.cas.lib.arcstorage.service.StorageProvider;
import cz.cas.lib.arcstorage.service.SystemStateService;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class CommonSyncService {

    private StorageProvider storageProvider;
    private JmsQueueManager jmsQueueManager;
    private int transactionTimeoutSeconds;
    private int synchronizationInitTimeoutSeconds;
    private SystemStateService systemStateService;

    /**
     * <ol>
     *     <li>sets storage to readonly mode</li>
     *     <li>waits until all queues are empty or wait timeout expires</li>
     *     <li>writes storage service for provided storage</li>
     * </ol>
     * <ul>
     *     <li>in case of success returns storage service nad leaves system in readonly mode, caller is responsible for followup actions</li>
     *     <li>in case of failure exception is thrown and storage is set to readwrite mode</li>
     *     <li>wait timeout is considered a failure - system is still processing objects</li>
     * </ul>
     *
     * @param systemState
     * @param storage
     * @return
     * @throws SomeLogicalStoragesNotReachableException
     * @throws InterruptedException
     * @throws JmsQueueNotEmptyException
     */
    public StorageService createStorageServiceInReadonlyVacuum(SystemState systemState, Storage storage) throws SomeLogicalStoragesNotReachableException, InterruptedException, JmsQueueNotEmptyException {
        StorageService destinationStorageService;
        try {
            //not checking reachability since that may end up upserting the storage in DB which is not intended since the storage is not inserted yet
            destinationStorageService = storageProvider.createAdapter(storage, false);
        } catch (Exception e) {
            log.error("Could not create storage service for  " + storage);
            throw e;
        }
        storage.setReachable(destinationStorageService.testConnection());
        if (!storage.isReachable()) {
            log.error("Storage " + storage + " not reachable.");
            throw new SomeLogicalStoragesNotReachableException(destinationStorageService.getStorage());
        }
        log.debug(storage + " reachable, waiting for processing objects to finish");
        systemStateService.setReadOnly(systemState, null);

        Thread.sleep(transactionTimeoutSeconds * 1000L);
        boolean allQueuesEmpty = jmsQueueManager.checkAllQueuesEmpty();
        int waitedSeconds = transactionTimeoutSeconds;
        while (!allQueuesEmpty) {
            log.debug("cant continue because queues are not empty - some objects are still processing: " +
                    "Archival storage will wait max. " + synchronizationInitTimeoutSeconds +
                    " seconds for processing objects to finish. Already waited " + waitedSeconds + " seconds");
            if (waitedSeconds > synchronizationInitTimeoutSeconds) {
                log.error("waited too long for processing objects to finish");
                systemStateService.setReadWrite(systemState);
                throw new JmsQueueNotEmptyException("can't continue since some JMS queue is not empty");
            }
            Thread.sleep(1000);
            waitedSeconds++;
            allQueuesEmpty = jmsQueueManager.checkAllQueuesEmpty();
        }
        return destinationStorageService;
    }

    @Autowired
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    @Autowired
    public void setJmsQueueManager(JmsQueueManager jmsQueueManager) {
        this.jmsQueueManager = jmsQueueManager;
    }

    @Autowired
    public void setTransactionTimeoutSeconds(@Value("${arcstorage.stateChangeTransactionTimeout}") int transactionTimeoutSeconds) {
        this.transactionTimeoutSeconds = transactionTimeoutSeconds;
    }

    @Autowired
    public void setSynchronizationInitTimeoutSeconds(@Value("${arcstorage.synchronizationInitTimeout}") int synchronizationInitTimeoutSeconds) {
        this.synchronizationInitTimeoutSeconds = synchronizationInitTimeoutSeconds;
    }

    @Autowired
    public void setSystemStateService(SystemStateService systemStateService) {
        this.systemStateService = systemStateService;
    }
}
