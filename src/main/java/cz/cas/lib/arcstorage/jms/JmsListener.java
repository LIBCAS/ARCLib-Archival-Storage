package cz.cas.lib.arcstorage.jms;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.dto.AipDto;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.dto.StorageQueueDto;
import cz.cas.lib.arcstorage.jms.context.ProcessingObjectContext;
import cz.cas.lib.arcstorage.jms.context.StoragesContextRegistry;
import cz.cas.lib.arcstorage.service.*;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storagesync.newstorage.StorageSyncStatus;
import cz.cas.lib.arcstorage.storagesync.newstorage.StorageSyncStatusStore;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.TextMessage;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class JmsListener implements MessageListener {

    private ObjectMapper om;
    private JmsQueueProcessor jmsQueueProcessor;
    private StorageAdministrationService storageAdministrationService;
    private StorageProvider storageProvider;
    private ArchivalDbService archivalDbService;
    private SystemStateService systemStateService;
    private StoragesContextRegistry storagesContext;
    private int jmsMessageProcessTimeout;
    private StorageSyncStatusStore storageSyncStatusStore;

    @Override
    public void onMessage(Message msg) {
        String storageId;
        JmsAction action;
        String stringMsg;
        Set<String> objectDbIds = new HashSet<>();
        try {
            storageId = ((ActiveMQQueue) msg.getJMSDestination()).getQueueName().replace(JmsConstants.QUEUE_PREFIX, "");
            action = JmsAction.valueOf(msg.getStringProperty(JmsConstants.HEADER_ACTION));
            stringMsg = ((TextMessage) msg).getText();
        } catch (JMSException e) {
            systemStateService.setReadOnly(systemStateService.get(), e);
            throw new RuntimeException(e);
        }

        boolean primaryStorage = storageProvider.getPrimaryStorage().getId().equals(storageId);

        //timeout is only set for write requests on secondary storages
        //timeout thread sets the flag to true if timeout is reached, timeout also calls handleFailure in that case
        //parent thread should check this flag at least at the end of the onMessage handler
        //if its true parent thread should throw exception (so that JMS message is not auto-ACKed)
        //if its true parent thread should not call handleFailure since it is called from the timeout thread itself
        //if its true parent can skip some operations, for example it can check for it regularly and skip some writes
        //if its false parent thread should interrupt the timeout thread so that it does not fire handleFailure once it ends while the parent already ended successfully before
        AtomicBoolean timeoutReached = new AtomicBoolean(false);
        Thread timeoutThread = null;

        try {
            StorageService targetStorage = storageProvider.createAdapter(storageId);
            if (targetStorage.getStorage().isReachable()) {
                switch (action) {
                    case SAVE_AIP -> {
                        StorageQueueDto<AipDto> dto = parseJson(stringMsg, AipDto.class);
                        String sipDbId = dto.getData().getSip().getDatabaseId();
                        String xmlDbId = dto.getData().getXml().getDatabaseId();

                        ProcessingObjectContext objContext = waitAndRegisterObjectProcess(storageId, msg.getJMSMessageID(), sipDbId, action);
                        waitAndRegisterObjectProcess(storageId, msg.getJMSMessageID(), xmlDbId, action);
                        objectDbIds.add(sipDbId);
                        objectDbIds.add(xmlDbId);
                        if (!primaryStorage) {
                            timeoutThread = startTimeoutThread(msg, storageId, action, objectDbIds, timeoutReached, objContext);
                        }
                        jmsQueueProcessor.saveAip(targetStorage, dto.getData(), dto.getUserId());
                    }
                    case SAVE -> {
                        StorageQueueDto<ArchivalObjectDto> dto = parseJson(stringMsg, ArchivalObjectDto.class);
                        ProcessingObjectContext objContext = waitAndRegisterObjectProcess(storageId, msg.getJMSMessageID(), dto.getData().getDatabaseId(), action);
                        objectDbIds.add(dto.getData().getDatabaseId());
                        Instant timestamp = dto.getOperationTimestamp();
                        if (!primaryStorage) {
                            timeoutThread = startTimeoutThread(msg, storageId, action, objectDbIds, timeoutReached, objContext);
                        }
                        jmsQueueProcessor.saveObject(targetStorage, dto.getData(), timestamp != null ? timestamp : Instant.now(), dto.getUserId());
                    }
                    case DELETE -> {
                        StorageQueueDto<ArchivalObjectDto> dto = parseJson(stringMsg, ArchivalObjectDto.class);
                        waitAndRegisterObjectProcess(storageId, msg.getJMSMessageID(), dto.getData().getDatabaseId(), action);
                        objectDbIds.add(dto.getData().getDatabaseId());
                        jmsQueueProcessor.delete(targetStorage, dto.getData(), dto.getOperationTimestamp());
                    }
                    case ROLLBACK -> {
                        StorageQueueDto<ArchivalObjectDto> dto = parseJson(stringMsg, ArchivalObjectDto.class);
                        waitAndRegisterObjectProcess(storageId, msg.getJMSMessageID(), dto.getData().getDatabaseId(), action);
                        objectDbIds.add(dto.getData().getDatabaseId());
                        jmsQueueProcessor.rollback(targetStorage, dto.getData(), dto.getOperationTimestamp());
                    }
                    case RENEW -> {
                        StorageQueueDto<ArchivalObjectDto> dto = parseJson(stringMsg, ArchivalObjectDto.class);
                        waitAndRegisterObjectProcess(storageId, msg.getJMSMessageID(), dto.getData().getDatabaseId(), action);
                        objectDbIds.add(dto.getData().getDatabaseId());
                        jmsQueueProcessor.renew(targetStorage, dto.getData(), dto.getOperationTimestamp());
                    }
                    case REMOVE -> {
                        StorageQueueDto<ArchivalObjectDto> dto = parseJson(stringMsg, ArchivalObjectDto.class);
                        waitAndRegisterObjectProcess(storageId, msg.getJMSMessageID(), dto.getData().getDatabaseId(), action);
                        objectDbIds.add(dto.getData().getDatabaseId());
                        jmsQueueProcessor.remove(targetStorage, dto.getData(), dto.getOperationTimestamp());
                    }
                    case FORGET -> {
                        StorageQueueDto<ArchivalObjectDto> dto = parseJson(stringMsg, ArchivalObjectDto.class);
                        waitAndRegisterObjectProcess(storageId, msg.getJMSMessageID(), dto.getData().getDatabaseId(), action);
                        objectDbIds.add(dto.getData().getDatabaseId());
                        jmsQueueProcessor.forget(targetStorage, dto.getData(), dto.getOperationTimestamp());
                    }
                    case COPY -> {
                        StorageQueueDto<ArchivalObjectDto> dto = parseJson(stringMsg, ArchivalObjectDto.class);
                        ProcessingObjectContext objContext = waitAndRegisterObjectProcess(storageId, msg.getJMSMessageID(), dto.getData().getDatabaseId(), action);
                        objectDbIds.add(dto.getData().getDatabaseId());
                        if (!primaryStorage) {
                            timeoutThread = startTimeoutThread(msg, storageId, action, objectDbIds, timeoutReached, objContext);
                        }
                        jmsQueueProcessor.copy(targetStorage, dto.getData(), dto.getOperationTimestamp());
                    }
                    case ACTIVATE_DATASPACE -> {
                        jmsQueueProcessor.activateDataspace(targetStorage, stringMsg);
                    }
                }
                unregisterObjectsProcess(storageId, objectDbIds);
            } else {
                handleFailure(msg, storageId, primaryStorage, action, objectDbIds, new SomeLogicalStoragesNotReachableException(targetStorage.getStorage()));
            }
        } catch (Exception e) {
            if (timeoutReached.get()) {
                throw new JmsConsumerTimeoutException(getLogId(msg, action, storageId) + " already reached timeout before and went through error handling, now the original (hanging) thread ended with exception", e);
            } else {
                if (timeoutThread != null) {
                    timeoutThread.interrupt();
                }
                handleFailure(msg, storageId, primaryStorage, action, objectDbIds, e);
            }
        }

        if (timeoutReached.get()) {
            throw new JmsConsumerTimeoutException(getLogId(msg, action, storageId) + " already reached timeout before and went through error handling, now the original (hanging) thread successfully finished");
        } else {
            if (timeoutThread != null) {
                timeoutThread.interrupt();
            }
            if (action == JmsAction.COPY) {
                synchronized (storageSyncStatusStore) {
                    StorageSyncStatus status = storageSyncStatusStore.findSyncStatusOfStorage(storageId);
                    status.setDone(status.getDone() + 1);
                    storageSyncStatusStore.save(status);
                }
            }
        }
    }

    private void handleFailure(Message message, String storageId, boolean primaryStorage, JmsAction action, Set<String> objectDbIds, Exception error) {
        if (primaryStorage) {
            log.error(getLogId(message, action, storageId), error);
            ObjectState failoverState = null;
            ObjectState expectedState = null;
            switch (action) {
                case ROLLBACK -> {
                    expectedState = ObjectState.ROLLED_BACK;
                    failoverState = ObjectState.ROLLBACK_FAILURE;
                }
                case DELETE -> {
                    expectedState = ObjectState.DELETED;
                    failoverState = ObjectState.DELETION_FAILURE;
                }
            }
            ObjectState expectedStateFinal = expectedState;
            if (failoverState != null) {
                if (objectDbIds.isEmpty()) {
                    throw new IllegalStateException("encountered error with action " + action + " at storage " + storageId + " .. tried to set failover state " + failoverState + " but the objectDbIds argument was empty");
                }

                //working around scenarios like:
                //2 requests are issued - DELETE then ROLLBACK, state in DB is ROLLED_BACK
                //DELETE msg fails and sets state to DELETION_FAILURE
                //ROLLBACK msg succeeds...
                //this would happen without filter.. with the filter DELETION msg failure does not set failover state if
                //the state in DB has changed since
                String[] filteredIds = objectDbIds.stream().filter(id -> {
                    ArchivalObject objInDb = archivalDbService.lookForObject(id);
                    return objInDb != null && objInDb.getState() == expectedStateFinal;
                }).toArray(String[]::new);

                if (filteredIds.length > 0) {
                    archivalDbService.setObjectsState(failoverState, filteredIds);
                }
            }
            unregisterObjectsProcess(storageId, objectDbIds);
        } else {
            storageAdministrationService.detachStorage(storageId, error);
            for (String objectDbId : objectDbIds) {
                //working around scenario mentioned above for secondary storage:
                //set stop signal in consumer of the first message instead of removing the object from context
                //if there are other consumer threads waiting in waitAndRegisterObjectProcess
                //they can see the signal and immediate end
                //therefore the second message on the same object fails too
                storagesContext.getProcessingObjectContext(storageId, objectDbId).getParallelRequestFailed().set(true);
            }
            throw new RuntimeException(error);
        }
    }

    /**
     * returns null if timeout is disabled in config
     *
     * @param message
     * @param storageId
     * @param action
     * @param objectDbIds
     * @param timeoutReached
     * @param objectContext
     * @return
     */
    private Thread startTimeoutThread(Message message, String storageId, JmsAction action, Set<String> objectDbIds, AtomicBoolean timeoutReached, ProcessingObjectContext objectContext) {
        if (jmsMessageProcessTimeout == -1) {
            return null;
        }
        Thread parentThread = Thread.currentThread();
        Thread timeoutThread = new Thread(() -> {
            try {
                Thread.sleep(jmsMessageProcessTimeout * 1000);
                timeoutReached.set(true);
                //might free the parent thread since it might check for the signal somewhere
                objectContext.getStopSignal().set(true);
                //might free the parent thread since it might be sleeping somewhere
                parentThread.interrupt();
                handleFailure(message, storageId, false, action, objectDbIds, new JmsConsumerTimeoutException(getLogId(message, action, storageId)));
            } catch (InterruptedException e) {
                //nothing to do, interrupting is valid use case
            }
        });
        timeoutThread.start();
        return timeoutThread;
    }

    @SneakyThrows
    private <T extends ArchivalObjectJmsDto> StorageQueueDto<T> parseJson(String json, Class<T> dataClazz) {
        com.fasterxml.jackson.databind.JavaType javaType = om.getTypeFactory().constructParametricType(StorageQueueDto.class, dataClazz);
        return om.readValue(json, javaType);
    }

    private void unregisterObjectsProcess(String storageId, Set<String> objDbIds) {
        for (String objectDbId : objDbIds) {
            storagesContext.unregisterProcessingObject(storageId, objectDbId);
        }
    }

    private ProcessingObjectContext waitAndRegisterObjectProcess(String storageId, String msgId, String objDbId, JmsAction action) throws InterruptedException {
        Pair<ProcessingObjectContext, Boolean> registeredObject = storagesContext.registerProcessingObject(storageId, msgId, objDbId, action);
        long sleptSeconds = 0;
        long logInterval = 1;

        while (!registeredObject.getRight()) {

            if (registeredObject.getLeft().getParallelRequestFailed().get()) {
                throw new JmsConsumerStoppedByOtherThreadException(getLogId(msgId, action, storageId) + " stopped because of failed " +
                        getLogId(registeredObject.getLeft().getMessageId(), registeredObject.getLeft().getAction(), storageId));
            }

            Thread.sleep(1000);
            sleptSeconds++;

            if (sleptSeconds == logInterval) {
                log.debug("Object already processing by other thread: {}, waiting to obtain lock, already waited {} seconds", registeredObject.getLeft(), sleptSeconds);
                logInterval = logInterval * 3;
            }

            registeredObject = storagesContext.registerProcessingObject(storageId, msgId, objDbId, action);
        }

        if (sleptSeconds > 0) {
            log.info("Lock for object {} obtained after {} seconds", objDbId, sleptSeconds);
        }
        return registeredObject.getLeft();
    }

    private String getLogId(Message message, JmsAction action, String storageId) {
        try {
            return getLogId(message.getJMSMessageID(), action, storageId);
        } catch (JMSException e) {
            return "process of action: " + action + ", storage ID: " + storageId;
        }
    }

    private String getLogId(String jmsMessageId, JmsAction action, String storageId) {
        return "process of message ID: " + jmsMessageId + ", action: " + action + ", storage ID: " + storageId;
    }


    @Autowired
    public void setOm(ObjectMapper om) {
        this.om = om;
    }

    @Autowired
    public void setJmsQueueProcessor(JmsQueueProcessor jmsQueueProcessor) {
        this.jmsQueueProcessor = jmsQueueProcessor;
    }

    @Autowired
    public void setStorageAdministrationService(StorageAdministrationService storageAdministrationService) {
        this.storageAdministrationService = storageAdministrationService;
    }

    @Autowired
    public void setStorageProvider(StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    @Autowired
    public void setArchivalDbService(ArchivalDbService archivalDbService) {
        this.archivalDbService = archivalDbService;
    }

    @Autowired
    public void setSystemStateService(SystemStateService systemStateService) {
        this.systemStateService = systemStateService;
    }

    @Autowired
    public void setStoragesContext(StoragesContextRegistry storagesContext) {
        this.storagesContext = storagesContext;
    }

    @Autowired
    public void setJmsMessageProcessTimeout(@Value("${arcstorage.jmsMessageProcessTimeout}") int jmsMessageProcessTimeout) {
        this.jmsMessageProcessTimeout = jmsMessageProcessTimeout;
    }

    @Autowired
    public void setStorageSyncStatusStore(StorageSyncStatusStore storageSyncStatusStore) {
        this.storageSyncStatusStore = storageSyncStatusStore;
    }
}
