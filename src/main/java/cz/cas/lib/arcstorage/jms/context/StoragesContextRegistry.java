package cz.cas.lib.arcstorage.jms.context;

import cz.cas.lib.arcstorage.jms.JmsAction;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class StoragesContextRegistry {
    private final ConcurrentMap<String, StorageContext> storages = new ConcurrentHashMap<>();

    public void registerStorage(String dbId, boolean primary) {
        storages.put(dbId, new StorageContext(dbId, primary, new ConcurrentHashMap<>()));
    }

    /**
     * @param storageId
     * @param msgId
     * @param objectDbId
     * @param jmsAction
     * @return Pair with currently registered object and flag indicating whether the registration of new object succeeded
     * <ul>
     *     <li>if the registration was successful the newly registered object and true is returned</li>
     *     <li>if the registration was not successful the other, already registered object and false is returned</li>
     * </ul>
     */
    public synchronized Pair<ProcessingObjectContext, Boolean> registerProcessingObject(String storageId, String msgId, String objectDbId, JmsAction jmsAction) {
        ConcurrentMap<String, ProcessingObjectContext> processingObjects = storages.get(storageId).getProcessingObjects();
        ProcessingObjectContext alreadyRegisteredObject = processingObjects.get(objectDbId);
        if (alreadyRegisteredObject != null) {
            return Pair.of(alreadyRegisteredObject, false);
        }
        ProcessingObjectContext newObject = new ProcessingObjectContext(msgId, objectDbId, jmsAction, new AtomicBoolean(false), new AtomicBoolean(false));
        processingObjects.put(objectDbId, newObject);
        return Pair.of(newObject, true);
    }

    public synchronized void unregisterProcessingObject(String storageId, String objectDbId) {
        ConcurrentMap<String, ProcessingObjectContext> processingObjects = storages.get(storageId).getProcessingObjects();
        processingObjects.remove(objectDbId);
    }

    public ProcessingObjectContext getProcessingObjectContext(String storageId, String objectDbId) {
        return storages.get(storageId).getProcessingObjects().get(objectDbId);
    }

    public List<ProcessingObjectContext> findProcessingObjectContexts(String objectDbId) {
        List<ProcessingObjectContext> list = new ArrayList<>();
        for (String storageId : storages.keySet()) {
            ProcessingObjectContext processingObjectContext = storages.get(storageId).getProcessingObjects().get(objectDbId);
            if (processingObjectContext != null) {
                list.add(processingObjectContext);
            }
        }
        return list;
    }
}
