package cz.cas.lib.arcstorage.jms;

import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.dto.AipDto;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.StorageBasicDto;
import cz.cas.lib.arcstorage.dto.StorageQueueDto;
import jakarta.jms.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.command.ActiveMQQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.Lifecycle;
import org.springframework.context.annotation.Lazy;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.config.SimpleJmsListenerEndpoint;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Component
public class JmsQueueManager {

    private static final int SAFE_QUEUE_DELETE_AFTER_LISTENER_SHUTDOWN_MS = 3000;

    private JmsListenerEndpointRegistry registry;
    private ConnectionFactory connectionFactory;
    private JmsListenerContainerFactory<?> listenerContainerFactory;
    private JmsListener jmsListener;
    private ObjectMapper objectMapper;
    private StorageStore storageStore;
    private int jmsQueueConsumerThreads;

    @SneakyThrows
    public boolean checkAllQueuesEmpty() {
        try (Connection connection = connectionFactory.createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            for (StorageBasicDto storage : storageStore.getAllAsDtos()) {
                QueueBrowser browser = session.createBrowser(new ActiveMQQueue(JmsConstants.QUEUE_PREFIX + storage.getId()));
                if (browser.getEnumeration().hasMoreElements()) {
                    return false;
                }
            }
            return true;
        }
    }

    public void assertQueuesEmpty(String storageId) throws JMSException, JmsQueueNotEmptyException {
        try (Connection connection = connectionFactory.createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            QueueBrowser browser = session.createBrowser(new ActiveMQQueue(JmsConstants.QUEUE_PREFIX + storageId));
            if (browser.getEnumeration().hasMoreElements()) {
                throw new JmsQueueNotEmptyException("queue of storage " + storageId + " is not empty");
            }
        }
    }

    /**
     * walks through all queues and removes requests to store objects if the requests are older then passed timestamp
     * <p>
     * walking through queues might take some time, hence this operation is ment to be used with batch of ids
     * </p>
     *
     * @param objectDbIdsToRemove
     * @param olderThen
     */
    @SneakyThrows
    public void deleteNotProcessingStoreRequests(Set<String> objectDbIdsToRemove, Instant olderThen) {
        try (Connection connection = connectionFactory.createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            for (StorageBasicDto storage : storageStore.getAllAsDtos()) {
                ActiveMQQueue queue = new ActiveMQQueue(JmsConstants.QUEUE_PREFIX + storage.getId());
                QueueBrowser browser = session.createBrowser(queue);
                Enumeration<?> messages = browser.getEnumeration();
                while (messages.hasMoreElements()) {
                    Message message = (Message) messages.nextElement();
                    if (!Instant.ofEpochMilli(message.getJMSTimestamp()).isBefore(olderThen)) {
                        continue;
                    }
                    JmsAction action = JmsAction.valueOf(message.getStringProperty(JmsConstants.HEADER_ACTION));
                    Set<String> objectDbIdsInMessage = new HashSet<>();
                    switch (action) {
                        case SAVE ->
                                objectDbIdsInMessage.add(parseJson(message.getBody(String.class), ArchivalObjectDto.class).getData().getDatabaseId());
                        case SAVE_AIP -> {
                            AipDto data = parseJson(message.getBody(String.class), AipDto.class).getData();
                            objectDbIdsInMessage.add(data.getSip().getDatabaseId());
                            objectDbIdsInMessage.add(data.getXml().getDatabaseId());
                        }
                    }
                    if (objectDbIdsToRemove.containsAll(objectDbIdsInMessage)) {
                        try (MessageConsumer consumer = session.createConsumer(queue, "JMSMessageID = '" + message.getJMSMessageID() + "'")) {
                            Message msg = consumer.receive(1000);
                            if (msg != null) {
                                log.info("removed message {} from queue {}", message.getBody(String.class), queue.getQueueName());
                            }
                        }
                    }
                }
            }
        }
    }

    public void startListener(String storageId) {
        Optional.ofNullable(registry.getListenerContainer(storageId)).ifPresentOrElse(Lifecycle::start, () -> {
            SimpleJmsListenerEndpoint endpoint = new SimpleJmsListenerEndpoint();
            endpoint.setId(storageId);
            endpoint.setDestination(JmsConstants.QUEUE_PREFIX + storageId);
            endpoint.setMessageListener(jmsListener);
            endpoint.setConcurrency(String.valueOf(jmsQueueConsumerThreads));
            registry.registerListenerContainer(endpoint, listenerContainerFactory, true);
        });
    }

    /**
     * silently skips if the listener does not exist
     *
     * @param storageId
     */
    public void stopListener(String storageId) {
        Optional.ofNullable(registry.getListenerContainer(storageId)).ifPresent(Lifecycle::stop);
    }

    public void deleteQueue(String storageId) throws JMSException {
        Optional.ofNullable(registry.getListenerContainer(storageId)).ifPresent(l -> {
            l.stop();
            log.trace("waiting for consumer of queue {} to stop", storageId);
            try {
                Thread.sleep(SAFE_QUEUE_DELETE_AFTER_LISTENER_SHUTDOWN_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        try (Connection connection = connectionFactory.createConnection()) {
            connection.start();
            // Cast to ActiveMQ-specific classes
            org.apache.activemq.ActiveMQConnection activeMQConnection =
                    (org.apache.activemq.ActiveMQConnection) connection;

            // Use ActiveMQ admin features to delete the queue
            activeMQConnection.destroyDestination(new ActiveMQQueue(JmsConstants.QUEUE_PREFIX + storageId));
        }
    }

    @SneakyThrows
    private <T extends ArchivalObjectJmsDto> StorageQueueDto<T> parseJson(String json, Class<T> dataClazz) {
        com.fasterxml.jackson.databind.JavaType javaType = objectMapper.getTypeFactory().constructParametricType(StorageQueueDto.class, dataClazz);
        return objectMapper.readValue(json, javaType);
    }

    @Autowired
    public void setRegistry(JmsListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    @Autowired
    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Autowired
    public void setListenerContainerFactory(JmsListenerContainerFactory<?> listenerContainerFactory) {
        this.listenerContainerFactory = listenerContainerFactory;
    }

    @Autowired
    public void setJmsListener(@Lazy JmsListener jmsListener) {
        this.jmsListener = jmsListener;
    }

    @Autowired
    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Autowired
    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }

    @Autowired
    public void setJmsQueueConsumerThreads(@Value("${arcstorage.jmsQueueConsumerThreads}") int jmsQueueConsumerThreads) {
        this.jmsQueueConsumerThreads = jmsQueueConsumerThreads;
    }
}
