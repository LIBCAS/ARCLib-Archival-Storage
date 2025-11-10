package cz.cas.lib.arcstorage.jms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cas.lib.arcstorage.dto.AipDto;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.StorageQueueDto;
import cz.cas.lib.arcstorage.mail.ArcstorageMailCenter;
import cz.cas.lib.arcstorage.service.SystemStateService;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class JmsSender {

    private final Map<JmsPriority, JmsTemplate> jmsTemplates = new HashMap<>();
    private ObjectMapper objectMapper;
    private JmsTemplate defaultJmsTemplate;
    private SystemStateService systemStateService;
    private ArcstorageMailCenter arcstorageMailCenter;

    public void saveAip(String targetStorageId, AipDto dto, String userId) {
        send(targetStorageId, dto, JmsAction.SAVE_AIP, null, userId);
    }

    /**
     * @param storageId
     * @param dto
     * @param operationTimestamp if null then receiver will use Instant.now when it gets the message
     * @param userId             might be null when sending to secondary storages
     */
    public void saveObject(String storageId, ArchivalObjectDto dto, @Nullable Instant operationTimestamp, String userId) {
        send(storageId, dto, JmsAction.SAVE, operationTimestamp, userId);
    }

    public void activateDataspace(String storageId, String dataSpace) {
        JmsAction action = JmsAction.ACTIVATE_DATASPACE;
        jmsTemplates.get(action.getPriority()).send(JmsConstants.QUEUE_PREFIX + storageId, session -> {
                    TextMessage message = session.createTextMessage(dataSpace);
                    message.setStringProperty(JmsConstants.HEADER_ACTION, action.name());
                    return message;
                }
        );
    }

    public void modifyObject(String storageId, ArchivalObjectDto dto, @NotNull Instant operationTimestamp, JmsAction action) {
        switch (action) {
            case DELETE, ROLLBACK, REMOVE, RENEW, FORGET -> {
                send(storageId, dto, action, operationTimestamp, null);
            }
            default -> throw new IllegalArgumentException("modifyObject does not support action " + action);
        }
    }

    public void copyObject(String storageId, ArchivalObjectDto dto, @NotNull Instant operationTimestamp) {
        send(storageId, dto, JmsAction.COPY, operationTimestamp, null);
    }

    /**
     * tries to send and receive JMS healthcheck message, in case of failure sets system to readonly mode and throws exception
     * <p>recommended before every for cycle / singular JMS interaction</p>
     * <p>highly recommended in methods which set DB state and THEN send messages
     * into queue without reflecting general error with JMS connection</p>
     *
     * @throws JmsHealthCheckException or other exception if healthcheck fails
     */
    public void healthCheck() throws JmsHealthCheckException {
        healthCheck(true);
    }

    public void healthCheckWithoutSettingReadOnlyOnFailure() {
        try {
            healthCheck(false);
        } catch (Exception e) {
            arcstorageMailCenter.sendFatalError(e, false);
        }
    }

    private void healthCheck(boolean failureSetsReadOnlyMode) throws JmsHealthCheckException {
        log.trace("sending JMS healthcheck");
        JmsHealthCheckException exception;
        try {
            Message response = defaultJmsTemplate.sendAndReceive(JmsConstants.HEALTHCHECK_QUEUE, s -> s.createTextMessage("healthcheck"));
            if (response != null && "true".equals(response.getBody(String.class))) {
                return;
            }
            exception = new JmsHealthCheckException();
        } catch (Exception e) {
            exception = new JmsHealthCheckException(e);
        }
        if (failureSetsReadOnlyMode) {
            systemStateService.setReadOnly(systemStateService.get(), exception);
        }
        throw exception;
    }

    private <T extends ArchivalObjectJmsDto> void send(String targetStorageId, T data, JmsAction action, Instant operationTimestamp, String userId) {
        jmsTemplates.get(action.getPriority()).send(JmsConstants.QUEUE_PREFIX + targetStorageId, session -> {
                    TextMessage message = session.createTextMessage(createJsonMessage(new StorageQueueDto<>(data, operationTimestamp, userId)));
                    message.setStringProperty(JmsConstants.HEADER_ACTION, action.name());
                    return message;
                }
        );
    }

    private <T extends ArchivalObjectJmsDto> String createJsonMessage(StorageQueueDto<T> storageQueueDto) {
        String json;
        try {
            json = objectMapper.writeValueAsString(storageQueueDto);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return json;
    }

    @Autowired
    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Autowired
    public void setJmsTemplates(ConnectionFactory cf) {
        for (JmsPriority pri : JmsPriority.values()) {
            JmsTemplate template = new JmsTemplate(cf);
            template.setPriority(pri.getValue());
            template.setExplicitQosEnabled(true);
            template.afterPropertiesSet();
            jmsTemplates.put(pri, template);
        }
    }

    @Autowired
    public void setDefaultJmsTemplate(JmsTemplate defaultJmsTemplate) {
        this.defaultJmsTemplate = defaultJmsTemplate;
    }

    @Autowired
    public void setSystemStateService(SystemStateService systemStateService) {
        this.systemStateService = systemStateService;
    }

    @Autowired
    public void setArcstorageMailCenter(ArcstorageMailCenter arcstorageMailCenter) {
        this.arcstorageMailCenter = arcstorageMailCenter;
    }
}
