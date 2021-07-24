package cz.cas.lib.arcstorage.mail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.ObjectConsistencyVerificationResultDto;
import cz.cas.lib.arcstorage.dto.RecoveryResultDto;
import cz.cas.lib.arcstorage.dto.StorageStateDto;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.security.Role;
import cz.cas.lib.arcstorage.security.user.UserStore;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storagesync.StorageSyncStatus;
import freemarker.template.TemplateException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Component
@Slf4j
public class ArcstorageMailCenter {
    private String applicationName;
    private String applicationUrl;
    private AsyncMailSender sender;
    private String senderEmail;
    private String senderName;
    private Templater templater;
    private UserStore userStore;
    private StorageStore storageStore;
    private ObjectMapper objectMapper;

    public void sendCleanupError(List<ArchivalObject> all, List<ArchivalObject> deleted, List<ArchivalObject> rolledBack, List<ArchivalObject> failed) {
        try {
            for (User user : userStore.findByRole(Role.ROLE_ADMIN)) {
                MimeMessageHelper message = generalMessage(user.getEmail(), "Archival Storage Cleanup Error", false);
                Map<String, Object> params = new HashMap<>();
                params.put("appName", applicationName);
                params.put("appUrl", applicationUrl);
                params.put("total", all.size());
                params.put("deleted", deleted);
                params.put("rolledBack", rolledBack);
                params.put("failed", failed);
                transformAndSend("mail/cleanupError.ftl", params, message);
            }
        } catch (MessagingException | IOException | TemplateException ex) {
            throw new GeneralException(ex);
        }
    }

    public void sendStorageSynchronizationError(StorageSyncStatus syncStatus) {
        log.error("storage synchronization error: " + syncStatus);
        try {
            for (User user : userStore.findByRole(Role.ROLE_ADMIN)) {
                MimeMessageHelper message = generalMessage(user.getEmail(), "Storage Synchronization Error", false);
                Map<String, Object> params = new HashMap<>();
                params.put("appName", applicationName);
                params.put("appUrl", applicationUrl);
                params.put("phase", syncStatus.getPhase());
                params.put("done", syncStatus.getDoneInThisPhase());
                params.put("total", syncStatus.getTotalInThisPhase());
                params.put("exceptionDetail", syncStatus.getExceptionStackTrace());
                params.put("exceptionMessage", syncStatus.getExceptionMsg());
                params.put("storage", syncStatus.getStorage());
                transformAndSend("mail/storageSynchronizationError.ftl", params, message);
            }
        } catch (MessagingException | IOException | TemplateException ex) {
            throw new GeneralException(ex);
        }
    }

    public void sendObjectRetrievalError(ArchivalObjectDto archivalObjectDto, Storage storageWhichSuceeded, List<Storage> storagesWhichFailed, List<Storage> storagesWithInvalidChecksum, List<Storage> recoveredStorages) {
        List<Storage> unreachableStorages = storageStore.findUnreachableStorages();
        boolean storageRecovered = unreachableStorages.isEmpty() && storageWhichSuceeded != null && storagesWhichFailed.isEmpty() && (storagesWithInvalidChecksum.size() == recoveredStorages.size());
        String conclusion = storageRecovered ? "The object was successfully recovered." : "The object was not fully recovered. Manual recovery required.";
        try {
            for (User user : userStore.findByRole(Role.ROLE_ADMIN)) {
                MimeMessageHelper message = generalMessage(user.getEmail(), "Object Retrieval Error", false);
                Map<String, Object> params = new HashMap<>();
                params.put("appName", applicationName);
                params.put("appUrl", applicationUrl);
                params.put("object", archivalObjectDto);
                params.put("unreachableStorages", unreachableStorages);
                params.put("invalidStorages", storagesWithInvalidChecksum);
                params.put("errorStorages", storagesWhichFailed);
                params.put("successfulStorage", storageWhichSuceeded);
                params.put("recoveredStorages", recoveredStorages);
                params.put("conclusion", conclusion);
                transformAndSend("mail/objectRetrievalError.ftl", params, message);
            }
        } catch (MessagingException | IOException | TemplateException ex) {
            throw new GeneralException(ex);
        }
    }

    public void sendAipsVerificationError(Map<String, RecoveryResultDto> recoveryResultsGroupedByStorages) {
        for (RecoveryResultDto dto : recoveryResultsGroupedByStorages.values()) {
            String conclusion = dto.getContentRecoveredObjectsIds().size() == dto.getContentInconsistencyObjectsIds().size() && dto.getMetadataRecoveredObjectsIds().size() == dto.getMetadataInconsistencyObjectsIds().size() ? "All inconsistent objects were successfully recovered at the storage." : "Some objects are still inconsistent.";
            Map<String, Object> params = new HashMap<>();
            params.put("appName", applicationName);
            params.put("appUrl", applicationUrl);
            params.put("storageId", dto.getStorageId());
            params.put("inconsistentObjects", dto.getContentInconsistencyObjectsIds());
            params.put("recoveredObjects", dto.getContentRecoveredObjectsIds());
            params.put("inconsistentMetadata", dto.getMetadataInconsistencyObjectsIds());
            params.put("recoveredMetadata", dto.getMetadataRecoveredObjectsIds());
            params.put("conclusion", conclusion);
            for (User user : userStore.findByRole(Role.ROLE_ADMIN)) {
                try {
                    MimeMessageHelper message = generalMessage(user.getEmail(), "AIPs verification error", false);
                    transformAndSend("mail/aipsVerificationAtStorageError.ftl", params, message);
                } catch (MessagingException | IOException | TemplateException e) {
                    throw new GeneralException(e);
                }
            }
        }
    }

    public void sendCleanupRecommendation(Map<String, ObjectConsistencyVerificationResultDto> storageIdsToVerificationResultMap) {
        Map<String, Object> params = new HashMap<>();
        List<String> objDescriptions = storageIdsToVerificationResultMap.values().stream().map(v -> v.getDatabaseId() + ", " + v.getStorageId() + ", " + v.getCreated() + ", " + v.getState()).collect(Collectors.toList());
        params.put("appName", applicationName);
        params.put("appUrl", applicationUrl);
        params.put("objects", objDescriptions);
        for (User user : userStore.findByRole(Role.ROLE_ADMIN)) {
            try {
                MimeMessageHelper message = generalMessage(user.getEmail(), "Objects cleanup recommendation", false);
                transformAndSend("mail/cleanupRecommendation.ftl", params, message);
            } catch (MessagingException | IOException | TemplateException e) {
                throw new GeneralException(e);
            }
        }
    }

    public void sendInitialStoragesCheckWarning(long storagesCount, int minStorages, List<StorageService> unreachableServices) {
        try {
            for (User user : userStore.findByRole(Role.ROLE_ADMIN)) {
                MimeMessageHelper message = generalMessage(user.getEmail(), "Initial Storage Check Warning", false);
                Map<String, Object> params = new HashMap<>();
                params.put("appName", applicationName);
                params.put("appUrl", applicationUrl);
                params.put("storagesCount", storagesCount);
                params.put("minStorages", minStorages);
                params.put("unreachableServices", unreachableServices.stream().map(s -> s.getStorage().getId()).collect(Collectors.toList()));
                transformAndSend("mail/initialStoragesCheckWarning.ftl", params, message);
            }
        } catch (MessagingException | IOException | TemplateException ex) {
            throw new GeneralException(ex);
        }
    }

    public void sendStorageStateReport(List<StorageStateDto> states) {
        List<String> statesStrings = states.stream().map(s -> {
            try {
                return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(s);
            } catch (JsonProcessingException e) {
                throw new GeneralException(e);
            }
        }).collect(Collectors.toList());
        try {
            for (User user : userStore.findByRole(Role.ROLE_ADMIN)) {
                MimeMessageHelper message = generalMessage(user.getEmail(), "Storage state report", false);
                Map<String, Object> params = new HashMap<>();
                params.put("appName", applicationName);
                params.put("appUrl", applicationUrl);
                params.put("states", statesStrings);
                transformAndSend("mail/storageStateReport.ftl", params, message);
            }
        } catch (MessagingException | IOException | TemplateException ex) {
            throw new GeneralException(ex);
        }
    }

    private MimeMessageHelper generalMessage(String emailTo, @Nullable String subject, boolean hasAttachment) throws MessagingException {
        MimeMessage message = sender.create();

        // use the true flag to indicate you need a multipart message
        MimeMessageHelper helper = new MimeMessageHelper(message, hasAttachment);

        if (emailTo != null) {
            helper.setTo(emailTo);
        }

        if (subject != null) {
            helper.setSubject(subject);
        }

        try {
            helper.setFrom(senderEmail, senderName);
        } catch (UnsupportedEncodingException ex) {
            log.warn("Can not set email 'from' encoding, fallbacking.");
            helper.setFrom(senderEmail);
        }

        return helper;
    }

    private CompletableFuture<Boolean> transformAndSend(String template, Map<String, Object> arguments, MimeMessageHelper helper)
            throws MessagingException, IOException, TemplateException {

        String text = templater.transform(template, arguments);
        helper.setText(text, true);

        MimeMessage message = helper.getMimeMessage();

        if (message.getAllRecipients() != null && message.getAllRecipients().length > 0) {
            return sender.send(message);
        } else {
            String msg = "Mail message was silently consumed because there were no recipients. Mail subject: "+helper.getMimeMessage().getSubject();
            log.warn(msg);
            return CompletableFuture.completedFuture(false);
        }
    }

    @Inject
    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Inject
    public void setApplicationName(@Value("${mail.app.name}") String applicationName) {
        this.applicationName = applicationName;
    }

    @Inject
    public void setApplicationUrl(@Value("${mail.app.url}") String applicationUrl) {
        this.applicationUrl = applicationUrl;
    }

    @Inject
    public void setSender(AsyncMailSender sender) {
        this.sender = sender;
    }

    @Inject
    public void setSenderEmail(@Value("${spring.mail.username}") String senderEmail) {
        this.senderEmail = senderEmail;
    }

    @Inject
    public void setSenderName(@Value("${mail.sender.name}") String senderName) {
        this.senderName = senderName;
    }

    @Inject
    public void setTemplater(Templater templater) {
        this.templater = templater;
    }

    @Inject
    public void setUserStore(UserStore userStore) {
        this.userStore = userStore;
    }

    @Inject
    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }
}
