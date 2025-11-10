package cz.cas.lib.arcstorage.mail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import cz.cas.lib.arcstorage.storagesync.newstorage.StorageSyncStatus;
import freemarker.template.TemplateException;
import jakarta.mail.Address;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.Nullable;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public void sendStorageSynchronizationError(StorageSyncStatus syncStatus) {
        log.error("storage synchronization error: {}", syncStatus);
        try {
            for (String email : getAdminEmails()) {
                MimeMessageHelper message = generalMessage(email, "Storage Synchronization Error", false);
                Map<String, Object> params = new HashMap<>();
                params.put("appName", applicationName);
                params.put("appUrl", applicationUrl);
                params.put("done", syncStatus.getDone());
                params.put("total", syncStatus.getTotal());
                params.put("exceptionDetail", syncStatus.getExceptionStackTrace());
                params.put("exceptionMessage", syncStatus.getExceptionMsg());
                params.put("storage", syncStatus.getStorage());
                transformAndSend("mail/storageSynchronizationError.ftl", params, message);
            }
        } catch (MessagingException | IOException | TemplateException ex) {
            throw new GeneralException(ex);
        }
    }

    public void sendStorageDetachedByError(Storage storage, Throwable error) {
        log.error("storage {} automatically detached because of error", storage, error);
        try {
            for (String email : getAdminEmails()) {
                MimeMessageHelper message = generalMessage(email, "Storage Detached Because Of Error", false);
                Map<String, Object> params = new HashMap<>();
                params.put("appName", applicationName);
                params.put("appUrl", applicationUrl);
                params.put("exceptionDetail", ExceptionUtils.getStackTrace(error));
                params.put("exceptionMessage", error.toString());
                params.put("storage", storage);
                transformAndSend("mail/storageDetachedBecauseOfError.ftl", params, message);
            }
        } catch (MessagingException | IOException | TemplateException ex) {
            throw new GeneralException(ex);
        }
    }

    public void sendFatalError(Exception error, boolean systemSetToReadonlyMode) {
        String logMsg = "encountered fatal error" + (systemSetToReadonlyMode ? ", system was set to readonly mode" : "");
        log.error(logMsg, error);
        try {
            for (String email : getAdminEmails()) {
                MimeMessageHelper message = generalMessage(email, "Encountered fatal error", false);
                Map<String, Object> params = new HashMap<>();
                params.put("appName", applicationName);
                params.put("appUrl", applicationUrl);
                params.put("exceptionDetail", ExceptionUtils.getStackTrace(error));
                params.put("exceptionMessage", error.toString());
                params.put("setToReadonly", systemSetToReadonlyMode);
                transformAndSend("mail/fatalError.ftl", params, message);
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
            for (String email : getAdminEmails()) {
                MimeMessageHelper message = generalMessage(email, "Object Retrieval Error", false);
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
            String conclusion = dto.getContentRecoveredObjectsIds().size() == dto.getContentInconsistencyObjectsIds().size() && dto.getMetadataRecoveredObjectsIds().size() == dto.getMetadataInconsistencyObjectsIds().size()
                    ? "All inconsistent objects were successfully recovered at the storage."
                    : "List of recovered objects differs from the list of inconsistent objects - objects missing in the list of recovered objects are still inconsistent.";
            Map<String, Object> params = new HashMap<>();
            params.put("appName", applicationName);
            params.put("appUrl", applicationUrl);
            params.put("storageId", dto.getStorageId());
            params.put("inconsistentObjects", dto.getContentInconsistencyObjectsIds());
            params.put("recoveredObjects", dto.getContentRecoveredObjectsIds());
            params.put("inconsistentMetadata", dto.getMetadataInconsistencyObjectsIds());
            params.put("recoveredMetadata", dto.getMetadataRecoveredObjectsIds());
            params.put("conclusion", conclusion);
            for (String email : getAdminEmails()) {
                try {
                    MimeMessageHelper message = generalMessage(email, "AIPs verification error", false);
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
        for (String email : getAdminEmails()) {
            try {
                MimeMessageHelper message = generalMessage(email, "Objects cleanup recommendation", false);
                transformAndSend("mail/cleanupRecommendation.ftl", params, message);
            } catch (MessagingException | IOException | TemplateException e) {
                throw new GeneralException(e);
            }
        }
    }

    public void sendInitialStoragesCheckWarning(long storagesCount, int minStorages, List<StorageService> unreachableServices) {
        try {
            for (String email : getAdminEmails()) {
                MimeMessageHelper message = generalMessage(email, "Initial Storage Check Warning", false);
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

    public void sendAttachedReachableStoragesInfo(Set<Storage> storages) {
        try {
            for (String email : getAdminEmails()) {
                MimeMessageHelper message = generalMessage(email, "All attached storages are reachable again", false);
                Map<String, Object> params = new HashMap<>();
                params.put("appName", applicationName);
                params.put("appUrl", applicationUrl);
                params.put("reachableStorages", storages);
                transformAndSend("mail/attachedReachableStoragesInfo.ftl", params, message);
            }
        } catch (MessagingException | IOException | TemplateException ex) {
            throw new GeneralException(ex);
        }
    }

    public void sendAttachedUnreachableStoragesWarning(Set<Storage> unreachableStorages) {
        try {
            for (String email : getAdminEmails()) {
                MimeMessageHelper message = generalMessage(email, "Some attached storages are not reachable", false);
                Map<String, Object> params = new HashMap<>();
                params.put("appName", applicationName);
                params.put("appUrl", applicationUrl);
                params.put("unreachableStorages", unreachableStorages);
                transformAndSend("mail/attachedUnreachableStoragesWarning.ftl", params, message);
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
            for (String email : getAdminEmails()) {
                MimeMessageHelper message = generalMessage(email, "Storage state report", false);
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

        String mailSubject = helper.getMimeMessage().getSubject();
        String recipients = Stream.of(message.getAllRecipients()).map(Address::toString).collect(Collectors.joining(", "));
        if (message.getAllRecipients() != null && message.getAllRecipients().length > 0) {
            return sender.send(message).exceptionally(ex -> {
                log.error("Error sending mail with subject: " + mailSubject + " to: " + recipients, ex);
                return null;
            });
        } else {
            String msg = "Mail message was silently consumed because there were no recipients. Mail subject: " + mailSubject;
            log.warn(msg);
            return CompletableFuture.completedFuture(false);
        }
    }

    private Set<String> getAdminEmails() {
        return userStore.findByRole(Role.ROLE_ADMIN).stream().map(User::getEmail).collect(Collectors.toSet());
    }

    @Autowired
    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Autowired
    public void setApplicationName(@Value("${mail.app.name}") String applicationName) {
        this.applicationName = applicationName;
    }

    @Autowired
    public void setApplicationUrl(@Value("${mail.app.url}") String applicationUrl) {
        this.applicationUrl = applicationUrl;
    }

    @Autowired
    public void setSender(AsyncMailSender sender) {
        this.sender = sender;
    }

    @Autowired
    public void setSenderEmail(@Value("${spring.mail.username}") String senderEmail) {
        this.senderEmail = senderEmail;
    }

    @Autowired
    public void setSenderName(@Value("${mail.sender.name}") String senderName) {
        this.senderName = senderName;
    }

    @Autowired
    public void setTemplater(Templater templater) {
        this.templater = templater;
    }

    @Autowired
    public void setUserStore(UserStore userStore) {
        this.userStore = userStore;
    }

    @Autowired
    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }
}
