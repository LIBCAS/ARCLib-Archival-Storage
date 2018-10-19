package cz.cas.lib.arcstorage.mail;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.security.Role;
import cz.cas.lib.arcstorage.security.user.UserStore;
import cz.cas.lib.arcstorage.storagesync.StorageSyncStatus;
import cz.cas.lib.arcstorage.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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

    public void sendCleanupError(List<ArchivalObject> all, List<ArchivalObject> deleted, List<ArchivalObject> rolledBack, List<ArchivalObject> failed) {
        try {
            for (User user : userStore.findByRole(Role.ROLE_ADMIN)) {
                MimeMessageHelper message = generalMessage(user.getEmail(), "Archival Storage Cleanup Error", false);
                Map<String, Object> params = new HashMap<>();
                params.put("appName", applicationName);
                params.put("appUrl", applicationUrl);
                params.put("total", all.size());
                params.put("deleted", getTemplateValue(deleted));
                params.put("deletedCount", deleted.size());
                params.put("rolledBack", getTemplateValue(rolledBack));
                params.put("rolledBackCount", rolledBack.size());
                params.put("failed", getTemplateValue(failed));
                params.put("failedCount", failed.size());
                InputStream template = Utils.resource("mail/cleanupError.vm");
                transformAndSend(template, params, message);
            }
        } catch (MessagingException | IOException ex) {
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
                params.put("exceptionClass", syncStatus.getExceptionClass());
                params.put("exceptionMessage", syncStatus.getExceptionMsg());
                params.put("storage", syncStatus.getStorage());
                InputStream template = Utils.resource("mail/storageSynchronizationError.vm");
                transformAndSend(template, params, message);
            }
        } catch (MessagingException | IOException ex) {
            throw new GeneralException(ex);
        }
    }

    public void sendObjectRetrievalError(ArchivalObjectDto archivalObjectDto, Storage storageWhichSuceeded, List<Storage> storagesWhichFailed, List<Storage> storagesWithInvalidChecksum, List<Storage> recoveredStorages) {
        List<Storage> unreachableStorages = storageStore.findUnreachableStorages();
        boolean storageRecovered = unreachableStorages.isEmpty() && storageWhichSuceeded != null && storagesWhichFailed.isEmpty() && (storagesWithInvalidChecksum.size() == recoveredStorages.size());
        String conclusion = storageRecovered ? "The object was successfully recovered in the whole Archival Storage." : "The object was not fully recovered. Manual recovery required.";
        try {
            for (User user : userStore.findByRole(Role.ROLE_ADMIN)) {
                MimeMessageHelper message = generalMessage(user.getEmail(), "Object Retrieval Error", false);
                Map<String, Object> params = new HashMap<>();
                params.put("appName", applicationName);
                params.put("appUrl", applicationUrl);
                params.put("object", archivalObjectDto);
                params.put("unreachableStorages", getTemplateValue(unreachableStorages));
                params.put("invalidStorages", getTemplateValue(storagesWithInvalidChecksum));
                params.put("errorStorages", getTemplateValue(storagesWhichFailed));
                params.put("successfulStorage", getTemplateValue(storageWhichSuceeded));
                params.put("recoveredStorages", getTemplateValue(recoveredStorages));
                params.put("conclusion", conclusion);
                InputStream template = Utils.resource("mail/objectRetrievalError.vm");
                transformAndSend(template, params, message);
            }
        } catch (MessagingException | IOException ex) {
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

    private CompletableFuture<Boolean> transformAndSend(InputStream template, Map<String, Object> arguments, MimeMessageHelper helper)
            throws MessagingException, IOException {

        String text = templater.transform(template, arguments);
        helper.setText(text, true);

        MimeMessage message = helper.getMimeMessage();

        if (message.getAllRecipients() != null && message.getAllRecipients().length > 0) {
            return sender.send(message);
        } else {
            String msg = "Mail message was silently consumed because there were no recipients.";
            log.warn(msg);
            throw new GeneralException("Mail message was silently consumed because there were no recipients.");
        }
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
    public void setSenderEmail(@Value("${mail.sender.email}") String senderEmail) {
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

    private Object getTemplateValue(Object object) {
        if (object == null)
            return "none";
        if (object instanceof List) {
            if (((List) object).isEmpty())
                return "none";
        }
        return object;
    }
}
