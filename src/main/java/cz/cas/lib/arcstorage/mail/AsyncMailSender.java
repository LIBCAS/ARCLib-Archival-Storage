package cz.cas.lib.arcstorage.mail;

import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.mail.internet.MimeMessage;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous wrapper around Spring {@link JavaMailSender}.
 */
@Component
public class AsyncMailSender {
    private JavaMailSender sender;

    @Async
    public CompletableFuture<Boolean> send(MimeMessage msg) {
        sender.send(msg);
        return CompletableFuture.completedFuture(true);
    }

    public MimeMessage create() {
        return sender.createMimeMessage();
    }

    @Inject
    public void setSender(JavaMailSender sender) {
        this.sender = sender;
    }
}
