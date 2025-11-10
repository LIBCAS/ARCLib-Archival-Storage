package cz.cas.lib.arcstorage.jms;

public class JmsConsumerStoppedByOtherThreadException extends RuntimeException {

    public JmsConsumerStoppedByOtherThreadException(String message) {
        super(message);
    }

    public JmsConsumerStoppedByOtherThreadException(String message, Throwable cause) {
        super(message, cause);
    }
}
