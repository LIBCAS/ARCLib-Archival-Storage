package cz.cas.lib.arcstorage.jms;

public class JmsConsumerTimeoutException extends RuntimeException {

    public JmsConsumerTimeoutException(String message) {
        super(message);
    }

    public JmsConsumerTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
