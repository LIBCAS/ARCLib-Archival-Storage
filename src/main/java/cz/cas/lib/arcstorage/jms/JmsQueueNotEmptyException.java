package cz.cas.lib.arcstorage.jms;

public class JmsQueueNotEmptyException extends Exception {

    public JmsQueueNotEmptyException(String message) {
        super(message);
    }
}
