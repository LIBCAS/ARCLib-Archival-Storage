package cz.cas.lib.arcstorage.storage.exception;

public class StorageConnectionException extends StorageException {
    public StorageConnectionException() {
    }

    public StorageConnectionException(String message) {
        super(message);
    }

    public StorageConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageConnectionException(Throwable cause) {
        super(cause);
    }
}
