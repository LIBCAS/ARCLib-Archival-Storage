package cz.cas.lib.arcstorage.storage.exception;

/**
 * Abstract class to be extended by exceptions which occurs on the storage layer.
 */
public abstract class StorageException extends Exception {
    public StorageException() {
    }

    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageException(Throwable cause) {
        super(cause);
    }
}
