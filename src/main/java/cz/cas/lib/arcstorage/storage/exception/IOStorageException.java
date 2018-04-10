package cz.cas.lib.arcstorage.storage.exception;

public class IOStorageException extends StorageException {

    public IOStorageException() {
    }

    public IOStorageException(String message) {
        super(message);
    }

    public IOStorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public IOStorageException(Throwable cause) {
        super(cause);
    }
}
