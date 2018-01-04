package cz.cas.lib.arcstorage.gateway.storage.exception;

public class FileCorruptedAfterStoreException extends StorageException {

    public FileCorruptedAfterStoreException() {
    }

    public FileCorruptedAfterStoreException(String message) {
        super(message);
    }

    public FileCorruptedAfterStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileCorruptedAfterStoreException(Throwable cause) {
        super(cause);
    }
}
