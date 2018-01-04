package cz.cas.lib.arcstorage.gateway.storage.exception;

public class FileDoesNotExistException extends StorageException {
    public FileDoesNotExistException() {
    }

    public FileDoesNotExistException(String message) {
        super(message);
    }

    public FileDoesNotExistException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileDoesNotExistException(Throwable cause) {
        super(cause);
    }
}
