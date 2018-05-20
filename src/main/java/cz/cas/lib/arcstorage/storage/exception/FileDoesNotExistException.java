package cz.cas.lib.arcstorage.storage.exception;

/**
 * thrown when file does not exist
 */
public class FileDoesNotExistException extends StorageException {
    public FileDoesNotExistException() {
    }

    public FileDoesNotExistException(String message) {
        super(message);
    }

    public FileDoesNotExistException(String message, Throwable cause) {
        super(message, cause);
    }
}
