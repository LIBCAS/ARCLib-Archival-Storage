package cz.cas.lib.arcstorage.storage.exception;

/**
 * thrown when the SSH connection to remote storage fails
 */
public class SshException extends StorageException {
    public SshException() {
    }

    public SshException(String message) {
        super(message);
    }

    public SshException(String message, Throwable cause) {
        super(message, cause);
    }

    public SshException(Throwable cause) {
        super(cause);
    }
}
