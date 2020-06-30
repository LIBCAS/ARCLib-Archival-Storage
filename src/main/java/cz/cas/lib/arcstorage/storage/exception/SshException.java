package cz.cas.lib.arcstorage.storage.exception;

import cz.cas.lib.arcstorage.domain.entity.Storage;

/**
 * thrown when the SSH connection to remote storage fails
 */
public class SshException extends StorageException {
    public SshException(Storage storage) {
        super(storage);
    }

    public SshException(String message, Storage storage) {
        super(message, storage);
    }

    public SshException(String message, Throwable cause, Storage storage) {
        super(message, cause, storage);
    }

    public SshException(Throwable cause, Storage storage) {
        super(cause, storage);
    }
}
