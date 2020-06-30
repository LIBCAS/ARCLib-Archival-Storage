package cz.cas.lib.arcstorage.storage.exception;

import cz.cas.lib.arcstorage.domain.entity.Storage;

/**
 * Abstract class to be extended by exceptions which occurs on the storage layer.
 */
public abstract class StorageException extends Exception {
    public StorageException(Storage storage) {
        super(storage.toString());
    }

    public StorageException(String message, Storage storage) {
        super(storage + ": " + message);
    }

    public StorageException(String message, Throwable cause, Storage storage) {
        super(storage + ": " + message, cause);
    }

    public StorageException(Throwable cause, Storage storage) {
        super(storage.toString(), cause);
    }
}
