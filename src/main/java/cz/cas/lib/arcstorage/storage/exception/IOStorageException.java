package cz.cas.lib.arcstorage.storage.exception;

import cz.cas.lib.arcstorage.domain.entity.Storage;

/**
 * thrown when some unrecognized IO exception occurs
 */
public class IOStorageException extends StorageException {

    public IOStorageException(Storage storage) {
        super(storage);
    }

    public IOStorageException(String message, Storage storage) {
        super(message, storage);
    }

    public IOStorageException(String message, Throwable cause, Storage storage) {
        super(message, cause, storage);
    }

    public IOStorageException(Throwable cause, Storage storage) {
        super(cause, storage);
    }
}
