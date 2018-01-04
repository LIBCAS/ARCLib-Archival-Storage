package cz.cas.lib.arcstorage.gateway.storage.exception;

import java.io.IOException;

public abstract class StorageException extends IOException {
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
