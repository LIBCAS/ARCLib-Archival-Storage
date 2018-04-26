package cz.cas.lib.arcstorage.gateway.exception;

import cz.cas.lib.arcstorage.domain.ArchivalObject;
import cz.cas.lib.arcstorage.storage.exception.StorageException;

public class InvalidChecksumException extends StorageException {
    private ArchivalObject archivalObject;

    public InvalidChecksumException(ArchivalObject archivalObject) {
        this.archivalObject = archivalObject;
    }

    public ArchivalObject getArchivalObject() {
        return archivalObject;
    }

    public void setArchivalObject(ArchivalObject archivalObject) {
        this.archivalObject = archivalObject;
    }
}
