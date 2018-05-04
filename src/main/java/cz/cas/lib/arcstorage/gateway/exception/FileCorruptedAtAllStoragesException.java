package cz.cas.lib.arcstorage.gateway.exception;

import cz.cas.lib.arcstorage.domain.ArchivalObject;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileCorruptedAtAllStoragesException extends StorageException {
    private ArchivalObject archivalObject;

    public FileCorruptedAtAllStoragesException(ArchivalObject archivalObject) {
        this.archivalObject = archivalObject;
    }

    @Override
    public String toString() {
        return "FileCorruptedAtAllStoragesException{" +
                "archivalObject=" + archivalObject +
                '}';
    }
}
