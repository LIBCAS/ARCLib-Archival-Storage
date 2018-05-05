package cz.cas.lib.arcstorage.service.exception;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileCorruptedAtAllStoragesException extends Exception {
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
