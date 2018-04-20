package cz.cas.lib.arcstorage.storage.exception;

import cz.cas.lib.arcstorage.gateway.dto.Checksum;

public class FileCorruptedAfterStoreException extends StorageException {

    public FileCorruptedAfterStoreException(String messagePrefix, Checksum actual, Checksum expected) {
        super(messagePrefix + " expected: " + expected + " but was: " + actual);
    }

    public FileCorruptedAfterStoreException(Checksum actual, Checksum expected) {
        this("", actual, expected);
    }
}
