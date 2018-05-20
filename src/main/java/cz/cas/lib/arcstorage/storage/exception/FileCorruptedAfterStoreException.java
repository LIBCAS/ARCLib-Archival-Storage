package cz.cas.lib.arcstorage.storage.exception;

import cz.cas.lib.arcstorage.dto.Checksum;

/**
 * thrown when the checksum verification performed after the object is stored fails
 */
public class FileCorruptedAfterStoreException extends StorageException {

    public FileCorruptedAfterStoreException(String messagePrefix, Checksum actual, Checksum expected) {
        super(messagePrefix + " expected: " + expected + " but was: " + actual);
    }

    public FileCorruptedAfterStoreException(Checksum actual, Checksum expected) {
        this("", actual, expected);
    }
}
