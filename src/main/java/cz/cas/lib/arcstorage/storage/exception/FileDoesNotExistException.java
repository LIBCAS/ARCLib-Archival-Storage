package cz.cas.lib.arcstorage.storage.exception;

import cz.cas.lib.arcstorage.domain.entity.Storage;

/**
 * thrown when file does not exist
 */
public class FileDoesNotExistException extends StorageException {
    public FileDoesNotExistException(String fileName, Storage storage) {
        super("File: " + fileName, storage);
    }
}
