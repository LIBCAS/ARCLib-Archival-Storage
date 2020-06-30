package cz.cas.lib.arcstorage.storage.exception;

import cz.cas.lib.arcstorage.domain.entity.Storage;

public class CantParseMetadataFile extends StorageException {

    public CantParseMetadataFile(String fileId, String message, Storage storage) {
        super("File: " + fileId + ": " + message, storage);
    }
}
