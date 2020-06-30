package cz.cas.lib.arcstorage.storagesync.exception;

import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;

public class PostSyncCheckException extends Exception {
    public PostSyncCheckException(ArchivalObjectDto failedObject) {
        super("post sync check failed for object: " + failedObject);
    }
}
