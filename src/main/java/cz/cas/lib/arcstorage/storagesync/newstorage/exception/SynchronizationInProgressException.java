package cz.cas.lib.arcstorage.storagesync.newstorage.exception;

import cz.cas.lib.arcstorage.storagesync.newstorage.StorageSyncStatus;

public class SynchronizationInProgressException extends Exception {
    public SynchronizationInProgressException(StorageSyncStatus status) {
        super(status.toString());
    }

    public SynchronizationInProgressException() {
        super();
    }
}
