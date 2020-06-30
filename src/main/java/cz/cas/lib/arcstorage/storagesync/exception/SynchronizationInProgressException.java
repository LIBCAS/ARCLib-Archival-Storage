package cz.cas.lib.arcstorage.storagesync.exception;

import cz.cas.lib.arcstorage.storagesync.StorageSyncStatus;

public class SynchronizationInProgressException extends Exception {
    public SynchronizationInProgressException(StorageSyncStatus status) {
        super(status.toString());
    }

    public SynchronizationInProgressException() {
        super();
    }
}
