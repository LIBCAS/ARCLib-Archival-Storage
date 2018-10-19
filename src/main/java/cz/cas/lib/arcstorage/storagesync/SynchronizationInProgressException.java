package cz.cas.lib.arcstorage.storagesync;

public class SynchronizationInProgressException extends Exception {
    public SynchronizationInProgressException(StorageSyncStatus status) {
        super(status.toString());
    }
}
