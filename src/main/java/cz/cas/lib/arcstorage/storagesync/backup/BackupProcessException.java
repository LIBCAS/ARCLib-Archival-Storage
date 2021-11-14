package cz.cas.lib.arcstorage.storagesync.backup;

public class BackupProcessException extends RuntimeException {
    public BackupProcessException(String message) {
        super(message);
    }

    public BackupProcessException(String message, Throwable cause) {
        super(message, cause);
    }
}
