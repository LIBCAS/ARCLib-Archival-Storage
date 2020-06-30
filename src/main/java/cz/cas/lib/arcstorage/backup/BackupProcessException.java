package cz.cas.lib.arcstorage.backup;

public class BackupProcessException extends RuntimeException {
    public BackupProcessException(String message) {
        super(message);
    }

    public BackupProcessException(String message, Throwable cause) {
        super(message, cause);
    }
}
