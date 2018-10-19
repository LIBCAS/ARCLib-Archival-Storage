package cz.cas.lib.arcstorage.service.exception;

/**
 * thrown when trying to write to archival storage while it is in readonly mode
 */
public class ReadOnlyStateException extends RuntimeException{
    public ReadOnlyStateException() {
        super("trying to write to archival storage while it is in readonly mode");
    }
}
