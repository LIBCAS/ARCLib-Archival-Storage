package cz.cas.lib.arcstorage.exception;

/**
 * e.g. count of storages cant be set to 0
 */
public class ForbiddenByConfigException extends Exception {
    public ForbiddenByConfigException(String message) {
        super(message);
    }
}
