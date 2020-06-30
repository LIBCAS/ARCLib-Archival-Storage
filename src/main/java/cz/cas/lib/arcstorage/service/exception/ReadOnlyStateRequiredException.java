package cz.cas.lib.arcstorage.service.exception;

public class ReadOnlyStateRequiredException extends RuntimeException {
    public ReadOnlyStateRequiredException() {
        super("requested operation requires the system to run in read only mode");
    }
}
