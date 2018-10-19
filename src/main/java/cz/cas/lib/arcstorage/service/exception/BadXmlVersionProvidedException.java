package cz.cas.lib.arcstorage.service.exception;

public class BadXmlVersionProvidedException extends Exception {
    public BadXmlVersionProvidedException(int provided, int latest) {
        super("Provided version number of new XML: " + provided + " does not follow the sequence. Current number of latest successfully archived XML: " + latest);
    }
}
