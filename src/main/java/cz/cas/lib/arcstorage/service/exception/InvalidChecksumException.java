package cz.cas.lib.arcstorage.service.exception;

import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.storage.exception.StorageException;

public class InvalidChecksumException extends StorageException {
    private Checksum computedChecksum;
    private Checksum expectedChecksum;

    public InvalidChecksumException(Checksum computedChecksum, Checksum expectedChecksum) {
        this.computedChecksum = computedChecksum;
        this.expectedChecksum = expectedChecksum;
    }

    @Override
    public String toString() {
        return "InvalidChecksumException{" +
                ", computedChecksum=" + computedChecksum +
                ", expectedChecksum=" + expectedChecksum +
                '}';
    }
}
