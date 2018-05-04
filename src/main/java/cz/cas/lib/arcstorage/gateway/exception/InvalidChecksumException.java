package cz.cas.lib.arcstorage.gateway.exception;

import cz.cas.lib.arcstorage.gateway.dto.Checksum;
import cz.cas.lib.arcstorage.storage.exception.StorageException;

import java.nio.file.Path;

public class InvalidChecksumException extends StorageException {
    private Path path;
    private Checksum computedChecksum;
    private Checksum expectedChecksum;

    public InvalidChecksumException(Path path, Checksum computedChecksum, Checksum expectedChecksum) {
        this.path = path;
        this.computedChecksum = computedChecksum;
        this.expectedChecksum = expectedChecksum;
    }

    @Override
    public String toString() {
        return "InvalidChecksumException{" +
                "path=" + path +
                ", computedChecksum=" + computedChecksum +
                ", expectedChecksum=" + expectedChecksum +
                '}';
    }
}
