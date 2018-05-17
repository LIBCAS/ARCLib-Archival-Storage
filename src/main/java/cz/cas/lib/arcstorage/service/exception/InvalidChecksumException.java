package cz.cas.lib.arcstorage.service.exception;

import cz.cas.lib.arcstorage.dto.Checksum;

import java.io.InvalidObjectException;

public class InvalidChecksumException extends InvalidObjectException {
    private Checksum computedChecksum;
    private Checksum expectedChecksum;

    public InvalidChecksumException(Checksum computedChecksum, Checksum expectedChecksum) {
        super(getMessage(computedChecksum, expectedChecksum));

        this.computedChecksum = computedChecksum;
        this.expectedChecksum = expectedChecksum;
    }

    private static String getMessage(Checksum computedChecksum, Checksum expectedChecksum){
        return "Invalid checksum: computed checksum is " + computedChecksum + " while expected checksum was " + expectedChecksum;
    }

    @Override
    public String toString() {
        return "InvalidChecksumException{" +
                ", computedChecksum=" + computedChecksum +
                ", expectedChecksum=" + expectedChecksum +
                '}';
    }
}
