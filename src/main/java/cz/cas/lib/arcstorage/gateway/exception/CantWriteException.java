package cz.cas.lib.arcstorage.gateway.exception;

import lombok.extern.log4j.Log4j;

@Log4j
public class CantWriteException extends RuntimeException {
    public CantWriteException(String filePath, Throwable cause) {
        super(filePath, cause);
        log.error("Cant write to file on path: " + filePath, cause);
    }
}
