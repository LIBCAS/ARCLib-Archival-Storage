package cz.cas.lib.arcstorage.gateway.exception;

import lombok.extern.log4j.Log4j;

@Log4j
public class CantReadException extends RuntimeException {
    public CantReadException(String filePath, Throwable cause) {
        super(filePath, cause);
        log.error("Cant read file on path: " + filePath, cause);
    }
}
