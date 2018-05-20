package cz.cas.lib.arcstorage.service.exception;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CantWriteException extends RuntimeException {
    public CantWriteException(String filePath, Throwable cause) {
        super(filePath, cause);
        log.error("Cant write to file on path: " + filePath, cause);
    }
}
