package cz.cas.lib.arcstorage.gateway.exception;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CantReadException extends RuntimeException {
    public CantReadException(String filePath, Throwable cause) {
        super(filePath, cause);
        log.error("Cant read file on path: " + filePath, cause);
    }
}
