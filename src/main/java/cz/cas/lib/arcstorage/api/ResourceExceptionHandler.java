package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.exception.BadRequestException;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import cz.cas.lib.arcstorage.exception.ForbiddenByConfigException;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.service.exception.BadXmlVersionProvidedException;
import cz.cas.lib.arcstorage.service.exception.InvalidChecksumException;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.state.StateException;
import cz.cas.lib.arcstorage.service.exception.storage.ObjectCouldNotBeRetrievedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storagesync.StorageStillProcessObjectsException;
import cz.cas.lib.arcstorage.storagesync.SynchronizationInProgressException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * {@link Exception} to HTTP codes mapping.
 * <p>
 * <p>
 * Uses Spring functionality for mapping concrete {@link Exception} onto a returned HTTP code.
 * To create new mapping just create new method with {@link ResponseStatus} and {@link ExceptionHandler}
 * annotations.
 * </p>
 */
@ControllerAdvice
public class ResourceExceptionHandler {

    @ResponseStatus(value = HttpStatus.NOT_FOUND)
    @ExceptionHandler(MissingObject.class)
    public void missingObject() {}

    @ResponseStatus(value = HttpStatus.CONFLICT)
    @ExceptionHandler(ConflictObject.class)
    public void conflictException() {}

    @ResponseStatus(value = HttpStatus.BAD_REQUEST)
    @ExceptionHandler(BindException.class)
    public void bindException() {}

    @ExceptionHandler(BadRequestException.class)
    public ResponseEntity badRequestException(BadRequestException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
    }

    @ExceptionHandler(StateException.class)
    public ResponseEntity invalidStateException(StateException e) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(e.toString());
    }

    @ExceptionHandler(InvalidChecksumException.class)
    public ResponseEntity invalidChecksumException(InvalidChecksumException e) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(e.toString());
    }

    @ExceptionHandler(ObjectCouldNotBeRetrievedException.class)
    public ResponseEntity fileCorruptedAtAllStoragesException(ObjectCouldNotBeRetrievedException e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.toString());
    }

    @ExceptionHandler(SomeLogicalStoragesNotReachableException.class)
    public ResponseEntity storageNotReachableException(SomeLogicalStoragesNotReachableException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(e.toString());
    }

    @ExceptionHandler(NoLogicalStorageAttachedException.class)
    public ResponseEntity noLogicalStorageAttachedException(NoLogicalStorageAttachedException e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.toString());
    }

    @ExceptionHandler(NoLogicalStorageReachableException.class)
    public ResponseEntity noLogicalStorageReachableException(NoLogicalStorageReachableException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(e.toString());
    }

    @ExceptionHandler(BadXmlVersionProvidedException.class)
    public ResponseEntity badXmlVersionProvidedException(BadXmlVersionProvidedException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(e.toString());
    }

    @ExceptionHandler(ForbiddenByConfigException.class)
    public ResponseEntity forbiddenByConfigException(ForbiddenByConfigException e) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(e.toString());
    }

    @ExceptionHandler(SynchronizationInProgressException.class)
    public ResponseEntity synchronizationInProgessException(SynchronizationInProgressException e) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(e.toString());
    }

    @ExceptionHandler(ReadOnlyStateException.class)
    public ResponseEntity readOnlyStateException(ReadOnlyStateException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(e.toString());
    }

    @ExceptionHandler(StorageStillProcessObjectsException.class)
    public ResponseEntity storageStillProcessObjectsException(StorageStillProcessObjectsException e) {
        return ResponseEntity.status(566).body(e.toString());
    }
}
