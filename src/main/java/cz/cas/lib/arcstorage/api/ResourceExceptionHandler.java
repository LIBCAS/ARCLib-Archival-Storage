package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.api.multipart.TmpFolderSizeLimitReachedException;
import cz.cas.lib.arcstorage.storagesync.backup.BackupProcessException;
import cz.cas.lib.arcstorage.exception.BadRequestException;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import cz.cas.lib.arcstorage.exception.ForbiddenByConfigException;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.service.exception.BadXmlVersionProvidedException;
import cz.cas.lib.arcstorage.service.exception.InvalidChecksumException;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateRequiredException;
import cz.cas.lib.arcstorage.service.exception.state.StateException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.ObjectCouldNotBeRetrievedException;
import cz.cas.lib.arcstorage.service.exception.storage.SomeLogicalStoragesNotReachableException;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.CantCreateDataspaceException;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.StorageStillProcessObjectsException;
import cz.cas.lib.arcstorage.storagesync.newstorage.exception.SynchronizationInProgressException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
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
@Slf4j
@ControllerAdvice
public class ResourceExceptionHandler {

    @ExceptionHandler(MissingObject.class)
    public ResponseEntity notFound(Exception e) {
        return errorResponse(e, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler({
            BadRequestException.class,
            BindException.class
    })
    public ResponseEntity badRequest(Exception e) {
        return errorResponse(e, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler({
            BackupProcessException.class,
            ObjectCouldNotBeRetrievedException.class,
            NoLogicalStorageAttachedException.class,
            StorageException.class,
            Exception.class})
    public ResponseEntity internalServerError(Exception e) {
        return errorResponse(e, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @ExceptionHandler({
            SomeLogicalStoragesNotReachableException.class,
            NoLogicalStorageReachableException.class,
            ReadOnlyStateException.class,
            TmpFolderSizeLimitReachedException.class
    })
    public ResponseEntity serviceUnavailable(Exception e) {
        return errorResponse(e, HttpStatus.SERVICE_UNAVAILABLE);
    }

    @ExceptionHandler({
            BadXmlVersionProvidedException.class,
            ConflictObject.class
    })
    public ResponseEntity conflict(Exception e) {
        return errorResponse(e, HttpStatus.CONFLICT);
    }

    @ExceptionHandler({
            ForbiddenByConfigException.class,
            SynchronizationInProgressException.class,
            StateException.class,
            ReadOnlyStateRequiredException.class,
            UnsupportedOperationException.class})
    public ResponseEntity forbidden(Exception e) {
        return errorResponse(e, HttpStatus.FORBIDDEN);
    }

    @ExceptionHandler(StorageStillProcessObjectsException.class)
    public ResponseEntity locked(Exception e) {
        return errorResponse(e, HttpStatus.LOCKED);
    }

    @ExceptionHandler({
            CantCreateDataspaceException.class,
            InvalidChecksumException.class
    })
    public ResponseEntity unprocessableEntity(Exception e) {
        return errorResponse(e, HttpStatus.UNPROCESSABLE_ENTITY);
    }

    /**
     * do not log stacktrace for this one
     */
    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity accessDeniedException(Exception e) {
        log.info(e.toString());
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(e.toString());
    }

    private ResponseEntity errorResponse(Throwable throwable, HttpStatus status) {
            log.error("error caught: " + throwable.toString(), throwable);
        return ResponseEntity.status(status).body(throwable.toString());
    }
}
