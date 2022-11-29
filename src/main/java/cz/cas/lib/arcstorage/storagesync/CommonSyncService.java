package cz.cas.lib.arcstorage.storagesync;

import cz.cas.lib.arcstorage.domain.views.ArchivalObjectLightweightView;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.ObjectRetrievalResource;
import cz.cas.lib.arcstorage.exception.ForbiddenByConfigException;
import cz.cas.lib.arcstorage.service.ArchivalService;
import cz.cas.lib.arcstorage.service.exception.state.FailedStateException;
import cz.cas.lib.arcstorage.service.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.service.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageAttachedException;
import cz.cas.lib.arcstorage.service.exception.storage.NoLogicalStorageReachableException;
import cz.cas.lib.arcstorage.service.exception.storage.ObjectCouldNotBeRetrievedException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Slf4j
public class CommonSyncService {

    private ArchivalService archivalService;
    private Path tmpFolder;
    private boolean forgetFeatureAllowed;

    public void copyObject(ArchivalObjectDto object, StorageService targetStorage) throws StorageException, NoLogicalStorageAttachedException, ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, RollbackStateException, StillProcessingStateException, FailedStateException {
        switch (object.getState()) {
            case DELETED:
            case DELETION_FAILURE:
            case ROLLED_BACK:
            case ROLLBACK_FAILURE:
            case ARCHIVAL_FAILURE:
                log.trace("copying metadata of object " + object);
                targetStorage.storeObject(object, new AtomicBoolean(false), object.getOwner().getDataSpace());
                break;
            case ARCHIVED:
            case REMOVED:
                log.trace("copying " + object);
                String objectRetrievalResourceId = null;
                try (ObjectRetrievalResource objectRetrievalResource = archivalService.getObject(object);
                     InputStream is = new BufferedInputStream(objectRetrievalResource.getInputStream())) {
                    objectRetrievalResourceId = objectRetrievalResource.getId();
                    object.setInputStream(is);
                    targetStorage.storeObject(object, new AtomicBoolean(false), object.getOwner().getDataSpace());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } finally {
                    if (objectRetrievalResourceId != null) {
                        tmpFolder.resolve(objectRetrievalResourceId).toFile().delete();
                    }
                }
                break;
            case PRE_PROCESSING:
            case PROCESSING:
            case FORGOT: //forgotten objects are not event present in DB, this should not occur
            default:
                throw new IllegalArgumentException("can't copy object " + object.getStorageId() + " because it is in " + object.getState() + " state");
        }
    }

    /**
     * @param objectAudit             operation to propagate
     * @param objectInDb              object in DB
     * @param targetStorage           storage to which operations are propagated
     * @param createMetaFileIfMissing if false and metadata file is missing at the storage then the underlying storage
     *                                operation should rather fail then create a new one
     */
    public void propagateModification(ObjectAudit objectAudit, ArchivalObjectLightweightView objectInDb, StorageService targetStorage, boolean createMetaFileIfMissing) throws StorageException, NoLogicalStorageAttachedException, ObjectCouldNotBeRetrievedException, NoLogicalStorageReachableException, RollbackStateException, StillProcessingStateException, FailedStateException, ForbiddenByConfigException {
        log.trace("propagating " + objectAudit);
        switch (objectAudit.getOperation()) {
            case REMOVAL:
                targetStorage.remove(objectInDb.toDto(), objectAudit.getUser().getDataSpace(), createMetaFileIfMissing);
                break;
            case RENEWAL:
                targetStorage.renew(objectInDb.toDto(), objectAudit.getUser().getDataSpace(), createMetaFileIfMissing);
                break;
            case DELETION:
                targetStorage.delete(objectInDb.toDto(), objectAudit.getUser().getDataSpace(), createMetaFileIfMissing);
                break;
            case ROLLBACK:
                targetStorage.rollbackObject(objectInDb.toDto(), objectAudit.getUser().getDataSpace());
                break;
            case ARCHIVAL_RETRY:
                copyObject(objectInDb.toDto(), targetStorage);
                break;
            case FORGET:
                if (!forgetFeatureAllowed) {
                    throw new ForbiddenByConfigException("forget feature not allowed");
                }
                //objectInDb is always null as forgotten data are not present in DB
                targetStorage.forgetObject(objectAudit.getIdInStorage(), objectAudit.getUser().getDataSpace(), objectAudit.getCreated());
                break;
            default:
                throw new IllegalArgumentException("unknown operation: " + objectAudit.getOperation());
        }
    }

    @Inject
    public void setArchivalService(ArchivalService archivalService) {
        this.archivalService = archivalService;
    }

    @Inject
    public void setTmpFolder(@Value("${arcstorage.tmpFolder}") String path) {
        this.tmpFolder = Paths.get(path);
    }

    @Inject
    public void setForgetFeatureAllowed(@Value("${arcstorage.optionalFeatures.forgetObject}") boolean forgetFeatureAllowed) {
        this.forgetFeatureAllowed = forgetFeatureAllowed;
    }
}
