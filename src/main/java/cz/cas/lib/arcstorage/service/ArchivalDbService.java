package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.domain.store.*;
import cz.cas.lib.arcstorage.domain.views.ArchivalObjectLightweightView;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import cz.cas.lib.arcstorage.exception.GeneralException;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.security.Role;
import cz.cas.lib.arcstorage.security.user.UserDetails;
import cz.cas.lib.arcstorage.security.user.UserStore;
import cz.cas.lib.arcstorage.service.exception.BadXmlVersionProvidedException;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.state.DeletedStateException;
import cz.cas.lib.arcstorage.service.exception.state.FailedStateException;
import cz.cas.lib.arcstorage.service.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.service.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.storage.StorageService;
import cz.cas.lib.arcstorage.storage.exception.StorageException;
import cz.cas.lib.arcstorage.storagesync.AuditedOperation;
import cz.cas.lib.arcstorage.storagesync.ObjectAudit;
import cz.cas.lib.arcstorage.storagesync.ObjectAuditStore;
import cz.cas.lib.arcstorage.util.ApplicationContextUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import static cz.cas.lib.arcstorage.storage.StorageUtils.*;
import static cz.cas.lib.arcstorage.util.Utils.*;

/**
 * Class used for communication with Archival Storage database which contains transactional data about Archival Storage packages.
 */
@Service
@Slf4j
public class ArchivalDbService {

    private AipSipStore aipSipStore;
    private AipXmlStore aipXmlStore;
    private ArchivalObjectStore archivalObjectStore;
    private ObjectAuditStore objectAuditStore;
    private UserDetails userDetails;
    private UserStore userStore;
    private SystemStateService systemStateService;
    private TransactionTemplate transactionTemplate;
    private ArchivalObjectLightweightViewStore archivalObjectLightweightViewStore;

    /**
     * Registers that AIP creation process has started. Stores AIP records to database and sets their state to <i>processing</i>.
     * returns AipSip entity and flag which is true if this is {@link AuditedOperation#ARCHIVAL_RETRY}
     *
     * @return pair of AipSip and flag indication whether the creation is first attempt (false) or retry (true)
     */
    public Pair<AipSip, Boolean> registerAipCreation(String sipId, Checksum sipChecksum, Checksum xmlChecksum, Instant creationTime) throws ReadOnlyStateException {
        AipSip existingSip = aipSipStore.find(sipId);
        if (existingSip != null &&
                !existingSip.getState().equals(ObjectState.ROLLED_BACK) &&
                !existingSip.getState().equals(ObjectState.ARCHIVAL_FAILURE) &&
                !existingSip.getState().equals(ObjectState.ROLLBACK_FAILURE))
            throw new ConflictObject(existingSip);
        User user = userStore.find(userDetails.getId());
        AipSip sip = new AipSip(sipId, sipChecksum, user, ObjectState.PRE_PROCESSING);
        AipXml xml;
        boolean archivalRetry = existingSip != null;
        if (archivalRetry) {
            eq(existingSip.getXmls().size(), 1, () -> new GeneralException("Internal error: trying ARCHIVAL RETRY on " +
                    "AIP: " + existingSip.getId() + " which has: " + existingSip.getXmls().size() + " linked XMLs.. should have exactly one"));
            xml = existingSip.getXml(0);
            sip.setCreated(existingSip.getCreated());
        } else {
            xml = new AipXml(UUID.randomUUID().toString(), xmlChecksum, new User(userDetails.getId()), sip, 1, ObjectState.PRE_PROCESSING);
            sip.setCreated(creationTime);
            xml.setCreated(creationTime);
        }
        return transactionTemplate.execute(status -> {
            if (systemStateService.get().isReadOnly())
                throw new ReadOnlyStateException();
            aipSipStore.save(sip);
            aipXmlStore.save(xml);
            if (archivalRetry) {
                objectAuditStore.save(new ObjectAudit(sip, new User(userDetails.getId()), AuditedOperation.ARCHIVAL_RETRY));
                objectAuditStore.save(new ObjectAudit(xml, new User(userDetails.getId()), AuditedOperation.ARCHIVAL_RETRY));
            }
            log.debug("Creation of AIP with id " + sip + " has been registered.");
            if (sip.getXmls().size() == 0)
                sip.addXml(xml);
            ApplicationContextUtils.getProcessingObjects().put(sipId, Pair.of(new AtomicBoolean(false), new ReentrantLock()));
            ApplicationContextUtils.getProcessingObjects().put(xml.getId(), Pair.of(new AtomicBoolean(false), new ReentrantLock()));
            return Pair.of(sip, archivalRetry);
        });
    }

    /**
     * Registers that AIP XML update process has started.
     *
     * @param sipId
     * @param xmlChecksum
     * @return created XML entity filled ID and version and flag which is true if this is {@link AuditedOperation#ARCHIVAL_RETRY}
     */
    public Pair<AipXml, Boolean> registerXmlUpdate(String sipId, Checksum xmlChecksum, Integer version) throws StillProcessingStateException, FailedStateException, RollbackStateException, DeletedStateException, BadXmlVersionProvidedException, ReadOnlyStateException {
        AipSip sip = aipSipStore.find(sipId);
        notNull(sip, () -> new MissingObject(AipSip.class, sipId));
        switch (sip.getState()) {
            case PROCESSING:
            case PRE_PROCESSING:
                throw new StillProcessingStateException(sip);
            case ARCHIVAL_FAILURE:
                throw new FailedStateException(sip);
            case ROLLED_BACK:
            case ROLLBACK_FAILURE:
                throw new RollbackStateException(sip);
            case DELETED:
            case DELETION_FAILURE:
                throw new DeletedStateException(sip);
        }
        AipXml latestXml = sip.getLatestXml();
        if (version != null && version != 0) {
            switch (latestXml.getState()) {
                case ARCHIVED:
                    if (latestXml.getVersion() != version - 1)
                        throw new BadXmlVersionProvidedException(version, latestXml.getVersion());
                    break;
                case PROCESSING:
                case PRE_PROCESSING:
                    throw new StillProcessingStateException(latestXml);
                case ARCHIVAL_FAILURE:
                case ROLLED_BACK:
                case ROLLBACK_FAILURE:
                    if (latestXml.getVersion() != version)
                        throw new BadXmlVersionProvidedException(version, latestXml.getVersion() - 1);
                    break;
                default:
                    throw new IllegalStateException("unsupported state: " + latestXml.getState() + " of AIP XML with ID: " + latestXml.getId());
            }
        } else {
            if (latestXml.getState() == ObjectState.ARCHIVED)
                version = latestXml.getVersion() + 1;
            else
                version = latestXml.getVersion();
        }
        //another attempt of previously failed XML versioning
        AipXml aipXml;
        boolean archivalRetry = version == latestXml.getVersion();
        if (archivalRetry) {
            latestXml.setChecksum(xmlChecksum);
            latestXml.setState(ObjectState.PRE_PROCESSING);
            aipXml = latestXml;
        } else {
            //successor of successfully archived previous version
            aipXml = new AipXml(UUID.randomUUID().toString(), xmlChecksum, new User(userDetails.getId()), new AipSip(sipId), version, ObjectState.PRE_PROCESSING);
        }
        return transactionTemplate.execute(status -> {
            if (systemStateService.get().isReadOnly())
                throw new ReadOnlyStateException();
            AipXml xml = aipXmlStore.save(aipXml);
            if (archivalRetry) {
                objectAuditStore.save(new ObjectAudit(xml, new User(userDetails.getId()), AuditedOperation.ARCHIVAL_RETRY));
            }
            ApplicationContextUtils.getProcessingObjects().put(aipXml.getId(), Pair.of(new AtomicBoolean(false), new ReentrantLock()));
            return Pair.of(xml, archivalRetry);
        });
    }

    /**
     * Sets object state to {@link ObjectState#DELETED}
     *
     * @param id id of the object
     * @return
     * @throws StillProcessingStateException
     * @throws RollbackStateException
     * @throws FailedStateException
     * @throws ReadOnlyStateException
     */
    public ArchivalObject deleteObject(String id) throws StillProcessingStateException, RollbackStateException, FailedStateException, ReadOnlyStateException {
        ArchivalObject obj = archivalObjectStore.find(id);
        notNull(obj, () -> {
            log.warn("Could not find object: " + id);
            return new MissingObject(ArchivalObjectDto.class, id);
        });
        if (obj instanceof AipXml) {
            throw new UnsupportedOperationException("AIP XML can't be deleted");
        }
        switch (obj.getState()) {
            case PROCESSING:
            case PRE_PROCESSING:
                throw new StillProcessingStateException(obj);
            case ARCHIVAL_FAILURE:
                throw new FailedStateException(obj);
            case ROLLBACK_FAILURE:
            case ROLLED_BACK:
                throw new RollbackStateException(obj);
        }
        obj.setState(ObjectState.DELETED);
        return transactionTemplate.execute(status -> {
            if (systemStateService.get().isReadOnly())
                throw new ReadOnlyStateException();
            archivalObjectStore.save(obj);
            objectAuditStore.save(new ObjectAudit(obj, new User(userDetails.getId()), AuditedOperation.DELETION));
            return obj;
        });
    }

    /**
     * Sets object state to {@link ObjectState#ROLLED_BACK}
     *
     * @param obj
     * @return
     * @throws ReadOnlyStateException
     */
    public ArchivalObject rollbackObject(ArchivalObject obj) throws ReadOnlyStateException {
        obj.setState(ObjectState.ROLLED_BACK);
        return transactionTemplate.execute(status -> {
            if (systemStateService.get().isReadOnly())
                throw new ReadOnlyStateException();
            archivalObjectStore.save(obj);
            objectAuditStore.save(new ObjectAudit(obj, new User(userDetails.getId()), AuditedOperation.ROLLBACK));
            return obj;
        });
    }

    /**
     * Sets object state to {@link ObjectState#REMOVED}
     *
     * @param id
     * @return
     * @throws DeletedStateException
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     * @throws FailedStateException
     * @throws ReadOnlyStateException
     */
    public ArchivalObject removeObject(String id) throws DeletedStateException, RollbackStateException, StillProcessingStateException, FailedStateException, ReadOnlyStateException {
        ArchivalObject obj = archivalObjectStore.find(id);
        notNull(obj, () -> new MissingObject(ArchivalObjectDto.class, id));
        if (obj instanceof AipXml) {
            throw new UnsupportedOperationException("AIP XML can't be removed");
        }
        switch (obj.getState()) {
            case ROLLED_BACK:
            case ROLLBACK_FAILURE:
                throw new RollbackStateException(obj);
            case DELETED:
            case DELETION_FAILURE:
                throw new DeletedStateException(obj);
            case PROCESSING:
            case PRE_PROCESSING:
                throw new StillProcessingStateException(obj);
            case ARCHIVAL_FAILURE:
                throw new FailedStateException(obj);
        }
        obj.setState(ObjectState.REMOVED);
        return transactionTemplate.execute(status -> {
            if (systemStateService.get().isReadOnly())
                throw new ReadOnlyStateException();
            archivalObjectStore.save(obj);
            objectAuditStore.save(new ObjectAudit(obj, new User(userDetails.getId()), AuditedOperation.REMOVAL));
            return obj;
        });
    }

    /**
     * Sets object state to {@link ObjectState#ARCHIVED}
     *
     * @param id id of the object
     * @return
     * @throws DeletedStateException
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     * @throws FailedStateException
     * @throws ReadOnlyStateException
     */
    public ArchivalObject renewObject(String id) throws DeletedStateException, RollbackStateException, StillProcessingStateException, FailedStateException, ReadOnlyStateException {
        ArchivalObject obj = archivalObjectStore.find(id);
        notNull(obj, () -> {
            log.warn("Could not find object: " + id);
            return new MissingObject(ArchivalObjectDto.class, id);
        });
        if (obj instanceof AipXml) {
            throw new UnsupportedOperationException("AIP XML can't be renewed");
        }
        switch (obj.getState()) {
            case ROLLED_BACK:
            case ROLLBACK_FAILURE:
                throw new RollbackStateException(obj);
            case DELETED:
            case DELETION_FAILURE:
                throw new DeletedStateException(obj);
            case PROCESSING:
            case PRE_PROCESSING:
                throw new StillProcessingStateException(obj);
            case ARCHIVAL_FAILURE:
                throw new FailedStateException(obj);
        }
        obj.setState(ObjectState.ARCHIVED);
        return transactionTemplate.execute(status -> {
            if (systemStateService.get().isReadOnly())
                throw new ReadOnlyStateException();
            archivalObjectStore.save(obj);
            objectAuditStore.save(new ObjectAudit(obj, new User(userDetails.getId()), AuditedOperation.RENEWAL));
            return obj;
        });
    }

    @Transactional
    public void saveObject(ArchivalObject object) {
        archivalObjectStore.save(object);
    }

    @Transactional
    public void setObjectsState(ObjectState state, String... dbIds) {
        ne(state, ObjectState.ARCHIVED, () -> new IllegalArgumentException("setting ARCHIVED state through this method is not allowed"));
        setObjectsStateInternal(state, dbIds);
    }

    @Transactional
    public void setArchived(String userId, ArchivalObjectDto... objects) {
        List<ObjectAudit> audits = Arrays.stream(objects).map(o -> new ObjectAudit(o, new User(userId), AuditedOperation.ARCHIVED)).collect(Collectors.toList());
        objectAuditStore.save(audits);
        setObjectsStateInternal(ObjectState.ARCHIVED, Arrays.stream(objects).map(ArchivalObjectDto::getDatabaseId).toArray(String[]::new));
    }

    /**
     * Retrieves AipSip entity.
     *
     * @param sipId
     * @return AipSip entity with populated list of xmls
     * @throws MissingObject
     */
    public AipSip getAip(String sipId) {
        AipSip sip = aipSipStore.find(sipId);
        notNull(sip, () -> {
            log.warn("Could not find AIP: " + sipId);
            return new MissingObject(AipSip.class, sipId);
        });
        return sip;
    }

    public AipXml getXml(String aipId, int xmlVersion) {
        AipXml xml = aipXmlStore.findBySipAndVersion(aipId, xmlVersion);
        notNull(xml, () -> new MissingObject(AipXml.class, toXmlId(aipId, xmlVersion)));
        return xml;
    }

    /**
     * Retrieves general object entity. Fails if the object does not exist.
     *
     * @param id
     * @return ArchivalObject entity
     * @throws MissingObject
     */
    public ArchivalObject getObject(String id) {
        ArchivalObject archivalObject = archivalObjectStore.find(id);
        notNull(archivalObject, () -> {
            log.warn("Could not find object: " + id);
            return new MissingObject(ArchivalObject.class, id);
        });
        return archivalObject;
    }

    /**
     * Retrieves general object entity. Does not fail if the object does not exist.
     *
     * @param id
     * @return ArchivalObject entity
     */
    public ArchivalObject lookForObject(String id) {
        return archivalObjectStore.find(id);
    }

    public List<ArchivalObject> findObjectsForCleanup(boolean alsoProcessing) {
        return archivalObjectStore.findObjectsForCleanup(alsoProcessing);
    }

    public long getObjectsTotalCount() {
        return archivalObjectStore.countAll();
    }

    /**
     * for every dataspace, random user of those with highest role will become owner of all objects in the dataspace
     *
     * @param storageService
     * @param overrideConflicts
     * @throws StorageException
     */
    @Async
    public void recoverDbDataFromStorage(StorageService storageService, boolean overrideConflicts) throws StorageException {
        String prefix = "DB RECOVERY";
        log.info(prefix + " starting, using " + storageService.getStorage());
        int newRecordsCounter = 0;

        Collection<User> users = userStore.findAll();
        Map<String, User> dataspacesAndOwners = new HashMap<>();
        for (User user : users) {
            if (user.getDataSpace() == null)
                continue;
            if (!dataspacesAndOwners.containsKey(user.getDataSpace())) {
                dataspacesAndOwners.put(user.getDataSpace(), user);
                continue;
            }
            if (user.getRole() == Role.ROLE_ADMIN) {
                dataspacesAndOwners.put(user.getDataSpace(), user);
                continue;
            }
            if (user.getRole() == Role.ROLE_READ_WRITE && dataspacesAndOwners.get(user.getDataSpace()).getRole() == Role.ROLE_READ) {
                dataspacesAndOwners.put(user.getDataSpace(), user);
                continue;
            }
            dataspacesAndOwners.put(user.getDataSpace(), user);
        }

        for (String dataspace : dataspacesAndOwners.keySet()) {
            User user = dataspacesAndOwners.get(dataspace);
            log.info(prefix + " of objects (AIP data parts / AIP XMLs / general objects) in dataspace: " + dataspace + " has started. User " + user + " choose as owner of new records.");
            List<ArchivalObjectDto> objectsAtStorage = storageService.createDtosForAllObjects(dataspace);
            Map<String, ArchivalObjectLightweightView> objectsInDb = archivalObjectLightweightViewStore.findObjectsOfUser(user).stream().collect(Collectors.toMap(o -> o.toDto().getStorageId(), Function.identity()));
            Set<ArchivalObjectLightweightView> conflictObjects = new HashSet<>();

            log.trace(prefix + " found " + objectsAtStorage.size() + " objects at storage");
            log.trace(prefix + " found " + objectsInDb + " objects in database");

            for (ArchivalObjectDto o : objectsAtStorage) {
                ArchivalObjectLightweightView fromDb = objectsInDb.get(o.getStorageId());
                objectsInDb.remove(o.getStorageId());
                if (fromDb == null && o.getState() != ObjectState.FORGOT) {
                    ArchivalObject entity = dtoToEntity(o);
                    entity.setOwner(user);
                    archivalObjectStore.save(entity);
                    newRecordsCounter++;
                } else {
                    if (fromDb.toDto().metadataEquals(o))
                        continue;
                    conflictObjects.add(fromDb);
                    if (overrideConflicts) {
                        ArchivalObject newEntity = dtoToEntity(o);
                        newEntity.setOwner(fromDb.getOwner());
                        newEntity.setId(fromDb.getId());
                        archivalObjectStore.save(newEntity);
                    }
                }
            }
            log.info(prefix + " of data in dataspace: " + dataspace + " has ended. " + newRecordsCounter + " new objects written to DB.");
            if (!conflictObjects.isEmpty()) {
                String action = overrideConflicts ? "metadata in DB were overridden by metadata at storage" : "metadata in DB / at storage were left inconsistent";
                log.warn(prefix + " found " + conflictObjects.size() + " objects which metadata differs between storage and DB. overrideConflicts was set to " + overrideConflicts + ": " + action);
                log.debug(prefix + " conflict objects: " + Arrays.toString(conflictObjects.stream().map(o -> o.toDto().toString()).toArray()));
            }
            Object[] objectsMissingAtStorage = objectsInDb.values().stream().filter(o -> o.getState().metadataMustBeStoredAtLogicalStorage()).map(o -> o.toDto().toString()).toArray();
            if (objectsMissingAtStorage.length != 0) {
                log.error(prefix + " " + objectsMissingAtStorage.length + " objects are missing at storage: " + Arrays.toString(objectsMissingAtStorage));
            }
        }
    }

    /**
     * COMPLETELY deletes the object from DB
     */
    public void forgetObject(ArchivalObject archivalObject) {
        transactionTemplate.executeWithoutResult(status -> {
            if (systemStateService.get().isReadOnly())
                throw new ReadOnlyStateException();
            archivalObjectStore.delete(archivalObject);
            objectAuditStore.save(new ObjectAudit(archivalObject, new User(userDetails.getId()), AuditedOperation.FORGET));
        });
    }

    public AipSip findSip(String id) {
        return aipSipStore.find(id);
    }

    private ArchivalObject dtoToEntity(ArchivalObjectDto o) {
        ArchivalObject entity;
        switch (o.getObjectType()) {
            case OBJECT:
                entity = new ArchivalObject();
                entity.setId(o.getStorageId());
                break;
            case SIP:
                entity = new AipSip();
                entity.setId(o.getStorageId());
                break;
            case XML:
                entity = new AipXml();
                ((AipXml) entity).setVersion(extractXmlVersion(o.getStorageId()));
                ((AipXml) entity).setSip(new AipSip(extractSipId(o.getStorageId())));
                break;
            default:
                throw new IllegalArgumentException("object type not set");
        }
        entity.setChecksum(o.getChecksum());
        entity.setState(o.getState());
        entity.setCreated(o.getCreated());
        entity.setOwner(o.getOwner());
        return entity;
    }

    private void setObjectsStateInternal(ObjectState state, String... dbIds) {
        archivalObjectStore.setObjectsState(state, asList(dbIds));
        if (state != ObjectState.PROCESSING && state != ObjectState.PRE_PROCESSING)
            for (String id : dbIds) {
                ApplicationContextUtils.getProcessingObjects().remove(id);
            }
        log.debug("State of objects with ids " + Arrays.toString(dbIds) + " has changed to " + state + ".");
    }

    @Autowired
    public void setArchivalObjectStore(ArchivalObjectStore archivalObjectStore) {
        this.archivalObjectStore = archivalObjectStore;
    }

    @Autowired
    public void setAipSipStore(AipSipStore store) {
        this.aipSipStore = store;
    }

    @Autowired
    public void setAipXmlStore(AipXmlStore store) {
        this.aipXmlStore = store;
    }

    @Autowired
    public void setObjectAuditStore(ObjectAuditStore objectAuditStore) {
        this.objectAuditStore = objectAuditStore;
    }

    @Autowired
    public void setUserDetails(UserDetails userDetails) {
        this.userDetails = userDetails;
    }

    @Autowired
    public void setSystemStateService(SystemStateService systemStateService) {
        this.systemStateService = systemStateService;
    }

    @Autowired
    public void setTransactionTemplate(PlatformTransactionManager transactionManager, @Value("${arcstorage.stateChangeTransactionTimeout}") int timeout) {
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setPropagationBehaviorName("PROPAGATION_REQUIRES_NEW");
        transactionTemplate.setTimeout(timeout);
        transactionTemplate.afterPropertiesSet();
    }

    @Autowired
    public void setUserStore(UserStore userStore) {
        this.userStore = userStore;
    }

    public void setTransactionTemplateTimeout(int timeout) {
        transactionTemplate.setTimeout(timeout);
    }

    @Autowired
    public void setArchivalObjectLightweightViewStore(ArchivalObjectLightweightViewStore archivalObjectLightweightViewStore) {
        this.archivalObjectLightweightViewStore = archivalObjectLightweightViewStore;
    }
}
