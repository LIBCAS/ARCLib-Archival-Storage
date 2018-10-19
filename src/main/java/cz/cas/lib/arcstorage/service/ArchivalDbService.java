package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.*;
import cz.cas.lib.arcstorage.domain.store.*;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.security.user.UserDetails;
import cz.cas.lib.arcstorage.security.user.UserStore;
import cz.cas.lib.arcstorage.service.exception.BadXmlVersionProvidedException;
import cz.cas.lib.arcstorage.service.exception.ReadOnlyStateException;
import cz.cas.lib.arcstorage.service.exception.state.DeletedStateException;
import cz.cas.lib.arcstorage.service.exception.state.FailedStateException;
import cz.cas.lib.arcstorage.service.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.service.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.storagesync.AuditedOperation;
import cz.cas.lib.arcstorage.storagesync.ObjectAudit;
import cz.cas.lib.arcstorage.storagesync.ObjectAuditStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import javax.inject.Inject;
import java.util.List;
import java.util.UUID;

import static cz.cas.lib.arcstorage.util.Utils.notNull;

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
    private ConfigurationStore configurationStore;
    private TransactionTemplate transactionTemplate;

    /**
     * Registers that AIP creation process has started. Stores AIP records to database and sets their state to <i>processing</i>.
     */
    public AipSip registerAipCreation(String sipId, Checksum sipChecksum, String xmlId, Checksum xmlChecksum) throws ReadOnlyStateException {
        AipSip existingSip = aipSipStore.find(sipId);
        if (existingSip != null &&
                !existingSip.getState().equals(ObjectState.ROLLED_BACK) &&
                !existingSip.getState().equals(ObjectState.ARCHIVAL_FAILURE))
            throw new ConflictObject(existingSip);
        User user = userStore.find(userDetails.getId());
        AipSip sip = new AipSip(sipId, sipChecksum, user, ObjectState.PRE_PROCESSING);
        AipXml xml;
        if (existingSip != null && existingSip.getXml(0) != null)
            xml = existingSip.getXml(0);
        else
            xml = new AipXml(xmlId, xmlChecksum, new User(userDetails.getId()), sip, 1, ObjectState.PRE_PROCESSING);
        return transactionTemplate.execute(new TransactionCallback<AipSip>() {
            @Override
            public AipSip doInTransaction(TransactionStatus status) {
                if (configurationStore.get().isReadOnly())
                    throw new ReadOnlyStateException();
                aipSipStore.save(sip);
                aipXmlStore.save(xml);
                log.info("Creation of AIP with id " + sip + " has been registered.");
                return sip;
            }
        });
    }

    /**
     * Registers that AIP creation process has ended.
     *
     * @param sipId
     * @param xmlId
     */
    @Transactional
    public void finishAipCreation(String sipId, String xmlId) {
        setObjectState(sipId, ObjectState.ARCHIVED);
        setObjectState(xmlId, ObjectState.ARCHIVED);
    }

    /**
     * Registers that AIP XML update process has started.
     *
     * @param sipId
     * @param xmlChecksum
     * @return created XML entity filled ID and version
     */
    public AipXml registerXmlUpdate(String sipId, Checksum xmlChecksum, Integer version) throws StillProcessingStateException, FailedStateException, RollbackStateException, DeletedStateException, BadXmlVersionProvidedException, ReadOnlyStateException {
        AipSip sip = aipSipStore.find(sipId);
        notNull(sip, () -> new MissingObject(AipSip.class, sipId));
        switch (sip.getState()) {
            case PROCESSING:
            case PRE_PROCESSING:
                throw new StillProcessingStateException(sip);
            case ARCHIVAL_FAILURE:
                throw new FailedStateException(sip);
            case ROLLED_BACK:
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
        int xmlVersion = version;
        return transactionTemplate.execute(new TransactionCallback<AipXml>() {
            @Override
            public AipXml doInTransaction(TransactionStatus status) {
                if (configurationStore.get().isReadOnly())
                    throw new ReadOnlyStateException();
                AipXml save = aipXmlStore.save(new AipXml(UUID.randomUUID().toString(), xmlChecksum, new User(userDetails.getId()), new AipSip(sipId), xmlVersion, ObjectState.PRE_PROCESSING));
                return save;
            }
        });
    }

    /**
     * Registers object deletion.
     *
     * @param id
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     */
    public ArchivalObject registerObjectDeletion(String id) throws StillProcessingStateException, RollbackStateException, FailedStateException, ReadOnlyStateException {
        ArchivalObject obj = archivalObjectStore.find(id);
        notNull(obj, () -> {
            log.warn("Could not find object: " + id);
            return new MissingObject(ArchivalObjectDto.class, id);
        });
        switch (obj.getState()) {
            case PROCESSING:
            case PRE_PROCESSING:
                throw new StillProcessingStateException(obj);
            case ARCHIVAL_FAILURE:
                throw new FailedStateException(obj);
            case ROLLED_BACK:
                throw new RollbackStateException(obj);
        }
        obj.setState(ObjectState.DELETED);
        return transactionTemplate.execute(new TransactionCallback<ArchivalObject>() {
            @Override
            public ArchivalObject doInTransaction(TransactionStatus status) {
                if (configurationStore.get().isReadOnly())
                    throw new ReadOnlyStateException();
                archivalObjectStore.save(obj);
                objectAuditStore.save(new ObjectAudit(id, new User(userDetails.getId()), getObjectType(obj), AuditedOperation.DELETION));
                return obj;
            }
        });
    }

    /**
     * Sets state of object.
     */
    @Transactional
    public void setObjectState(String databaseId, ObjectState state) {
        ArchivalObject object = archivalObjectStore.find(databaseId);
        object.setState(state);
        archivalObjectStore.save(object);
        log.info("State of object with id " + databaseId + " has changed to " + state + ".");
    }

    @Transactional
    public void saveObject(ArchivalObject object) {
        archivalObjectStore.save(object);
    }

    @Transactional
    public void setObjectsState(ObjectState state, List<String> ids) {
        archivalObjectStore.setObjectsState(state, ids);
        log.info("State of objects with ids " + ids.toString() + " has changed to " + state + ".");
    }

    /**
     * Called when the creation and also rollback processes of AIP SIP failed.
     *
     * @param sipId
     * @param xmlId
     */
    @Transactional
    public void setAipFailed(String sipId, String xmlId) {
        AipSip sip = aipSipStore.find(sipId);
        notNull(sip, () -> {
            log.warn("Could not find AIP: " + sipId);
            return new MissingObject(AipSip.class, sipId);
        });
        sip.setState(ObjectState.ARCHIVAL_FAILURE);
        aipSipStore.save(sip);

        AipXml xml = aipXmlStore.find(xmlId);
        notNull(xml, () -> {
            log.warn("Could not find XML: " + xmlId);
            return new MissingObject(AipXml.class, xmlId);
        });
        xml.setState(ObjectState.ARCHIVAL_FAILURE);
        aipXmlStore.save(xml);
    }

    /**
     * Logically removes object i.e. sets its state to {@link ObjectState#REMOVED} in the database.
     *
     * @param id
     * @throws DeletedStateException         if object is deleted
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     */
    public ArchivalObject removeObject(String id) throws DeletedStateException, RollbackStateException, StillProcessingStateException, FailedStateException, ReadOnlyStateException {
        ArchivalObject obj = archivalObjectStore.find(id);
        notNull(obj, () -> {
            log.warn("Could not find object: " + id);
            return new MissingObject(ArchivalObjectDto.class, id);
        });
        switch (obj.getState()) {
            case ROLLED_BACK:
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
        return transactionTemplate.execute(new TransactionCallback<ArchivalObject>() {
            @Override
            public ArchivalObject doInTransaction(TransactionStatus status) {
                if (configurationStore.get().isReadOnly())
                    throw new ReadOnlyStateException();
                archivalObjectStore.save(obj);
                objectAuditStore.save(new ObjectAudit(id, new User(userDetails.getId()), getObjectType(obj), AuditedOperation.REMOVAL));
                return obj;
            }
        });
    }

    /**
     * Renews logically removed object i.e. sets its state to {@link ObjectState#ARCHIVED} in the database.
     *
     * @param id
     * @throws DeletedStateException         if object is deleted
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     */
    public ArchivalObject renewObject(String id) throws DeletedStateException, RollbackStateException, StillProcessingStateException, FailedStateException, ReadOnlyStateException {
        ArchivalObject obj = archivalObjectStore.find(id);
        notNull(obj, () -> {
            log.warn("Could not find object: " + id);
            return new MissingObject(ArchivalObjectDto.class, id);
        });
        switch (obj.getState()) {
            case ROLLED_BACK:
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
        return transactionTemplate.execute(new TransactionCallback<ArchivalObject>() {
            @Override
            public ArchivalObject doInTransaction(TransactionStatus status) {
                if (configurationStore.get().isReadOnly())
                    throw new ReadOnlyStateException();
                archivalObjectStore.save(obj);
                objectAuditStore.save(new ObjectAudit(id, new User(userDetails.getId()), getObjectType(obj), AuditedOperation.RENEWAL));
                return obj;
            }
        });
    }

    /**
     * Retrieves AipSip entity.
     *
     * @param sipId
     * @return AipSip entity with populated list of xmls
     */
    public AipSip getAip(String sipId) {
        AipSip sip = aipSipStore.find(sipId);
        notNull(sip, () -> {
            log.warn("Could not find AIP: " + sipId);
            return new MissingObject(AipSip.class, sipId);
        });
        return sip;
    }

    /**
     * Retrieves general object entity.
     *
     * @param id
     * @return ArchivalObject entity
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
     * Rollback SIP and related XML. Used when the AIP creation process fails.
     *
     * @param id
     */
    @Transactional
    public void rollbackAip(String id, String xmlId) {
        setObjectState(id, ObjectState.ROLLED_BACK);
        setObjectState(xmlId, ObjectState.ROLLED_BACK);
    }

    public List<ArchivalObject> findObjectsForCleanup(boolean alsoProcessing) {
        return archivalObjectStore.findObjectsForCleanup(alsoProcessing);
    }

    public long getObjectsTotalCount() {
        return archivalObjectStore.countAll();
    }

    private ObjectType getObjectType(ArchivalObject obj) {
        if (obj instanceof AipXml)
            return ObjectType.XML;
        if (obj instanceof AipSip)
            return ObjectType.SIP;
        return ObjectType.OBJECT;
    }


    @Inject
    public void setArchivalObjectStore(ArchivalObjectStore archivalObjectStore) {
        this.archivalObjectStore = archivalObjectStore;
    }

    @Inject
    public void setAipSipStore(AipSipStore store) {
        this.aipSipStore = store;
    }

    @Inject
    public void setAipXmlStore(AipXmlStore store) {
        this.aipXmlStore = store;
    }

    @Inject
    public void setObjectAuditStore(ObjectAuditStore objectAuditStore) {
        this.objectAuditStore = objectAuditStore;
    }

    @Inject
    public void setUserDetails(UserDetails userDetails) {
        this.userDetails = userDetails;
    }

    @Inject
    public void setConfigurationStore(ConfigurationStore configurationStore) {
        this.configurationStore = configurationStore;
    }

    @Inject
    public void setTransactionTemplate(PlatformTransactionManager transactionManager, @Value("${arcstorage.state-change-transaction-timeout}") int timeout) {
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setTimeout(timeout);
    }

    @Inject
    public void setUserStore(UserStore userStore) {
        this.userStore = userStore;
    }

    public void setTransactionTemplateTimeout(int timeout) {
        transactionTemplate.setTimeout(timeout);
    }
}
