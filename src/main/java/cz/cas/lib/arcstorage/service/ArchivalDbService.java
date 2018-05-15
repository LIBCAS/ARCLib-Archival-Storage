package cz.cas.lib.arcstorage.service;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.domain.store.*;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.service.exception.state.DeletedStateException;
import cz.cas.lib.arcstorage.service.exception.state.FailedStateException;
import cz.cas.lib.arcstorage.service.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.service.exception.state.StillProcessingStateException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static cz.cas.lib.arcstorage.util.Utils.notNull;


/**
 * Class used for communication with Archival Storage database which contains transactional data about Archival Storage packages.
 */
@Service
@Transactional
@Slf4j
public class ArchivalDbService {

    private AipSipStore aipSipStore;
    private AipXmlStore aipXmlStore;
    private ArchivalObjectStore archivalObjectStore;

    /**
     * Registers that AIP creation process has started. Stores AIP records to database and sets their state to <i>processing</i>.
     */
    @org.springframework.transaction.annotation.Transactional(propagation = Propagation.REQUIRES_NEW)
    public void registerAipCreation(String sipId, Checksum sipChecksum, String xmlId, Checksum xmlChecksum) {
        AipSip existingSip = aipSipStore.find(sipId);
        if (existingSip != null && !existingSip.getState().equals(ObjectState.ROLLED_BACK) && !existingSip.getState().equals(ObjectState.FAILED))
            throw new ConflictObject(existingSip);
        AipSip sip = new AipSip(sipId, sipChecksum, ObjectState.PROCESSING);
        AipXml xml = new AipXml(xmlId, xmlChecksum, sip, 1, ObjectState.PROCESSING);
        aipSipStore.save(sip);
        aipXmlStore.save(xml);
    }

    /**
     * Registers that AIP creation process has ended.
     *
     * @param sipId
     * @param xmlId
     */
    public void finishAipCreation(String sipId, String xmlId) {
        setObjectState(sipId, ObjectType.SIP, ObjectState.ARCHIVED);
        setObjectState(xmlId, ObjectType.XML, ObjectState.ARCHIVED);
    }

    /**
     * Registers that AIP XML update process has started.
     *
     * @param sipId
     * @param xmlChecksum
     * @return created XML entity filled ID and version
     */
    @org.springframework.transaction.annotation.Transactional(propagation = Propagation.REQUIRES_NEW)
    public AipXml registerXmlUpdate(String sipId, Checksum xmlChecksum, Optional<Integer> version) {
        if (version.isPresent()) {
            AipXml existingXml = aipXmlStore.findBySipAndVersion(sipId, version.get());
            if (existingXml != null && !existingXml.getState().equals(ObjectState.ROLLED_BACK) && !existingXml.getState().equals(ObjectState.FAILED))
                throw new ConflictObject(existingXml);
            if (existingXml == null)
                existingXml = new AipXml(UUID.randomUUID().toString(), xmlChecksum, new AipSip(sipId), version.get(), ObjectState.PROCESSING);
            else
                existingXml.setState(ObjectState.PROCESSING);
            aipXmlStore.save(existingXml);
            return existingXml;
        } else {
            int xmlVersion = aipXmlStore.getNextXmlVersionNumber(sipId);
            AipXml newVersion = new AipXml(UUID.randomUUID().toString(), xmlChecksum, new AipSip(sipId), xmlVersion, ObjectState.PROCESSING);
            aipXmlStore.save(newVersion);
            return newVersion;
        }
    }

    /**
     * Registers that AIP SIP deletion process has started.
     *
     * @param sipId
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     */
    public void registerSipDeletion(String sipId) throws StillProcessingStateException, RollbackStateException, FailedStateException {
        AipSip sip = aipSipStore.find(sipId);
        notNull(sip, () -> {
            log.warn("Could not find AIP: " + sipId);
            return new MissingObject(AipSip.class, sipId);
        });
        switch (sip.getState()) {
            case PROCESSING:
                throw new StillProcessingStateException(sip);
            case FAILED:
                throw new FailedStateException(sip);
            case ROLLED_BACK:
                throw new RollbackStateException(sip);
        }
        sip.setState(ObjectState.PROCESSING);
        aipSipStore.save(sip);
    }

    /**
     * Sets state of object.
     */
    public void setObjectState(String databaseId, ObjectType objectType, ObjectState state) {
        DomainStore store;
        switch (objectType) {
            case SIP:
                store = aipSipStore;
                break;
            case XML:
                store = aipXmlStore;
                break;
            case OBJECT:
                store = archivalObjectStore;
                break;
            default:
                throw new IllegalArgumentException("null object type");
        }
        ArchivalObject object = (ArchivalObject) store.find(databaseId);
        object.setState(state);
        store.save(object);
    }

    /**
     * Called when the creation and also rollback processes of AIP SIP failed.
     *
     * @param sipId
     * @param xmlId
     */
    public void setAipFailed(String sipId, String xmlId) {
        AipSip sip = aipSipStore.find(sipId);
        notNull(sip, () -> {
            log.warn("Could not find AIP: " + sipId);
            return new MissingObject(AipSip.class, sipId);
        });
        sip.setState(ObjectState.FAILED);
        aipSipStore.save(sip);

        AipXml xml = aipXmlStore.find(xmlId);
        notNull(xml, () -> {
            log.warn("Could not find XML: " + xmlId);
            return new MissingObject(AipXml.class, xmlId);
        });
        xml.setState(ObjectState.FAILED);
        aipXmlStore.save(xml);
    }

    /**
     * Logically removes SIP i.e. sets its state to {@link ObjectState#REMOVED} in the database.
     *
     * @param sipId
     * @throws DeletedStateException         if SIP is deleted
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     */
    public void removeAip(String sipId) throws DeletedStateException, RollbackStateException, StillProcessingStateException, FailedStateException {
        AipSip sip = aipSipStore.find(sipId);
        notNull(sip, () -> {
            log.warn("Could not find AIP: " + sipId);
            return new MissingObject(AipSip.class, sipId);
        });
        switch (sip.getState()) {
            case ROLLED_BACK:
                throw new RollbackStateException(sip);
            case DELETED:
                throw new DeletedStateException(sip);
            case PROCESSING:
                throw new StillProcessingStateException(sip);
            case FAILED:
                throw new FailedStateException(sip);
        }
        sip.setState(ObjectState.REMOVED);
        aipSipStore.save(sip);
    }

    /**
     * Renews logically removed SIP i.e. sets its state to {@link ObjectState#REMOVED} in the database.
     *
     * @param sipId
     * @throws DeletedStateException         if SIP is deleted
     * @throws RollbackStateException
     * @throws StillProcessingStateException
     */
    public void renewAip(String sipId) throws DeletedStateException, RollbackStateException, StillProcessingStateException, FailedStateException {
        AipSip sip = aipSipStore.find(sipId);
        notNull(sip, () -> {
            log.warn("Could not find AIP: " + sipId);
            return new MissingObject(AipSip.class, sipId);
        });
        switch (sip.getState()) {
            case ROLLED_BACK:
                throw new RollbackStateException(sip);
            case DELETED:
                throw new DeletedStateException(sip);
            case PROCESSING:
                throw new StillProcessingStateException(sip);
            case FAILED:
                throw new FailedStateException(sip);
        }
        sip.setState(ObjectState.ARCHIVED);
        aipSipStore.save(sip);
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
            log.warn("Could not find AIP: %s" + sipId);
            return new MissingObject(AipSip.class, sipId);
        });
        return sip;
    }

    /**
     * Rollback SIP and related XML. Used when the AIP creation process fails.
     *
     * @param id
     */
    public void rollbackAip(String id, String xmlId) {
        setObjectState(id, ObjectType.SIP, ObjectState.ROLLED_BACK);
        setObjectState(xmlId, ObjectType.XML, ObjectState.ROLLED_BACK);
    }

    /**
     * Fill initialized lists passed as parameters with records of files in processing state.
     *
     * @param unfinishedSips
     * @param unfinishedXmls
     */
    public void fillUnfinishedFilesLists(List<AipSip> unfinishedSips, List<AipXml> unfinishedXmls) {
        unfinishedSips.addAll(aipSipStore.findUnfinishedSips());
        unfinishedXmls.addAll(aipXmlStore.findUnfinishedXmls());
    }

    /**
     * Deletes records of files in processing state.
     */
    public void rollbackUnfinishedFilesRecords() {
        aipSipStore.rollbackUnfinishedSipsRecords();
        aipXmlStore.rollbackUnfinishedXmlsRecords();
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
}
