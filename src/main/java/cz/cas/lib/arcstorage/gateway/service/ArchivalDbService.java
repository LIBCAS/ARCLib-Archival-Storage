package cz.cas.lib.arcstorage.gateway.service;

import cz.cas.lib.arcstorage.domain.AipSip;
import cz.cas.lib.arcstorage.domain.AipXml;
import cz.cas.lib.arcstorage.domain.ObjectState;
import cz.cas.lib.arcstorage.exception.BadArgument;
import cz.cas.lib.arcstorage.exception.ConflictObject;
import cz.cas.lib.arcstorage.exception.MissingObject;
import cz.cas.lib.arcstorage.gateway.dto.Checksum;
import cz.cas.lib.arcstorage.gateway.exception.state.DeletedStateException;
import cz.cas.lib.arcstorage.gateway.exception.state.FailedStateException;
import cz.cas.lib.arcstorage.gateway.exception.state.RollbackStateException;
import cz.cas.lib.arcstorage.gateway.exception.state.StillProcessingStateException;
import cz.cas.lib.arcstorage.store.AipSipStore;
import cz.cas.lib.arcstorage.store.AipXmlStore;
import cz.cas.lib.arcstorage.store.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;

import javax.inject.Inject;
import java.util.List;

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

    /**
     * Registers that AIP creation process has started. Stores AIP records to database and sets their state to <i>processing</i>.
     *
     * @param sipId
     * @param sipChecksum
     * @param xmlChecksum
     * @return generated ID of XML record
     */
    @org.springframework.transaction.annotation.Transactional(propagation = Propagation.REQUIRES_NEW)
    public String registerAipCreation(String sipId, Checksum sipChecksum, Checksum xmlChecksum) {
        notNull(sipId, () -> new BadArgument(sipId));
        notNull(sipChecksum, () -> new BadArgument(sipChecksum));
        notNull(xmlChecksum, () -> new BadArgument(xmlChecksum));

        AipSip sip = aipSipStore.find(sipId);
        if (sip != null)
            throw new ConflictObject(sip);
        sip = new AipSip(sipId, sipChecksum, ObjectState.PROCESSING);
        AipXml xml = new AipXml(xmlChecksum, new AipSip(sipId), 1, ObjectState.PROCESSING);
        aipSipStore.save(sip);
        aipXmlStore.save(xml);
        return xml.getId();
    }

    /**
     * Registers that AIP creation process has ended.
     *
     * @param sipId
     * @param xmlId
     */
    public void finishAipCreation(String sipId, String xmlId) {
        AipSip sip = aipSipStore.find(sipId);
        sip.setState(ObjectState.ARCHIVED);
        aipSipStore.save(sip);
        finishXmlProcess(xmlId);
    }

    /**
     * Registers that AIP XML update process has started.
     *
     * @param sipId
     * @param xmlChecksum
     * @return created XML entity filled with generated ID and version
     */
    public AipXml registerXmlUpdate(String sipId, Checksum xmlChecksum) {
        notNull(sipId, () -> new BadArgument(sipId));
        notNull(xmlChecksum, () -> new BadArgument(xmlChecksum));

        int xmlVersion = aipXmlStore.getNextXmlVersionNumber(sipId);
        AipXml newVersion = new AipXml(xmlChecksum, new AipSip(sipId), xmlVersion, ObjectState.PROCESSING);
        aipXmlStore.save(newVersion);
        return newVersion;
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
            case ROLLBACKED:
                throw new RollbackStateException(sip);
        }
        sip.setState(ObjectState.PROCESSING);
        aipSipStore.save(sip);
    }

    /**
     * Registers that AIP SIP deletion process has ended.
     *
     * @param sipId
     */
    public void finishSipDeletion(String sipId) {
        AipSip sip = aipSipStore.find(sipId);
        sip.setState(ObjectState.DELETED);
        aipSipStore.save(sip);
    }

    /**
     * Registers that process which used AIP XML file has ended.
     *
     * @param xmlId
     */
    public void finishXmlProcess(String xmlId) {
        AipXml xml = aipXmlStore.find(xmlId);
        xml.setState(ObjectState.ARCHIVED);
        aipXmlStore.save(xml);
    }

    /**
     * Called when the creation and also rollback processes of AIP SIP failed.
     *
     * @param sipId
     * @param xmlId
     */
    public void setSipFailed(String sipId, String xmlId) {
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
     * Called when the update and also rollback processes of AIP XML failed.
     *
     * @param xmlId
     */
    public void setXmlFailed(String xmlId) {
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
    public void removeSip(String sipId) throws DeletedStateException, RollbackStateException, StillProcessingStateException, FailedStateException {
        AipSip sip = aipSipStore.find(sipId);
        notNull(sip, () -> {
            log.warn("Could not find AIP: " + sipId);
            return new MissingObject(AipSip.class, sipId);
        });
        switch (sip.getState()) {
            case ROLLBACKED:
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
    public void rollbackSip(String id, String xmlId) {
        AipSip sip = aipSipStore.find(id);
        sip.setState(ObjectState.ROLLBACKED);
        aipSipStore.save(sip);
        rollbackXml(xmlId);
    }

    /**
     * Rollback XML. Used when the XML update process or AIP creation process fails.
     *
     * @param id
     */
    public void rollbackXml(String id) {
        AipXml xml = aipXmlStore.find(id);
        xml.setState(ObjectState.ROLLBACKED);
        aipXmlStore.save(xml);
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
    public void setAipSipStore(AipSipStore store) {
        this.aipSipStore = store;
    }

    @Inject
    public void setAipXmlStore(AipXmlStore store) {
        this.aipXmlStore = store;
    }
}
