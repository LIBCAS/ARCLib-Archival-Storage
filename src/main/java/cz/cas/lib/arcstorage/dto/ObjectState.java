package cz.cas.lib.arcstorage.dto;

/**
 * Object states are used to control the application logic.
 * In addition to database they are stored at storage layer to be able to reconstructs them in the case the database is lost.
 */
public enum ObjectState {
    //phase before processing, during which, for example, the checksum of the object is verified after the transfer
    PRE_PROCESSING(false, false, true, false),
    //object is being processed (creating/deleting/rollback), used for both SIP and XML, stored also at storage layer
    PROCESSING(false, false, true, false),
    //object creation has finished and now is archived, used for both SIP and XML, stored also at storage layer
    ARCHIVED(true, true, false, false),
    //object processing has failed and so its file content was physically deleted from the storage, used for both SIP and XML, record itself remains in database and also in storage
    ROLLED_BACK(true, false, false, false),
    //object has been physically deleted from the storage, used only for SIP (XMLs cant be deleted), stored also at storage layer
    DELETED(true, false, false, false),
    //object has been logically removed: it exists in storage but should not be accessible to all users, used only for SIP (XMLs cant be removed), stored also at storage layer
    REMOVED(true, true, false, false),
    //object archiving has failed and following rollback has also failed, this state is held only in DB
    ARCHIVAL_FAILURE(false, false, false, true),
    //object deletion has failed, this state is held only in DB
    DELETION_FAILURE(false, false, false, true),
    //object rollback has failed, this state is held only in DB
    ROLLBACK_FAILURE(false, false, false, true),
    //object has been forgotten - completely deleted from DB and storage - only the metadata file with object state is kept at storage
    FORGOT(true, false, false, false);

    /**
     * states which must be in sync between storage and database
     */
    private boolean metadataMustBeStoredAtLogicalStorage;
    private boolean contentMustBeStoredAtLogicalStorage;
    /**
     * processing states, may be inconsistent between storage and DB, possible candidates for cleanup
     */
    private boolean processing;
    /**
     * failure states, may be inconsistent between storage and DB, candidates for cleanup
     */
    private boolean fail;

    ObjectState(boolean metadataMustBeStoredAtLogicalStorage, boolean contentMustBeStoredAtLogicalStorage, boolean processing, boolean fail) {
        this.metadataMustBeStoredAtLogicalStorage = metadataMustBeStoredAtLogicalStorage;
        this.contentMustBeStoredAtLogicalStorage = contentMustBeStoredAtLogicalStorage;
        this.processing = processing;
        this.fail = fail;
    }

    public boolean metadataMustBeStoredAtLogicalStorage() {
        return metadataMustBeStoredAtLogicalStorage;
    }

    public boolean contentMustBeStoredAtLogicalStorage() {
        return contentMustBeStoredAtLogicalStorage;
    }

    public boolean isProcessing() {
        return processing;
    }

    public boolean isFail() {
        return fail;
    }
}
