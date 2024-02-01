package cz.cas.lib.arcstorage.storagesync;

/**
 * first archival attempts are not audited
 */
public enum AuditedOperation {
    REMOVAL,
    DELETION,
    RENEWAL,
    /**
     * rollback API requests (not including rollbacks which were done automatically when archival attempt failed)
     */
    ROLLBACK,
    /**
     * archiving after rollback
     */
    ARCHIVAL_RETRY,
    /**
     * complete deletion of the object - from DB and storage
     */
    FORGET,
    ARCHIVED
}
