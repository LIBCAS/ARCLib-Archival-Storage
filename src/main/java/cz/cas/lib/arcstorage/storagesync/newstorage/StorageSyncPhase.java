package cz.cas.lib.arcstorage.storagesync.newstorage;

import cz.cas.lib.arcstorage.storagesync.AuditedOperation;

public enum StorageSyncPhase {
    /** waiting for the quiet time without new writes, creating dataspace on the new storage**/
    INIT,
    /**
     * copying those objects which were already archived when sync started
     */
    COPYING_ARCHIVED_OBJECTS,
    /**
     * propagating REMOVE, RENEW and DELETE operations according to {@link AuditedOperation} log
     * and setting system to read-only mode at the end
     */
    PROPAGATING_OPERATIONS,
    /**
     * checking state of all copied objects at new storage
     */
    POST_SYNC_CHECK,
    /**
     * done
     */
    DONE
}
