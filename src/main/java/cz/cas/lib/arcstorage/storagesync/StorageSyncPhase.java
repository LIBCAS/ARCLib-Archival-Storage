package cz.cas.lib.arcstorage.storagesync;

public enum StorageSyncPhase {
    INIT,
    /**
     * those which were already archived when sync started
     */
    COPYING_ARCHIVED_OBJECTS,
    /**
     * propagating REMOVE, RENEW and DELETE operations according to {@link AuditedOperation} log
     */
    PROPAGATING_OPERATIONS,
    FINISHING,
    DONE
}
