package cz.cas.lib.arcstorage.storagesync.newstorage;

import cz.cas.lib.arcstorage.domain.entity.DomainObject;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.store.InstantGenerator;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;

import java.time.Instant;

@Getter
@Entity
@Table(name = "arcstorage_storage_sync_status")
@NoArgsConstructor
public class StorageSyncStatus extends DomainObject {

    @ManyToOne
    private Storage storage;

    @Setter
    private Instant created;

    @GeneratorType(type = InstantGenerator.class, when = GenerationTime.ALWAYS)
    private Instant updated;

    @Setter
    @Enumerated(EnumType.STRING)
    private StorageSyncPhase phase;

    /**
     * number of objects/operations to be synced.. if the sync was stopped and then continues, this number does not contain
     * objects/operations which were synced previously
     */
    @Setter
    private long totalInThisPhase;

    /**
     * number of objects/operations already synced
     */
    @Setter
    private long doneInThisPhase;

    private String exceptionMsg;

    private String exceptionStackTrace;

    /**
     * marks the latest object/record which was not yet synchronized to a new storage
     * when sync fails, the timestamp is marked, admin has to solve the failure and can then continue with the sync
     */
    @Setter
    private Instant stuckAt;

    public void setExceptionInfo(Exception ex, Instant problemObjectCreationTime) {
        setExceptionInfo(ex);
        this.stuckAt = problemObjectCreationTime;
    }

    /**
     * should be used only when the exception is not related to processing of some object, e.g. when whole storage is not reachable
     */
    public void setExceptionInfo(Exception ex) {
        this.exceptionStackTrace = ExceptionUtils.getStackTrace(ex);
        this.exceptionMsg = ex.toString();
    }

    public void clearExeptionInfo() {
        exceptionStackTrace = null;
        exceptionMsg = null;
        stuckAt = null;
    }

    public StorageSyncStatus(Storage storage) {
        this.storage = storage;
        this.phase = StorageSyncPhase.INIT;
    }

    @Override
    public String toString() {
        return "StorageSyncStatus{" +
                "storage=" + storage +
                ", created=" + created +
                ", updated=" + updated +
                ", phase=" + phase +
                ", totalInThisPhase=" + totalInThisPhase +
                ", doneInThisPhase=" + doneInThisPhase +
                ", exceptionMsg='" + exceptionMsg + '\'' +
                ", stuckAt=" + stuckAt +
                '}';
    }
}
