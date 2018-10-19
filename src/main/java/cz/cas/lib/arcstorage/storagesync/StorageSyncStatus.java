package cz.cas.lib.arcstorage.storagesync;

import cz.cas.lib.arcstorage.domain.entity.DomainObject;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.store.InstantGenerator;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;

import javax.persistence.*;
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
    @GeneratorType( type = InstantGenerator.class, when = GenerationTime.ALWAYS)
    private Instant updated;
    @Setter
    @Enumerated(EnumType.STRING)
    private StorageSyncPhase phase;
    @Setter
    private long totalInThisPhase;
    @Setter
    private long doneInThisPhase;
    private Class exceptionClass;
    private String exceptionMsg;
    @Setter
    private Instant stuckAt;

    public void setExceptionInfo(Class exceptionClass, String exceptionMsg, Instant problemObjectCreationTime) {
        this.exceptionClass = exceptionClass;
        this.exceptionMsg = exceptionMsg;
        this.stuckAt = problemObjectCreationTime;
    }

    public void clearExeptionInfo() {
        exceptionClass = null;
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
                ", exceptionClass=" + exceptionClass +
                ", exceptionMsg='" + exceptionMsg + '\'' +
                ", stuckAt=" + stuckAt +
                '}';
    }
}
