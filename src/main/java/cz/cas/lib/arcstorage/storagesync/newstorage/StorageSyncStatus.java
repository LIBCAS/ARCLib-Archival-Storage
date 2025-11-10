package cz.cas.lib.arcstorage.storagesync.newstorage;

import cz.cas.lib.arcstorage.domain.entity.DomainObject;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import jakarta.persistence.Entity;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.Instant;

@Getter
@Entity
@Table(name = "arcstorage_storage_sync_status")
@NoArgsConstructor
public class StorageSyncStatus extends DomainObject {

    @ManyToOne
    private Storage storage;

    @CreationTimestamp
    private Instant created;

    @UpdateTimestamp
    private Instant updated;

    /**
     * number of objects to copy
     */
    @Setter
    private long total;

    /**
     * number of objects already copied
     */
    @Setter
    private long done;

    private String exceptionMsg;

    private String exceptionStackTrace;

    public void setExceptionInfo(Throwable ex) {
        this.exceptionStackTrace = ExceptionUtils.getStackTrace(ex);
        this.exceptionMsg = ex.toString();
    }

    public void clearExceptionInfo() {
        exceptionStackTrace = null;
        exceptionMsg = null;
    }

    public StorageSyncStatus(Storage storage) {
        this.storage = storage;
    }

    public boolean isFinished() {
        return done == total;
    }

    @Override
    public String toString() {
        return "StorageSyncStatus{" +
                "storage=" + storage +
                ", created=" + created +
                ", updated=" + updated +
                ", total=" + total +
                ", done=" + done +
                ", exceptionMsg='" + exceptionMsg + '\'' +
                '}';
    }
}
