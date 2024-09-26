package cz.cas.lib.arcstorage.domain.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.*;
import org.hibernate.annotations.BatchSize;

import java.time.Instant;

@Entity
@Table(name = "arcstorage_system_state")
@NoArgsConstructor
@Getter
@Setter
@BatchSize(size = 100)
@AllArgsConstructor
@ToString
public class SystemState extends DomainObject {

    private int minStorageCount;

    private boolean readOnly;

    private int reachabilityCheckIntervalInMinutes;

    private Instant lastReachabilityCheck;

    private Instant lastVerifiedObjectCreation;

    public SystemState(int minStorageCount, boolean readOnly) {
        this.minStorageCount = minStorageCount;
        this.readOnly = readOnly;
    }

}
