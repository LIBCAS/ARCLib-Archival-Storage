package cz.cas.lib.arcstorage.domain.entity;

import cz.cas.lib.arcstorage.dto.StorageType;
import cz.cas.lib.arcstorage.storage.StorageService;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Getter
@Setter
@Entity
@Table(name = "arcstorage_storage")
@NoArgsConstructor
@AllArgsConstructor
public class Storage extends DomainObject {

    @NotNull
    private String name;

    @NotNull
    private String host;

    private int port;

    /**
     * storage with higher priority is preferred for read against the one with lower priority
     * <p>
     * if {@link SystemState#primaryStorage} is set then it is always the most prioritized no matter this attribute
     * </p>
     */
    private int priority;

    @Enumerated(EnumType.STRING)
    @Column(name = "type")
    @NotNull
    private StorageType storageType;

    private String note;

    /**
     * config in JSON format
     */
    @NotNull
    private String config;

    /**
     * tested and updated by the system automatically, with {@link StorageService#testConnection()}
     */
    private boolean reachable;

    /**
     * timestamp of last detach by admin, if the storage is no longer detached then null
     */
    private Instant detachedByAdmin;

    /**
     * timestamp of last detach by error, if the storage is no longer detached then null
     */
    private Instant detachedByError;

    public Storage(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Storage{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", storageType=" + storageType +
                '}';
    }

    public boolean isDetached() {
        return detachedByAdmin != null || detachedByError != null;
    }
}
