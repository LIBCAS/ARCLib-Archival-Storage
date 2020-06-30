package cz.cas.lib.arcstorage.domain.entity;

import cz.cas.lib.arcstorage.dto.StorageType;
import cz.cas.lib.arcstorage.storage.StorageService;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import javax.validation.constraints.NotNull;

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
    private String config;
    /**
     * tested and updated by the system automatically, with {@link StorageService#testConnection()}
     */
    private boolean reachable;
    /**
     * set to true for new storage added to the running system, managed completely by the system
     */
    private boolean synchronizing;

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
}
