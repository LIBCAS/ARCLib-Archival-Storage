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
     * set to true for new storage added to the running system, or set to true manually by admin for whatever reason
     */
    private boolean writeOnly;
    /**
     * tested and updated by the system automatically, with {@link StorageService#testConnection()}
     */
    private boolean reachable;

    public Storage(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Storage{" +
                "name='" + name + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", priority=" + priority +
                ", storageType=" + storageType +
                ", note='" + note + '\'' +
                ", config='" + config + '\'' +
                ", writeOnly=" + writeOnly +
                ", reachable=" + reachable +
                '}';
    }
}
