package cz.cas.lib.arcstorage.domain.entity;

import cz.cas.lib.arcstorage.dto.StorageType;
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
    String name;
    @NotNull
    String host;
    int port;
    int priority;
    /**
     * place to save data, folder path for FS, ZFS, bucket name for CEPH S3
     */
    @NotNull
    String location;
    @Enumerated(EnumType.STRING)
    @Column(name = "type")
    @NotNull
    StorageType storageType;
    String note;
    /**
     * config in JSON format
     */
    String config;
    boolean reachable;

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
                ", location='" + location + '\'' +
                ", storageType=" + storageType +
                ", note='" + note + '\'' +
                ", config='" + config + '\'' +
                ", reachable=" + reachable +
                '}';
    }
}
