package cz.cas.lib.arcstorage.domain.entity;

import cz.cas.lib.arcstorage.dto.StorageType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Table;

@Getter
@Setter
@Entity
@Table(name = "arcstorage_storage_config")
@NoArgsConstructor
@AllArgsConstructor
public class StorageConfig extends DomainObject {
    String name;
    String host;
    int port;
    int priority;
    /**
     * place to store data, folder path for FS, ZFS, bucket name for CEPH S3
     */
    String location;
    @Enumerated(EnumType.STRING)
    StorageType storageType;
    String note;
    /**
     * config in JSON format
     */
    String config;
    boolean reachable;

    @Override
    public String toString() {
        return "StorageConfig{" +
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