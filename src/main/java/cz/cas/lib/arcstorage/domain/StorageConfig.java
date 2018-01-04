package cz.cas.lib.arcstorage.domain;

import cz.cas.lib.arcstorage.gateway.dto.StorageType;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Table;

@Getter
@Setter
@Entity
@Table(name = "arcstorage_storage_config")
public class StorageConfig extends DomainObject {
    String name;
    String host;
    int port;
    int priority;
    String sipLocation;
    String xmlLocation;
    @Enumerated(EnumType.STRING)
    StorageType storageType;
    String note;
    /**
     * config in JSON format
     */
    String config;
}