package cz.cas.lib.arcstorage.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import cz.cas.lib.arcstorage.gateway.dto.Checksum;
import cz.cas.lib.arcstorage.store.InstantGenerator;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;

import javax.persistence.*;
import java.time.Instant;

/**
 * Abstract class for core files of archival storage i.e. AIP SIP and AIP XML.
 */
@Getter
@Setter
@MappedSuperclass
@NoArgsConstructor
public abstract class ArchivalObject extends DomainObject {
    @Column(updatable = false, nullable = false)
    @JsonIgnore
    @AttributeOverrides({
            @AttributeOverride(name = "type", column = @Column(name = "checksumType")),
            @AttributeOverride(name = "hash", column = @Column(name = "checksumHash"))
    })
    protected Checksum checksum;
    @Column(updatable = false)
    @GeneratorType(type = InstantGenerator.class, when = GenerationTime.INSERT)
    protected Instant created;
    @Transient
    boolean consistent;

    public ArchivalObject(String id, Checksum checksum) {
        this.id = id;
        this.checksum = checksum;
    }

    public ArchivalObject(Checksum checksum) {
        this.checksum = checksum;
    }
}
