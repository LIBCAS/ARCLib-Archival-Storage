package cz.cas.lib.arcstorage.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import cz.cas.lib.arcstorage.store.InstantGenerator;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;

import javax.persistence.*;
import java.time.Instant;

/**
 * Abstract class for core files of archival storage i.e. AipSip data and AipSip XML.
 */
@Getter
@Setter
@MappedSuperclass
@NoArgsConstructor
public abstract class ArchivalObject extends DomainObject {
    @Column(updatable = false, nullable = false)
    @JsonIgnore
    protected String md5;
    @Column(updatable = false)
    @GeneratorType(type = InstantGenerator.class, when = GenerationTime.INSERT)
    protected Instant created;
    @Transient
    boolean consistent;

    public ArchivalObject(String id, String md5) {
        this.id = id;
        this.md5 = md5;
    }

    public ArchivalObject(String md5) {
        this.md5 = md5;
    }
}
