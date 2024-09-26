package cz.cas.lib.arcstorage.domain.views;

import com.fasterxml.jackson.annotation.JsonIgnore;
import cz.cas.lib.arcstorage.domain.entity.DomainObject;
import cz.cas.lib.arcstorage.domain.entity.User;
import cz.cas.lib.arcstorage.domain.store.InstantGenerator;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ObjectState;
import lombok.Getter;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.GeneratorType;

import jakarta.persistence.*;
import java.time.Instant;

@MappedSuperclass
@Getter
public abstract class ArchivalObjectLightweightView extends DomainObject {

    @Column(updatable = false, nullable = false)
    @JsonIgnore
    @AttributeOverrides({
            @AttributeOverride(name = "type", column = @Column(name = "checksumType")),
            @AttributeOverride(name = "value", column = @Column(name = "checksumValue"))
    })
    protected Checksum checksum;

    @GeneratorType(type = InstantGenerator.class, when = GenerationTime.INSERT)
    protected Instant created;

    @Enumerated(EnumType.STRING)
    protected ObjectState state;

    @ManyToOne
    protected User owner;

    public abstract ArchivalObjectDto toDto();

    @Override
    public String toString() {
        return "ArchivalObject{" +
                "id='" + id + '\'' +
                ", checksum=" + checksum +
                ", created=" + created +
                ", state=" + state +
                ", owner=" + owner +
                '}';
    }
}
