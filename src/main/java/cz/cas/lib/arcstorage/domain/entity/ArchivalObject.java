package cz.cas.lib.arcstorage.domain.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import cz.cas.lib.arcstorage.domain.store.InstantGenerator;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ObjectState;
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
@NoArgsConstructor
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
@Entity
@Table(name = "arcstorage_object")
public class ArchivalObject extends DomainObject {
    @Column(updatable = false, nullable = false)
    @JsonIgnore
    @AttributeOverrides({
            @AttributeOverride(name = "type", column = @Column(name = "checksumType")),
            @AttributeOverride(name = "value", column = @Column(name = "checksumValue"))
    })
    private Checksum checksum;

    @GeneratorType(type = InstantGenerator.class, when = GenerationTime.INSERT)
    private Instant created;

    @Enumerated(EnumType.STRING)
    private ObjectState state;

    @ManyToOne
    private User owner;

    public ArchivalObject(Checksum checksum, User owner, ObjectState state) {
        this.checksum = checksum;
        this.state = state;
        this.owner=owner;
    }

    /**
     * has to be overridden
     *
     */
    public ArchivalObjectDto toDto() {
        return new ArchivalObjectDto(id, id, checksum, getOwner(), null, state, created, ObjectType.OBJECT);
    }

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
