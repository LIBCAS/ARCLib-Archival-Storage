package cz.cas.lib.arcstorage.domain.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.dto.Checksum;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.BatchSize;

import javax.persistence.*;

/**
 * XML database entity.
 */
@Getter
@Setter
@BatchSize(size = 100)
@Entity
@Table(name = "arcstorage_aip_xml")
@NoArgsConstructor
public class AipXml extends ArchivalObject {

    @ManyToOne
    @JoinColumn(name = "arcstorage_aip_sip_id")
    @JsonIgnore
    private AipSip sip;
    private int version;

    @Enumerated(EnumType.STRING)
    private ObjectState state;

    /**
     * Creates entity with assigned id.
     */
    public AipXml(String id, Checksum checksum, AipSip sip, int version, ObjectState state) {
        super(id, checksum);
        this.sip = sip;
        this.version = version;
        this.state = state;
    }
}
