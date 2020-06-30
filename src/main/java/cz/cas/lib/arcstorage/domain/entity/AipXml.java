package cz.cas.lib.arcstorage.domain.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ObjectState;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.BatchSize;

import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;

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
    /**
     * Version number of XML is not incremented if the previous archival attempt failed, therefore there might be more
     * {{@link AipXml}} of the same {{@link AipSip}} with the same version number. Zero or exactly one of them can be in
     * {@link ObjectState#ARCHIVED}.
     */
    private int version;

    /**
     * Creates entity with assigned id.
     */
    public AipXml(String id, Checksum checksum, User owner, AipSip sip, int version, ObjectState state) {
        super(checksum, owner, state);
        this.id = id;
        this.sip = sip;
        this.version = version;
    }

    @Override
    public ArchivalObjectDto toDto() {
        return new ArchivalObjectDto(toXmlId(sip.getId(), version), id, getChecksum(), getOwner(), null, getState(), getCreated(), ObjectType.XML);
    }
}
