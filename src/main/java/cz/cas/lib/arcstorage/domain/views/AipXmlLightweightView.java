package cz.cas.lib.arcstorage.domain.views;

import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import lombok.Getter;
import org.hibernate.annotations.Immutable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;

/**
 * view without {@link AipXml#sip} relation
 */
@Immutable
@Entity(name = "arcstorage_aip_xml_lightweight_view")
@Table(name = "arcstorage_aip_xml")
@Getter
public class AipXmlLightweightView extends ArchivalObjectLightweightView {

    private int version;

    @Column(name = "arcstorage_aip_sip_id")
    private String arcstorageAipSipId;

    public ArchivalObjectDto toDto() {
        return new ArchivalObjectDto(toXmlId(arcstorageAipSipId, version), id, checksum, owner, null, state, created, ObjectType.XML);
    }
}


