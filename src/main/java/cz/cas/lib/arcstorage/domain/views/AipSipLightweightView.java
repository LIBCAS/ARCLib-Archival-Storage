package cz.cas.lib.arcstorage.domain.views;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import lombok.Getter;
import org.hibernate.annotations.Immutable;

import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * view without {@link AipSip#xmls} eager relation
 */
@Immutable
@Entity(name = "arcstorage_aip_sip_lightweight_view")
@Table(name = "arcstorage_aip_sip")
@Getter
public class AipSipLightweightView extends ArchivalObjectLightweightView {

    public ArchivalObjectDto toDto() {
        return new ArchivalObjectDto(id, id, checksum, owner, null, state, created, ObjectType.SIP);
    }
}