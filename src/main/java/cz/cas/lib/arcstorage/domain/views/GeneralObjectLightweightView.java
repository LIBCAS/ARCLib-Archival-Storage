package cz.cas.lib.arcstorage.domain.views;

import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Getter;
import org.hibernate.annotations.Immutable;


@Getter
@Immutable
@Entity(name = "arcstorage_object_lightweight_view")
@Table(name = "arcstorage_object")
public class GeneralObjectLightweightView extends ArchivalObjectLightweightView {

    @Override
    public ArchivalObjectDto toDto() {
        return new ArchivalObjectDto(id, id, checksum, owner.getDataSpace(), null, state, created, ObjectType.OBJECT);
    }
}
