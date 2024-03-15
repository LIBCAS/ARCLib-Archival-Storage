package cz.cas.lib.arcstorage.dto;

import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ObjectInfoDto {
    private String id;
    private String idInStorage;
    private Instant created;
    private Checksum checksum;
    private String dataSpace;
    private ObjectState state;
    private ObjectType type;

    public static ObjectInfoDto of(ArchivalObjectDto d) {
        return new ObjectInfoDto(d.getDatabaseId(), d.getStorageId(), d.getCreated(), d.getChecksum(), d.getOwner().getDataSpace(), d.getState(), d.getObjectType());
    }
}
