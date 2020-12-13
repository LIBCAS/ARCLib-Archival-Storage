package cz.cas.lib.arcstorage.dto;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class StorageBasicDto {
    private String id;
    private String name;
    private String storageType;
    private boolean synchronizing;

    public static StorageBasicDto transmute(Storage entity) {
        StorageBasicDto dto = new StorageBasicDto();
        dto.setId(entity.getId());
        dto.setName(entity.getName());
        dto.setStorageType(entity.getStorageType().name());
        dto.setSynchronizing(entity.isSynchronizing());
        return dto;
    }
}
