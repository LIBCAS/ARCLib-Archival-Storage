package cz.cas.lib.arcstorage.gateway.storage.fs;

import cz.cas.lib.arcstorage.domain.StorageConfig;
import cz.cas.lib.arcstorage.gateway.dto.SpaceInfo;
import cz.cas.lib.arcstorage.gateway.dto.StorageState;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class FsStorageState extends StorageState {
    private SpaceInfo spaceInfo;

    public FsStorageState(StorageConfig storageConfig, SpaceInfo spaceInfo) {
        super(storageConfig);
        this.spaceInfo = spaceInfo;
    }
}
