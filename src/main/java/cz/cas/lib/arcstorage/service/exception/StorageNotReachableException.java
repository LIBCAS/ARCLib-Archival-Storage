package cz.cas.lib.arcstorage.service.exception;

import cz.cas.lib.arcstorage.domain.entity.StorageConfig;

import java.util.Arrays;
import java.util.List;

public class StorageNotReachableException extends Exception {

    StorageConfig[] storageConfigs;

    public StorageNotReachableException(StorageConfig... configs) {
        this.storageConfigs = configs;
    }

    public StorageNotReachableException(List<StorageConfig> configs) {
        this.storageConfigs = (StorageConfig[]) configs.toArray();
    }

    @Override
    public String toString() {
        return "StorageNotReachableException{" +
                "storageConfigs=" + Arrays.toString(storageConfigs) +
                '}';
    }
}
