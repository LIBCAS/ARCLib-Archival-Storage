package cz.cas.lib.arcstorage.gateway.exception;

import cz.cas.lib.arcstorage.domain.StorageConfig;

import java.util.Arrays;
import java.util.List;

public class StorageNotReachableException extends Exception {

    public StorageNotReachableException(StorageConfig... configs) {
        super(Arrays.toString(configs));
    }

    public StorageNotReachableException(List<StorageConfig> configs) {
        super(Arrays.toString(configs.toArray()));
    }
}
