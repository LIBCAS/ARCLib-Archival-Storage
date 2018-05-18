package cz.cas.lib.arcstorage.service.exception;

import cz.cas.lib.arcstorage.domain.entity.Storage;

import java.util.Arrays;
import java.util.List;

import static cz.cas.lib.arcstorage.util.Utils.asList;

public class StorageNotReachableException extends Exception {

    List<Storage> storages;

    public StorageNotReachableException(Storage... configs) {
        this.storages = asList(configs);
    }

    public StorageNotReachableException(List<Storage> configs) {
        this.storages = configs;
    }

    @Override
    public String toString() {
        return "StorageNotReachableException{" +
                "storages=" + Arrays.toString(storages.toArray()) +
                '}';
    }
}
