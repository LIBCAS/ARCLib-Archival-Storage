package cz.cas.lib.arcstorage.service.exception;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.entity.Storage;

import java.util.Arrays;
import java.util.List;

public class StorageNotReachableException extends Exception {

    Storage[] storages;

    public StorageNotReachableException(Storage... configs) {
        this.storages = configs;
    }

    public StorageNotReachableException(List<Storage> configs) {
        this.storages = (Storage[]) configs.toArray();
    }

    @Override
    public String toString() {
        return "StorageNotReachableException{" +
                "storages=" + Arrays.toString(storages) +
                '}';
    }
}
