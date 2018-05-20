package cz.cas.lib.arcstorage.service.exception.storage;

import cz.cas.lib.arcstorage.domain.entity.Storage;

import java.util.Arrays;
import java.util.List;

import static cz.cas.lib.arcstorage.util.Utils.asList;

public class SomeLogicalStoragesNotReachableException extends Exception {

    List<Storage> storages;

    public SomeLogicalStoragesNotReachableException(Storage... configs) {
        this.storages = asList(configs);
    }

    public SomeLogicalStoragesNotReachableException(List<Storage> configs) {
        this.storages = configs;
    }

    @Override
    public String toString() {
        return "SomeLogicalStoragesNotReachableException{" +
                "storages=" + Arrays.toString(storages.toArray()) +
                '}';
    }
}
