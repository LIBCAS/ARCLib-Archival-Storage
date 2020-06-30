package cz.cas.lib.arcstorage.storagesync.exception;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;

import java.util.Arrays;
import java.util.List;

public class StorageStillProcessObjectsException extends Exception {
    public StorageStillProcessObjectsException(List<ArchivalObject> objectList) {
        super(Arrays.toString(objectList.toArray()));
    }
}
