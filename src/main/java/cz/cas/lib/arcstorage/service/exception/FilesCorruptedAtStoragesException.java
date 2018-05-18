package cz.cas.lib.arcstorage.service.exception;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.storage.StorageService;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class FilesCorruptedAtStoragesException extends Exception {
    private AipSip invalidChecksumSip;
    private List<AipXml> invalidChecksumXmls;
    private List<StorageService> storages;

    public FilesCorruptedAtStoragesException(AipSip invalidChecksumSip, List<AipXml> invalidChecksumXmls, List<StorageService> storages) {
        this.invalidChecksumSip = invalidChecksumSip;
        this.invalidChecksumXmls = invalidChecksumXmls;
        this.storages = storages;
    }

    public FilesCorruptedAtStoragesException(List<AipXml> invalidChecksumXmls, List<StorageService> storages) {
        this.invalidChecksumXmls = invalidChecksumXmls;
        this.storages = storages;
    }

    public FilesCorruptedAtStoragesException(AipSip invalidChecksumSip, List<StorageService> storages) {
        this.invalidChecksumSip = invalidChecksumSip;
        this.storages = storages;
    }

    @Override
    public String toString() {
        String corruptedObjects;
        if (invalidChecksumSip != null && !invalidChecksumXmls.isEmpty()) {
            corruptedObjects = "invalidChecksumSip=" + invalidChecksumSip + ", invalidChecksumXmls=" + invalidChecksumXmls;
        } else if (invalidChecksumSip != null) {
            corruptedObjects = "invalidChecksumSip=" + invalidChecksumSip;
        } else {
            corruptedObjects = "invalidChecksumXmls=" + invalidChecksumXmls;
        }

        return "FilesCorruptedAtStoragesException{" +
                corruptedObjects +
                ", storages=" + storages +
                '}';
    }
}
