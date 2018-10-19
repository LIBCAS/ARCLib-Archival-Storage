package cz.cas.lib.arcstorage.service.exception.storage;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Used when get request does not find any valid copy of the file. Storages with invalid copies are contained in exception
 * message. Those which were not reachable are not contained.
 */
@Slf4j
public class ObjectCouldNotBeRetrievedException extends Exception {
    private AipSip invalidChecksumSip;
    private ArchivalObjectDto archivalObject;
    private List<AipXml> invalidChecksumXmls;

    public ObjectCouldNotBeRetrievedException(AipSip invalidChecksumSip, List<AipXml> invalidChecksumXmls) {
        this.invalidChecksumSip = invalidChecksumSip;
        this.invalidChecksumXmls = invalidChecksumXmls;
    }

    public ObjectCouldNotBeRetrievedException(List<AipXml> invalidChecksumXmls) {
        this.invalidChecksumXmls = invalidChecksumXmls;
    }

    public ObjectCouldNotBeRetrievedException(ArchivalObjectDto archivalObject) {
        this.archivalObject = archivalObject;
    }

    public ObjectCouldNotBeRetrievedException(AipSip invalidChecksumSip) {
        this.invalidChecksumSip = invalidChecksumSip;
    }

    @Override
    public String toString() {
        String corruptedObjects;
        if (invalidChecksumSip != null && invalidChecksumXmls != null) {
            corruptedObjects = "invalidChecksumSip=" + invalidChecksumSip + ", invalidChecksumXmls=" + invalidChecksumXmls;
        } else if (invalidChecksumSip != null) {
            corruptedObjects = "invalidChecksumSip=" + invalidChecksumSip;
        } else if (invalidChecksumXmls != null) {
            corruptedObjects = "invalidChecksumXmls=" + invalidChecksumXmls;
        } else
            corruptedObjects = "invalidChecksumObject=" + archivalObject;

        return "ObjectCouldNotBeRetrievedException{" +
                corruptedObjects +
                '}';
    }
}
