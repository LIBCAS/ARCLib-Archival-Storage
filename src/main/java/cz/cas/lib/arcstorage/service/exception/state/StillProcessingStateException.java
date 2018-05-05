package cz.cas.lib.arcstorage.service.exception.state;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;

public class StillProcessingStateException extends StateException {

    public StillProcessingStateException(ArchivalObject obj) {
        super(obj);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
