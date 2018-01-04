package cz.cas.lib.arcstorage.gateway.exception;

import cz.cas.lib.arcstorage.domain.ArchivalObject;

public class DeletedException extends StateException {

    public DeletedException(ArchivalObject obj) {
        super(obj);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
