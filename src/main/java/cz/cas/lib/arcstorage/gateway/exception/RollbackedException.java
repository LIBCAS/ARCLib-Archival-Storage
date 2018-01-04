package cz.cas.lib.arcstorage.gateway.exception;

import cz.cas.lib.arcstorage.domain.ArchivalObject;

public class RollbackedException extends StateException {

    public RollbackedException(ArchivalObject obj) {
        super(obj);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
