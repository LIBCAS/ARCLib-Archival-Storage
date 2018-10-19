package cz.cas.lib.arcstorage.service.exception.state;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;

public class RemovedStateException extends StateException {

    public RemovedStateException(ArchivalObject obj) {
        super(obj);
    }

    public RemovedStateException(ArchivalObjectDto obj) {
        super(obj);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
