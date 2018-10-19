package cz.cas.lib.arcstorage.service.exception.state;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;

public class RollbackStateException extends StateException {

    public RollbackStateException(ArchivalObject obj) {
        super(obj);
    }

    public RollbackStateException(ArchivalObjectDto obj) {
        super(obj);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
