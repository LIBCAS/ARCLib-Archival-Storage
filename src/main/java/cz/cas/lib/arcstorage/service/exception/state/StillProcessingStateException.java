package cz.cas.lib.arcstorage.service.exception.state;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;

public class StillProcessingStateException extends StateException {

    public StillProcessingStateException(ArchivalObject obj) {
        super(obj);
    }

    public StillProcessingStateException(ArchivalObjectDto obj) {
        super(obj);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
