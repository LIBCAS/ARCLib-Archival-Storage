package cz.cas.lib.arcstorage.service.exception.state;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;

public class DeletedStateException extends StateException {

    public DeletedStateException(ArchivalObject obj) {
        super(obj);
    }

    public DeletedStateException(ArchivalObjectDto obj) {
        super(obj);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
