package cz.cas.lib.arcstorage.service.exception.state;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;

public class StateException extends Exception {
    private ArchivalObject obj;
    private ArchivalObjectDto objDto;

    public StateException(ArchivalObject obj) {
        this.obj = obj;
    }

    public StateException(ArchivalObjectDto obj) {
        this.objDto = obj;
    }

    @Override
    public String toString() {
        if (objDto != null)
            return "Can't perform operation, object: " + objDto.getStorageId() + " is in state " + objDto.getState().toString();
        if (obj instanceof AipSip) {
            AipSip sip = ((AipSip) obj);
            return "Can't perform operation, SIP: " + sip.getId() + " is in state " + sip.getState().toString();
        }
        if (obj instanceof AipXml) {
            AipXml xml = ((AipXml) obj);
            return "Can't perform operation, XML version: " + xml.getVersion() + " of SIP: " + xml.getSip().getId() + " is in state " + xml.getState().toString();
        } else
            return "Can't perform operation, object: " + obj.getId() + " is in state " + obj.getState().toString();
    }
}
