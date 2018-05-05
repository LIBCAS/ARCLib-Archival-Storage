package cz.cas.lib.arcstorage.service.exception.state;

import cz.cas.lib.arcstorage.domain.entity.AipSip;
import cz.cas.lib.arcstorage.domain.entity.AipXml;
import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;

public class StateException extends Exception {
    private ArchivalObject obj;

    public StateException(ArchivalObject obj) {
        this.obj = obj;
    }

    @Override
    public String toString() {
        if (obj instanceof AipSip) {
            AipSip sip = ((AipSip) obj);
            return "Can't perform operation SIP: " + sip.getId() + " is in state " + sip.getState().toString();
        } else if (obj instanceof AipXml) {
            AipXml xml = ((AipXml) obj);
            return "Can't perform operation XML version: " + xml.getVersion() + " of SIP: " + xml.getSip().getId() + " is in state " + xml.getState().toString();
        } else
            return "Can't perform operation.";
    }
}
