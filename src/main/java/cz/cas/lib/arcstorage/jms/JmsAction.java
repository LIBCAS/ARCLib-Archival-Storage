package cz.cas.lib.arcstorage.jms;

import lombok.Getter;

import static cz.cas.lib.arcstorage.jms.JmsPriority.*;


public enum JmsAction {

    ACTIVATE_DATASPACE(CONFIGURATION),
    SAVE_AIP(NEW_OBJECT),
    SAVE(NEW_OBJECT),
    COPY(OLD_OBJECT),
    DELETE(MODIFICATION),
    ROLLBACK(MODIFICATION),
    RENEW(MODIFICATION),
    REMOVE(MODIFICATION),
    FORGET(MODIFICATION);

    @Getter
    private final JmsPriority priority;

    JmsAction(JmsPriority priority) {
        this.priority = priority;
    }
}
