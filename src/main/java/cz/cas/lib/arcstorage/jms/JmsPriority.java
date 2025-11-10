package cz.cas.lib.arcstorage.jms;

import lombok.Getter;

public enum JmsPriority {
    CONFIGURATION(9),
    NEW_OBJECT(7),
    OLD_OBJECT(5),
    MODIFICATION(3),
    ;

    @Getter
    private final int value;

    JmsPriority(int value) {
        this.value = value;
    }
}
