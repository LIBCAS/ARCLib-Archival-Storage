package cz.cas.lib.arcstorage.domain;

import lombok.Getter;

@Getter
public enum AipState {
    PROCESSING,
    ARCHIVED,
    DELETED,
    REMOVED,
    ROLLBACKED
}
