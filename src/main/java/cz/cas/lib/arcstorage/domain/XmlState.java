package cz.cas.lib.arcstorage.domain;

import lombok.Getter;

@Getter
public enum XmlState {
    ARCHIVED,
    PROCESSING,
    ROLLBACKED
}
