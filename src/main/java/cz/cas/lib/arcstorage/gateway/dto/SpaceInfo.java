package cz.cas.lib.arcstorage.gateway.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class SpaceInfo {
    private long capacity;
    private long used;
    private long free;
}

