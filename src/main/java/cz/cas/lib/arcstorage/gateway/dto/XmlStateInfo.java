package cz.cas.lib.arcstorage.gateway.dto;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class XmlStateInfo {
    private int version;
    private boolean consistent;
    private Checksum checksum;
}
