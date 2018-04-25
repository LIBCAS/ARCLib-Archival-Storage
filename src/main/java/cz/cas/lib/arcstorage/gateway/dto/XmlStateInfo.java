package cz.cas.lib.arcstorage.gateway.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class XmlStateInfo {
    private int version;
    private boolean consistent;
    private Checksum storageChecksum;
    private Checksum databaseChecksum;
}
