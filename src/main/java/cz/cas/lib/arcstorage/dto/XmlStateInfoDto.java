package cz.cas.lib.arcstorage.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class XmlStateInfoDto {
    private int version;
    private boolean consistent;
    private Checksum storageChecksum;
    private Checksum databaseChecksum;
}
