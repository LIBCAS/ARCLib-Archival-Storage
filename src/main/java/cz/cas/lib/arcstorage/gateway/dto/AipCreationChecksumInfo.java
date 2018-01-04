package cz.cas.lib.arcstorage.gateway.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class AipCreationChecksumInfo {
    private Checksum sipChecksum;
    private Checksum xmlChecksum;
}
