package cz.cas.lib.arcstorage.gateway.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class AipCreationMd5InfoDto {
    private String sipMd5;
    private String xmlMd5;
}
