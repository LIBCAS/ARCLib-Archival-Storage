package cz.cas.lib.arcstorage.gateway.dto;

import cz.cas.lib.arcstorage.domain.StorageConfig;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * General DTO for storage state information to be sent through API.
 */
@Getter
@Setter
@AllArgsConstructor
public class StorageStateDto {
    private StorageConfig storageConfig;
    private Map<String, String> storageStateData;
}
