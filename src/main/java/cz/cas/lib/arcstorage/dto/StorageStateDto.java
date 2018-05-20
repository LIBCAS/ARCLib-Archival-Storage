package cz.cas.lib.arcstorage.dto;

import cz.cas.lib.arcstorage.domain.entity.Storage;
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
    private Storage storage;
    private Map<String, String> storageStateData;
}
