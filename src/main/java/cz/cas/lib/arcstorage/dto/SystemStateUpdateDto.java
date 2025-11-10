package cz.cas.lib.arcstorage.dto;

import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SystemStateUpdateDto {

    @NotNull
    private Integer minStorageCount;

    @NotNull
    private Integer reachabilityCheckIntervalInMinutes;
}
