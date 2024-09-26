package cz.cas.lib.arcstorage.dto;

import lombok.Getter;
import lombok.Setter;

import jakarta.validation.constraints.NotNull;

@Getter
@Setter
public class StorageUpdateDto {
    @NotNull
    String id;
    int priority;
    String name;
    String note;
}
