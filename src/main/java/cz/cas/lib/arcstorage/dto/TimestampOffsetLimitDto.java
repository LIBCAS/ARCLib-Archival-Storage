package cz.cas.lib.arcstorage.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;

@Getter
@Setter
public class TimestampOffsetLimitDto {

    @Schema(description = "exclusive offset, if set, only objects with created > from are retrieved")
    private Instant from;

    @Schema(description = "limit of retrieved records")
    private Integer count;
}
