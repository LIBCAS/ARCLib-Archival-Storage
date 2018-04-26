package cz.cas.lib.arcstorage.gateway.dto;

import lombok.Getter;
import lombok.Setter;

/**
 * DTO for archive file containing its id, checksum and input stream. Used directly as DTO for SIP object with its content.
 */
@Setter
@Getter
public class ArchivalObjectDto extends FileContentDto {
    private String id;
    private Checksum checksum;

    public ArchivalObjectDto(String id, FileContentDto fileContentDto, Checksum checksum) {
        super(fileContentDto.getInputStream(), fileContentDto.getChannels());
        this.id = id;
        this.checksum = checksum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ArchivalObjectDto archivalObjectDto = (ArchivalObjectDto) o;

        return getId().equals(archivalObjectDto.getId());
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }
}


