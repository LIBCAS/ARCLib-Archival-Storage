package cz.cas.lib.arcstorage.dto;

import lombok.Getter;
import lombok.Setter;

import java.io.InputStream;

/**
 * DTO for archive file containing its id, checksum and input stream. Used directly as DTO for SIP object with its content.
 */
@Setter
@Getter
public class ArchivalObjectDto {
    private String id;
    private Checksum checksum;
    private InputStream inputStream;

    public ArchivalObjectDto(String id, InputStream inputStream, Checksum checksum) {
        this.id = id;
        this.checksum = checksum;
        this.inputStream = inputStream;
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


