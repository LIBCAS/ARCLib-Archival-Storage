package cz.cas.lib.arcstorage.dto;

import lombok.Getter;
import lombok.Setter;

import java.io.InputStream;

/**
 * DTO for SIP containing its storageId, checksum and input stream.
 */
@Setter
@Getter
public class SipDto {
    private String id;
    private Checksum checksum;
    private InputStream inputStream;

    public SipDto(String id, InputStream inputStream, Checksum checksum) {
        this.id = id;
        this.checksum = checksum;
        this.inputStream = inputStream;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ArchivalObjectDto archivalObjectDto = (ArchivalObjectDto) o;

        return getId().equals(archivalObjectDto.getStorageId());
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }
}
