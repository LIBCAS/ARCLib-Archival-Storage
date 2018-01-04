package cz.cas.lib.arcstorage.gateway.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.InputStream;

/**
 * Transfer class for file data
 */
@Setter
@Getter
@AllArgsConstructor
public class FileRef {
    private String id;
    private InputStream stream;
    private Checksum checksum;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileRef fileRef = (FileRef) o;

        return getId().equals(fileRef.getId());
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }
}


