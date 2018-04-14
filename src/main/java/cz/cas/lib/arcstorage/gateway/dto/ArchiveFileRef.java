package cz.cas.lib.arcstorage.gateway.dto;

import lombok.Getter;
import lombok.Setter;

/**
 * Transfer class for file data
 */
@Setter
@Getter
public class ArchiveFileRef extends FileRef {
    private String id;
    private Checksum checksum;

    public ArchiveFileRef(String id, FileRef fileRef, Checksum checksum) {
        super(fileRef.getInputStream(), fileRef.getChannels());
        this.id = id;
        this.checksum = checksum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ArchiveFileRef archiveFileRef = (ArchiveFileRef) o;

        return getId().equals(archiveFileRef.getId());
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }
}


