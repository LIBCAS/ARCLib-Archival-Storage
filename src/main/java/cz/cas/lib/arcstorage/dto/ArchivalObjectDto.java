package cz.cas.lib.arcstorage.dto;

import lombok.Getter;
import lombok.Setter;

import java.io.InputStream;

/**
 * DTO for archive file containing its id, checksum and input stream.
 * <p>{@link #storageId} and {@link #databaseId} differs only in case of XML object</p>
 */
@Setter
@Getter
public class ArchivalObjectDto {
    private String storageId;
    private String databaseId;
    private Checksum checksum;
    private InputStream inputStream;

    public ArchivalObjectDto(String databaseId, String storageId, InputStream inputStream, Checksum checksum) {
        this.databaseId = databaseId;
        this.storageId = storageId;
        this.checksum = checksum;
        this.inputStream = inputStream;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ArchivalObjectDto that = (ArchivalObjectDto) o;

        if (getStorageId() != null ? !getStorageId().equals(that.getStorageId()) : that.getStorageId() != null)
            return false;
        return getDatabaseId() != null ? getDatabaseId().equals(that.getDatabaseId()) : that.getDatabaseId() == null;
    }

    @Override
    public int hashCode() {
        int result = getStorageId() != null ? getStorageId().hashCode() : 0;
        result = 31 * result + (getDatabaseId() != null ? getDatabaseId().hashCode() : 0);
        return result;
    }
}


