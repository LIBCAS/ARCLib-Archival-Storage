package cz.cas.lib.arcstorage.dto;

import cz.cas.lib.arcstorage.domain.entity.ArchivalObject;
import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.domain.entity.User;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.InputStream;
import java.time.Instant;
import java.util.Objects;

/**
 * DTO for archive file containing its id, checksum and optionally input stream and other data.
 * <p>{@link #storageId} and {@link #databaseId} differs only in case of XML object</p>
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class ArchivalObjectDto {
    private String storageId;
    private String databaseId;
    private Checksum checksum;
    /**
     * object may be populated just with id
     */
    private User owner;

    private InputStream inputStream;
    private ObjectState state;
    private Instant created;
    /**
     * set automatically when DTO is created via {@link ArchivalObject#toDto()}
     * may be used as a hint but also may be not filled.. double check that it is filled before using
     */
    private ObjectType objectType;

    public boolean metadataEquals(ArchivalObjectDto that) {
        if (this == that) return true;
        if (that == null || getClass() != that.getClass()) return false;
        return Objects.equals(getStorageId(), that.getStorageId()) &&
                Objects.equals(getChecksum(), that.getChecksum()) &&
                getState() == that.getState() &&
                Objects.equals(getCreated(), that.getCreated());
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

    /**
     * copies the object and assigns new stream to it
     * @param oldDto
     * @param newInputStream
     */
    public ArchivalObjectDto(ArchivalObjectDto oldDto, InputStream newInputStream){
        this.storageId = oldDto.storageId;
        this.databaseId = oldDto.databaseId;
        this.checksum = oldDto.checksum;
        this.owner = oldDto.owner;
        this.inputStream = newInputStream;
        this.state = oldDto.state;
        this.created = oldDto.created;
        this.objectType = oldDto.objectType;
    }

    @Override
    public String toString() {
        return "ArchivalObjectDto{" +
                "storageId='" + storageId + '\'' +
                ", databaseId='" + databaseId + '\'' +
                '}';
    }
}


