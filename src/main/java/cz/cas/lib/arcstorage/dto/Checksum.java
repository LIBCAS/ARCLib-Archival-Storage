package cz.cas.lib.arcstorage.dto;

import lombok.*;

import javax.persistence.Embeddable;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
@Embeddable
@ToString
public class Checksum {
    @Enumerated(EnumType.STRING)
    private ChecksumType type;
    private String hash;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Checksum checksum = (Checksum) o;
        return getType() == checksum.getType() && getHash().equalsIgnoreCase(checksum.getHash());
    }

    @Override
    public int hashCode() {
        int result = getType().hashCode();
        result = 31 * result + getHash().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Checksum{" +
                "type=" + type +
                ", hash='" + hash + '\'' +
                '}';
    }
}
