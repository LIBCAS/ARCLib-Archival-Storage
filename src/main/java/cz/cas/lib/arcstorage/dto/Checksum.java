package cz.cas.lib.arcstorage.dto;

import lombok.*;

import jakarta.persistence.Embeddable;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;

@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Setter
@Getter
@Embeddable
public class Checksum {
    @NonNull
    @Enumerated(EnumType.STRING)
    private ChecksumType type;
    @NonNull
    private String value;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Checksum checksum = (Checksum) o;
        return getType() == checksum.getType() && getValue().equalsIgnoreCase(checksum.getValue());
    }

    @Override
    public int hashCode() {
        int result = getType().hashCode();
        result = 31 * result + getValue().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Checksum{" +
                "type=" + type +
                ", value='" + value + '\'' +
                '}';
    }
}
