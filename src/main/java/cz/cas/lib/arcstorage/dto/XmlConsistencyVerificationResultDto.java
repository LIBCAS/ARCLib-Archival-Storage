package cz.cas.lib.arcstorage.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@NoArgsConstructor
@Setter
@Getter
public class XmlConsistencyVerificationResultDto extends ObjectConsistencyVerificationResultDto {
    private int version;

    public XmlConsistencyVerificationResultDto(String databaseId, String storageId, ObjectState state, boolean contentConsistent, boolean metadataConsistent, Checksum storageChecksum, Checksum databaseChecksum, Instant created, int version) {
        super(databaseId, storageId, state, contentConsistent, metadataConsistent, storageChecksum, databaseChecksum, created);
        this.version = version;
    }
}
