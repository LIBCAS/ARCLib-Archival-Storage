package cz.cas.lib.arcstorage.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ObjectConsistencyVerificationResultDto {
    /**
     * database id of the object
     */
    private String databaseId;
    /**
     * id of the object at storage
     */
    private String storageId;
    /**
     * state of the object in database
     */
    private ObjectState state;
    /**
     * whether checksum of the object computed at storage matches the checksum in database.. this may be set to true only
     * if {@link ObjectState#contentMustBeStoredAtLogicalStorage()} is true
     */
    private boolean contentConsistent;
    /**
     * whether the metadata of object stored at storage matches the object metadata stored in database
     */
    private boolean metadataConsistent;
    /**
     * freshly computed checksum of the object at storage, this is set to null if {@link ObjectState#contentMustBeStoredAtLogicalStorage()} is false
     */
    private Checksum storageChecksum;
    /**
     * checksum of the object stored in database
     */
    private Checksum databaseChecksum;
    /**
     * creation time of the object
     */
    private Instant created;

    public boolean considerCleanup() {
        return state.isFail() || (state.isProcessing() && created.isBefore(Instant.now().minus(1, ChronoUnit.DAYS)));
    }
}

