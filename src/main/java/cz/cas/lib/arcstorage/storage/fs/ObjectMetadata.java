package cz.cas.lib.arcstorage.storage.fs;

import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ChecksumType;
import cz.cas.lib.arcstorage.dto.ObjectState;
import cz.cas.lib.arcstorage.storage.exception.CantParseMetadataFile;
import lombok.Getter;
import lombok.NonNull;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;

import static cz.cas.lib.arcstorage.util.Utils.notNull;


public class ObjectMetadata {

    public static final String KEY_CHECKSUM_TYPE = "checksum_type";
    public static final String KEY_CHECKSUM_VALUE = "checksum_value";
    public static final String KEY_CREATED = "created";
    public static final String KEY_STATE = "state";

    @Getter
    private Instant created;
    @Getter
    private ObjectState state;
    @Getter
    private Checksum checksum;
    @Getter
    private String storageId;

    public ObjectMetadata(@NonNull String storageId, @NonNull ObjectState state, @NonNull Instant created, @NonNull Checksum checksum) {
        this.storageId = storageId;
        this.checksum = checksum;
        this.state = state;
        this.created = created;
    }

    public ObjectMetadata(@NonNull List<String> lines, @NonNull String storageId, @NonNull Storage storage) throws CantParseMetadataFile {
        if (lines.size() != 4)
            throw new CantParseMetadataFile(storageId, "Some metadata missing, file content: " + Arrays.toString(lines.toArray()), storage);
        this.storageId = storageId;
        String checksumValue = null;
        ChecksumType checksumType = null;
        for (String line : lines) {
            int separatorIndex = line.indexOf(':');
            String key = line.substring(0, separatorIndex);
            String value = line.substring(separatorIndex + 1);
            try {
                switch (key) {
                    case KEY_CHECKSUM_TYPE:
                        checksumType = ChecksumType.valueOf(value);
                        break;
                    case KEY_CHECKSUM_VALUE:
                        checksumValue = value;
                        break;
                    case KEY_CREATED:
                        created = Instant.parse(value);
                        break;
                    case KEY_STATE:
                        state = ObjectState.valueOf(value);
                        break;
                    default:
                        throw new CantParseMetadataFile(storageId, "Unknown metadata key: " + value, storage);
                }
            } catch (DateTimeParseException | IllegalArgumentException e) {
                throw new CantParseMetadataFile(storageId, "Couldn't deserialize " + key + " value: " + value, storage);
            }
        }
        this.checksum = new Checksum(checksumType, checksumValue);
    }

    /**
     * Serializes metadata of the object. {@link #storageId} is not serialized.
     *
     * @return serialized metadata
     */
    public byte[] serialize() throws IOException {
        String keyValueFormat = "%s:%s";
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(bos));
        bufferedWriter.write(String.format(keyValueFormat, KEY_STATE, state));
        if (created != null) {
            bufferedWriter.newLine();
            bufferedWriter.write(String.format(keyValueFormat, KEY_CREATED, created));
        }
        if (checksum != null) {
            bufferedWriter.newLine();
            bufferedWriter.write(String.format(keyValueFormat, KEY_CHECKSUM_TYPE, checksum.getType()));
            bufferedWriter.newLine();
            bufferedWriter.write(String.format(keyValueFormat, KEY_CHECKSUM_VALUE, checksum.getValue()));
        }
        bufferedWriter.flush();
        return bos.toByteArray();
    }

    public void setState(ObjectState state) {
        notNull(state, () -> new IllegalArgumentException("missing metadata"));
        this.state = state;
    }

    public ArchivalObjectDto toDto(ObjectType objectType) {
        return new ArchivalObjectDto(storageId, null, checksum, null, null, state, created, objectType);
    }
}
