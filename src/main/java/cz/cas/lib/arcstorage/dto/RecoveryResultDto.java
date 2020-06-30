package cz.cas.lib.arcstorage.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;

import static cz.cas.lib.arcstorage.util.Utils.ne;

@Getter
@Setter
public class RecoveryResultDto {

    public RecoveryResultDto(String storageId) {
        this.storageId = storageId;
    }

    private String storageId;

    /**
     * sets of {@link ArchivalObjectDto#storageId}
     */
    private Set<String> contentInconsistencyObjectsIds = new HashSet<>();
    private Set<String> contentRecoveredObjectsIds = new HashSet<>();
    private Set<String> metadataInconsistencyObjectsIds = new HashSet<>();
    private Set<String> metadataRecoveredObjectsIds = new HashSet<>();

    public void merge(RecoveryResultDto dto) {
        ne(dto.getStorageId(), storageId, () ->
                new IllegalArgumentException("can't merge dtos of different storages, this storage id: " + storageId + " other storage id: " + dto.getStorageId()));
        contentInconsistencyObjectsIds.addAll(dto.getContentInconsistencyObjectsIds());
        contentRecoveredObjectsIds.addAll(dto.getContentRecoveredObjectsIds());
        metadataInconsistencyObjectsIds.addAll(dto.getMetadataInconsistencyObjectsIds());
        metadataRecoveredObjectsIds.addAll(dto.getMetadataInconsistencyObjectsIds());
    }
}
