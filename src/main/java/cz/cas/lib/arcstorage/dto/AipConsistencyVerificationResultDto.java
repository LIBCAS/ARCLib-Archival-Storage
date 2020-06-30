package cz.cas.lib.arcstorage.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class AipConsistencyVerificationResultDto {
    private String storageName;
    private StorageType storageType;
    private boolean reachable;

    private ObjectConsistencyVerificationResultDto aipState;
    private List<XmlConsistencyVerificationResultDto> xmlStates = new ArrayList<>();

    public AipConsistencyVerificationResultDto(String storageName, StorageType storageType, boolean reachable) {
        this.storageName = storageName;
        this.storageType = storageType;
        this.reachable = reachable;
    }

    public void addXmlInfo(XmlConsistencyVerificationResultDto xmlStateInfoDto) {
        xmlStates.add(xmlStateInfoDto);
    }
}


