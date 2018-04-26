package cz.cas.lib.arcstorage.gateway.dto;

import cz.cas.lib.arcstorage.domain.ObjectState;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class AipStateInfoDto {
    private String storageName;
    private StorageType storageType;
    private ObjectState objectState;
    private boolean consistent;
    private Checksum storageChecksum;
    private Checksum databaseChecksum;
    private List<XmlStateInfoDto> xmlsState = new ArrayList<>();

    public AipStateInfoDto(String storageName, StorageType storageType, ObjectState objectState, Checksum databaseChecksum) {
        this.storageName = storageName;
        this.storageType = storageType;
        this.objectState = objectState;
        this.databaseChecksum = databaseChecksum;
    }

    public void addXmlInfo(XmlStateInfoDto xmlStateInfoDto) {
        xmlsState.add(xmlStateInfoDto);
    }
}


