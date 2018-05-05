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
    private boolean reachable;
    private Checksum sipStorageChecksum;
    private Checksum sipDatabaseChecksum;
    private List<XmlStateInfoDto> xmlsState = new ArrayList<>();

    /**
     * Used by reachable storage services. Dto is further filled.
     *
     * @param storageName
     * @param storageType
     * @param objectState
     * @param sipDatabaseChecksum
     */
    public AipStateInfoDto(String storageName, StorageType storageType, ObjectState objectState, Checksum sipDatabaseChecksum) {
        this.storageName = storageName;
        this.storageType = storageType;
        this.objectState = objectState;
        this.sipDatabaseChecksum = sipDatabaseChecksum;
        reachable = true;
    }

    /**
     * Used when storage service is unreachable.
     *
     * @param storageName
     * @param storageType
     */
    public AipStateInfoDto(String storageName, StorageType storageType) {
        this.storageName = storageName;
        this.storageType = storageType;
        reachable = false;
    }

    public void addXmlInfo(XmlStateInfoDto xmlStateInfoDto) {
        xmlsState.add(xmlStateInfoDto);
    }
}


