package cz.cas.lib.arcstorage.gateway.dto;

import cz.cas.lib.arcstorage.domain.AipState;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class AipStateInfo {
    private String storageName;
    private StorageType storageType;
    private AipState aipState;
    private boolean consistent;
    private Checksum checksum;
    private List<XmlStateInfo> xmlsState = new ArrayList<>();

    public AipStateInfo(String storageName, StorageType storageType, AipState aipState) {
        this.storageName = storageName;
        this.storageType = storageType;
        this.aipState = aipState;
    }

    public void addXmlInfo(XmlStateInfo xmlStateInfo) {
        xmlsState.add(xmlStateInfo);
    }
}


