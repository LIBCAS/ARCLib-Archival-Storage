package cz.cas.lib.arcstorage.dto;

import cz.cas.lib.arcstorage.domain.entity.ObjectType;
import cz.cas.lib.arcstorage.domain.entity.User;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static cz.cas.lib.arcstorage.storage.StorageUtils.toXmlId;

/**
 * DTO for transfer of AIP object.
 * <p>
 * As opposite to {@link AipRetrievalResource} which only contains input streams and is used to transfer data from storage,
 * this DTO contains also checksums and is used to transfer data TO storage layer or to enrich data from storage layer on the
 * service layer.
 * </p>
 */
@NoArgsConstructor
public class AipDto {

    /**
     * DTO for SIP object related to this AIP.
     */
    @Setter
    @Getter
    private ArchivalObjectDto sip;

    /**
     * List with DTOs of XML objects related to this AIP. THIS MAY NOT CONTAIN ALL XMLS AND ORDER IS NOT GUARANTEED.
     * This is DTO. XMLs presence and order is dependent on how the list is filled by application logic.
     */
    private List<ArchivalObjectDto> xmls = new ArrayList<>();

    /**
     * Constructor used when transferring this DTO from service layer to storage layer. One XML with version 1 and null id is added.
     */
    public AipDto(String ownerId, String sipId, InputStream sipStream, Checksum sipChecksum, InputStream aipXmlStream, Checksum xmlChecksum) {
        Instant now = Instant.now();
        sip = new ArchivalObjectDto(sipId, sipId, sipChecksum, new User(ownerId), sipStream, ObjectState.PRE_PROCESSING, now, ObjectType.SIP);
        xmls.add(new ArchivalObjectDto(toXmlId(sipId, 1), null, xmlChecksum, new User(ownerId), aipXmlStream, ObjectState.PRE_PROCESSING, now, ObjectType.XML));
    }

    /**
     * Copies existing AIP ref and assigns new input streams to it.
     * Used for example when there is a need to send the same AIP to multiple storages where every storage needs own instance of AIP input streams.
     */
    public AipDto(AipDto aipDto, InputStream sipIs, InputStream xmlIs) {
        sip = new ArchivalObjectDto(aipDto.getSip(), sipIs);
        xmls.add(new ArchivalObjectDto(aipDto.getXml(), xmlIs));
    }

    public List<ArchivalObjectDto> getXmls() {
        return Collections.unmodifiableList(xmls);
    }

    public ArchivalObjectDto getXml(int index) {
        return xmls.get(index);
    }

    /**
     * Used when the aip has single XML i.e. it is just under process related to creation.
     */
    public ArchivalObjectDto getXml() {
        return xmls.get(0);
    }

    public void addXml(ArchivalObjectDto xml) {
        xmls.add(xml);
    }

    public String toString() {
        return "Aip: " + sip.getDatabaseId();
    }
}
