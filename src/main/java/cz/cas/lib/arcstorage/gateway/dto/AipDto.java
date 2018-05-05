package cz.cas.lib.arcstorage.gateway.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
     * Constructor used when transferring this DTO from service layer to storage layer.
     */
    public AipDto(String sipId, InputStream sipStream, Checksum sipChecksum, String xmlId, InputStream aipXmlStream, Checksum xmlChecksum) {
        sip = new ArchivalObjectDto(sipId, sipStream, sipChecksum);
        xmls.add(new ArchivalObjectDto(xmlId, aipXmlStream, xmlChecksum));
    }

    /**
     * Copies existing AIP ref and assigns new input streams to it.
     * Used for example when there is a need to send the same AIP to multiple storages where every storage needs own instance of AIP input streams.
     */
    public AipDto(AipDto aipDto, InputStream sipIs, InputStream xmlIs) {
        sip = new ArchivalObjectDto(aipDto.getSip().getId(), sipIs, aipDto.getSip().getChecksum());
        xmls.add(new ArchivalObjectDto(aipDto.getXml().getId(), xmlIs, aipDto.getXml().getChecksum()));
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
}
