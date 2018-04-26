package cz.cas.lib.arcstorage.gateway.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * DTO for transfer of AIP object with its file content together with AIP XML objects and their content.
 * Contains XML version in addition to other attributes of {@link ArchivalObjectDto}.
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
    private List<XmlDto> xmls = new ArrayList<>();

    /**
     * Constructor used when transferring this DTO between service layer and storage layer.
     * The service layer XML ID is not important in this case and is set to null.
     */
    public AipDto(String sipId, InputStream sipStream, Checksum sipChecksum, InputStream aipXmlStream, Checksum xmlChecksum) {
        sip = new ArchivalObjectDto(sipId, new FileContentDto(sipStream), sipChecksum);
        xmls.add(new XmlDto(new FileContentDto(aipXmlStream), xmlChecksum, 1));
    }

    /**
     * Copies existing AIP ref and assigns new input streams to it.
     * Used for example when there is a need to send the same AIP to multiple storages where every storage needs own instance of AIP input streams.
     */
    public AipDto(AipDto aipDto, InputStream sipIs, InputStream xmlIs) {
        sip = new ArchivalObjectDto(aipDto.getSip().getId(), new FileContentDto(sipIs), aipDto.getSip().getChecksum());
        xmls.add(new XmlDto(aipDto.getXml().getId(), new FileContentDto(xmlIs), aipDto.getXml().getChecksum(), aipDto.getXml().getVersion()));
    }

    public List<XmlDto> getXmls() {
        return Collections.unmodifiableList(xmls);
    }

    public XmlDto getXml(int index) {
        return xmls.get(index);
    }

    /**
     * Used when the aip has single XML i.e. it is just under process related to creation.
     */
    public XmlDto getXml() {
        return xmls.get(0);
    }

    public void addXml(XmlDto xml) {
        xmls.add(xml);
    }
}
